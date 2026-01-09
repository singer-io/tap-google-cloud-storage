# tap_google_cloud_storage/gcs.py

import io
import json
import re
import singer
from google.cloud import storage
from singer_encodings import csv as singer_csv
from tap_google_cloud_storage import conversion as conversion  # type: ignore

LOGGER = singer.get_logger()

SDC_SOURCE_BUCKET_COLUMN = "_sdc_source_bucket"
SDC_SOURCE_FILE_COLUMN = "_sdc_source_file"
SDC_SOURCE_LINENO_COLUMN = "_sdc_source_lineno"
SDC_EXTRA_COLUMN = "_sdc_extra"


def setup_gcs_client(config):
    """
    Create and return a GCS client using the config directly as service account JSON.
    """
    try:
        client = storage.Client.from_service_account_info(config)
        return client

    except Exception as e:
        LOGGER.error("Failed to create GCS client: %s", e)
        raise


def list_files_in_bucket(config):
    """
    Generator to list files in a GCS bucket.
    Used to validate connectivity & permissions.
    """
    client = setup_gcs_client(config)
    bucket_name = config.get("bucket")
    prefix = config.get("root_path", "")
    LOGGER.info("Listing files in bucket: %s (prefix=%s)", bucket_name, prefix or "<none>")
    if not bucket_name:
        LOGGER.error("Bucket not found in config")
        raise ValueError("Bucket not found in config")

    bucket = client.bucket(bucket_name)

    try:
        blobs = bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            yield blob
    except Exception as e:
        LOGGER.error("Failed to list files in GCS bucket: %s", e)
        raise


def _iter_matching_blobs(config, table_spec):
    """Yield blobs matching table_spec search_prefix and search_pattern."""
    search_prefix = table_spec.get('search_prefix', '') or ''
    root = config.get('root_path', '') or ''
    effective_prefix = f"{root}{search_prefix}" if root else search_prefix
    pattern = table_spec.get('search_pattern')
    regex = re.compile(pattern) if pattern else None

    for blob in list_files_in_bucket({**config, 'root_path': effective_prefix}):
        name = blob.name
        if regex is None or regex.search(name):
            yield blob


def _get_records_for_csv(gcs_path, sample_rate, buffer, table_spec):
    current_row = 0
    sampled_row_count = 0
    iterator = singer_csv.get_row_iterator(buffer, table_spec, None, True)
    if not iterator:
        return
    for row in iterator:
        if len(row) == 0:
            current_row += 1
            continue
        if (current_row % sample_rate) == 0:
            if row.get(SDC_EXTRA_COLUMN):
                row.pop(SDC_EXTRA_COLUMN)
            sampled_row_count += 1
            yield row
        current_row += 1


def _get_records_for_jsonl(sample_rate, data_bytes):
    current_row = 0
    for line in data_bytes.splitlines():
        if (current_row % sample_rate) == 0:
            decoded = line.decode('utf-8') if isinstance(line, (bytes, bytearray)) else line
            if decoded and decoded.strip():
                try:
                    yield json.loads(decoded)
                except Exception:
                    pass
        current_row += 1


def _get_records_for_json(sample_rate, data_bytes):
    try:
        loaded = json.loads(data_bytes.decode('utf-8'))
    except Exception:
        return
    if isinstance(loaded, list):
        for idx, item in enumerate(loaded):
            if (idx % sample_rate) == 0 and isinstance(item, dict):
                yield item
    elif isinstance(loaded, dict):
        # Single JSON object; yield as one sample
        yield loaded


def _get_records_for_parquet(sample_rate, data_bytes):
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except Exception:
        return
    try:
        table = pq.read_table(pa.BufferReader(data_bytes))
    except Exception:
        return
    # Convert to batches and iterate rows with sample_rate
    batches = table.to_batches()
    row_idx = 0
    for batch in batches:
        rows = batch.to_pylist()
        for row in rows:
            if (row_idx % sample_rate) == 0 and isinstance(row, dict):
                yield row
            row_idx += 1


def _get_records_for_avro(sample_rate, data_bytes):
    try:
        from fastavro import reader  # type: ignore
    except Exception:
        return
    try:
        bio = io.BytesIO(data_bytes)
        avro_reader = reader(bio)
    except Exception:
        return
    for idx, record in enumerate(avro_reader):
        if (idx % sample_rate) == 0 and isinstance(record, dict):
            yield record


def get_sampled_schema_for_table(config, table_spec, max_files=10, sample_rate=100):
    LOGGER.info('Sampling records to determine table schema for table "%s".', table_spec.get('table_name'))

    samples = []
    bucket_name = config.get('bucket')
    found_any = False

    for idx, blob in enumerate(_iter_matching_blobs(config, table_spec)):
        found_any = True
        if idx >= max_files:
            break
        name = blob.name
        lower = name.lower()

        try:
            data = blob.download_as_bytes()
        except Exception as e:
            LOGGER.warning('Skipping %s due to download error: %s', name, e)
            continue

        # Delimited text: CSV/TXT/TSV/PSV (default delimiters if none provided)
        if lower.endswith('.csv') or lower.endswith('.txt') or lower.endswith('.tsv') or lower.endswith('.psv'):
            # Clone table_spec to inject default delimiter if not provided
            ts = dict(table_spec or {})
            if 'delimiter' not in ts or ts.get('delimiter') in (None, ''):
                if lower.endswith('.tsv'):
                    ts['delimiter'] = '\t'
                elif lower.endswith('.psv'):
                    ts['delimiter'] = '|'
                else:
                    ts['delimiter'] = ','
            for rec in _get_records_for_csv(name, sample_rate, io.BytesIO(data), ts):
                samples.append(rec)
        elif lower.endswith('.jsonl'):
            for rec in _get_records_for_jsonl(sample_rate, data):
                samples.append(rec)
        elif lower.endswith('.json'):
            for rec in _get_records_for_json(sample_rate, data):
                samples.append(rec)
        elif lower.endswith('.parquet'):
            for rec in _get_records_for_parquet(sample_rate, data):
                samples.append(rec)
        elif lower.endswith('.avro'):
            for rec in _get_records_for_avro(sample_rate, data):
                samples.append(rec)
        else:
            LOGGER.warning('"%s" with unsupported extension for sampling; skipping', name)

    # If no objects matched the configured prefix/pattern, signal caller to skip stream
    if not found_any:
        return None

    if not samples:
        return {
            'type': 'object',
            'properties': {}
        }

    metadata_schema = {
        SDC_SOURCE_BUCKET_COLUMN: {'type': 'string'},
        SDC_SOURCE_FILE_COLUMN: {'type': 'string'},
        SDC_SOURCE_LINENO_COLUMN: {'type': 'integer'},
        SDC_EXTRA_COLUMN: {'type': 'array', 'items': {
            'anyOf': [{'type': 'object', 'properties': {}}, {'type': 'string'}]}}
    }

    data_schema = conversion.generate_schema(samples, table_spec)

    return {
        'type': 'object',
        'properties': {**data_schema, **metadata_schema}
    }
