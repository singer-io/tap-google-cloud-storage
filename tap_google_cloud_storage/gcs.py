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
    LOGGER.info("Listing files in bucket: %s", bucket_name)
    if not bucket_name:
        LOGGER.error("Bucket not found in config")
        raise ValueError("Bucket not found in config")

    prefix = config.get("root_path", "")

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


def get_sampled_schema_for_table(config, table_spec, max_files=10, sample_rate=100):
    LOGGER.info('Sampling records to determine table schema.')

    samples = []
    bucket_name = config.get('bucket')

    for idx, blob in enumerate(_iter_matching_blobs(config, table_spec)):
        if idx >= max_files:
            break
        name = blob.name
        lower = name.lower()

        try:
            data = blob.download_as_bytes()
        except Exception as e:
            LOGGER.warning('Skipping %s due to download error: %s', name, e)
            continue

        if lower.endswith('.csv') or lower.endswith('.txt'):
            for rec in _get_records_for_csv(name, sample_rate, io.BytesIO(data), table_spec):
                samples.append(rec)
        elif lower.endswith('.jsonl'):
            for rec in _get_records_for_jsonl(sample_rate, data):
                samples.append(rec)
        else:
            LOGGER.warning('"%s" with unsupported extension for sampling; skipping', name)

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
