# tap_google_cloud_storage/gcs.py

import io
import json
import re
import gzip
import singer
from google.cloud import storage
import gcsfs
from singer_encodings import (
    csv as singer_csv,
    jsonl as singer_jsonl,
    parquet as singer_parquet,
    avro as singer_avro,
    compression
)
from tap_google_cloud_storage import conversion as conversion  # type: ignore

LOGGER = singer.get_logger()

# Global GCS filesystem instance for streaming Parquet/Avro files
fs = None

# Global counter for skipped files during discovery and sync
skipped_files_count = 0

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


def setup_gcsfs_client(config):
    """
    Setup GCS filesystem client for streaming Parquet/Avro files.
    Uses gcsfs which provides a filesystem interface with random access support.
    """
    global fs
    if fs is None:
        try:
            LOGGER.info('Creating GCS filesystem client for streaming Parquet/Avro')
            # gcsfs can use the same service account credentials
            fs = gcsfs.GCSFileSystem(token=config, project=config.get('project_id'))
        except Exception as e:
            LOGGER.error("Failed to create GCS filesystem client: %s", e)
            raise
    return fs


def list_files_in_bucket(config):
    """
    Generator to list files in a GCS bucket.
    Used to validate connectivity & permissions.
    """
    client = setup_gcs_client(config)
    bucket_name = config.get("bucket")
    prefix = config.get("root_path", "")
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


def get_file_handle(config, gcs_path):
    """
    Get a streaming file handle for CSV/JSONL files.
    Returns a file-like object that can be read in chunks without loading entire file into memory.
    """
    try:
        client = setup_gcs_client(config)
        bucket = client.bucket(config['bucket'])
        blob = bucket.blob(gcs_path)
        # Open blob as streaming reader - doesn't load entire file into memory
        return blob.open('rb')
    except Exception as exc:
        LOGGER.warning("Failed to open streaming handle for %s: %s", gcs_path, exc)
        return None


def get_gcsfs_file_handle(config, gcs_path):
    """
    Get a streaming file handle for Parquet/Avro/compressed files using gcsfs.
    This provides random access (seeking) without downloading the entire file.
    """
    try:
        bucket = config['bucket']
        fs_client = setup_gcsfs_client(config)
        # Open file with gcsfs - supports streaming with random access
        return fs_client.open(f'gs://{bucket}/{gcs_path}', 'rb')
    except Exception as exc:
        LOGGER.warning("Failed to open streaming handle for %s: %s", gcs_path, exc)
        return None


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


def get_input_files_for_table(config, table_spec, modified_since=None):
    """
    Get all files matching the table spec pattern and modified since the given timestamp.
    Yields dictionaries with 'key' and 'last_modified' for each matching file.
    """
    table_name = table_spec.get('table_name', '')
    pattern = table_spec.get('search_pattern', '')

    matched_files_count = 0

    for blob in _iter_matching_blobs(config, table_spec):
        updated = getattr(blob, 'updated', None)
        if not updated:
            LOGGER.debug('Skipping blob "%s" - no updated timestamp', blob.name)
            continue

        if modified_since is None or updated > modified_since:
            matched_files_count += 1
            yield {'key': blob.name, 'last_modified': updated}
    if matched_files_count == 0:
        LOGGER.warning('No files found matching pattern "%s" modified since %s',
                      pattern, modified_since)


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
    for row in singer_jsonl.get_row_iterator(io.BytesIO(data_bytes)):
        if (current_row % sample_rate) == 0 and isinstance(row, dict):
            yield row
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
    row_idx = 0
    for row in singer_parquet.get_row_iterator(io.BytesIO(data_bytes)):
        if (row_idx % sample_rate) == 0 and isinstance(row, dict):
            yield row
        row_idx += 1


def _get_records_for_avro(sample_rate, data_bytes):
    for idx, record in enumerate(singer_avro.get_row_iterator(io.BytesIO(data_bytes))):
        if (idx % sample_rate) == 0 and isinstance(record, dict):
            yield record


def iter_records_for_blob(config, table_spec, blob):
    """Yield records for a single blob for a table, adding _sdc metadata."""
    bucket_name = config.get('bucket')
    name = blob.name
    lower = name.lower()
    try:
        data = blob.download_as_bytes()
    except Exception as e:
        LOGGER.warning('Skipping %s due to download error during sync: %s', name, e)
        return

    row_idx = 0
    def add_meta(rec, lineno):
        if not isinstance(rec, dict):
            return None
        rec_with_meta = dict(rec)
        rec_with_meta[SDC_SOURCE_BUCKET_COLUMN] = bucket_name
        rec_with_meta[SDC_SOURCE_FILE_COLUMN] = name
        rec_with_meta[SDC_SOURCE_LINENO_COLUMN] = lineno
        return rec_with_meta

    if lower.endswith('.csv') or lower.endswith('.txt') or lower.endswith('.tsv') or lower.endswith('.psv'):
        ts = dict(table_spec or {})
        if 'delimiter' not in ts or ts.get('delimiter') in (None, ''):
            if lower.endswith('.tsv'):
                ts['delimiter'] = '\t'
            elif lower.endswith('.psv'):
                ts['delimiter'] = '|'
            else:
                ts['delimiter'] = ','
        iterator = singer_csv.get_row_iterator(io.BytesIO(data), ts, None, True)
        for rec in iterator or []:
            row_idx += 1
            # _sdc_extra may exist from singer_encodings CSV helper; include as-is
            out = add_meta(rec, row_idx)
            if out is not None:
                yield out
    elif lower.endswith('.jsonl'):
        for rec in singer_jsonl.get_row_iterator(io.BytesIO(data)):
            row_idx += 1
            out = add_meta(rec, row_idx)
            if out is not None:
                yield out
    elif lower.endswith('.json'):
        try:
            loaded = json.loads(data.decode('utf-8'))
        except Exception:
            loaded = None
        if isinstance(loaded, list):
            for rec in loaded:
                row_idx += 1
                out = add_meta(rec, row_idx)
                if out is not None:
                    yield out
        elif isinstance(loaded, dict):
            row_idx = 1
            out = add_meta(loaded, row_idx)
            if out is not None:
                yield out
    elif lower.endswith('.parquet'):
        for rec in singer_parquet.get_row_iterator(io.BytesIO(data)):
            row_idx += 1
            out = add_meta(rec, row_idx)
            if out is not None:
                yield out
    elif lower.endswith('.avro'):
        for rec in singer_avro.get_row_iterator(io.BytesIO(data)):
            row_idx += 1
            out = add_meta(rec, row_idx)
            if out is not None:
                yield out


def iter_records_for_table(config, table_spec):
    """Yield records for all matching blobs for a table, adding _sdc metadata."""
    for blob in _iter_matching_blobs(config, table_spec):
        yield from iter_records_for_blob(config, table_spec, blob)


def get_sampled_schema_for_table(config, table_spec, max_files=10, sample_rate=100):
    LOGGER.info('Sampling records to determine table schema for table "%s".', table_spec.get('table_name'))

    samples = []
    bucket_name = config.get('bucket')
    found_any = False
    global skipped_files_count

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
            skipped_files_count += 1
            continue

        # Handle ZIP files - extract and sample contents
        if lower.endswith('.zip'):
            try:
                from singer_encodings import compression
                decompressed_files = compression.infer(io.BytesIO(data), name)
                for decompressed_file in decompressed_files:
                    de_name = decompressed_file.name
                    de_lower = de_name.lower()
                    de_extension = de_lower.split('.')[-1]

                    # Skip nested compressed files
                    if de_extension in ['zip', 'gz', 'tar']:
                        LOGGER.warning('Skipping "%s/%s" - nested compression not supported for sampling', name, de_name)
                        skipped_files_count += 1
                        continue

                    # Sample the extracted file
                    if de_lower.endswith('.csv') or de_lower.endswith('.txt') or de_lower.endswith('.tsv') or de_lower.endswith('.psv'):
                        ts = dict(table_spec or {})
                        if 'delimiter' not in ts or ts.get('delimiter') in (None, ''):
                            if de_lower.endswith('.tsv'):
                                ts['delimiter'] = '\t'
                            elif de_lower.endswith('.psv'):
                                ts['delimiter'] = '|'
                            else:
                                ts['delimiter'] = ','
                        for rec in _get_records_for_csv(f"{name}/{de_name}", sample_rate, decompressed_file, ts):
                            samples.append(rec)
                    elif de_lower.endswith('.jsonl'):
                        de_data = decompressed_file.read()
                        for rec in _get_records_for_jsonl(sample_rate, de_data):
                            samples.append(rec)
                    else:
                        LOGGER.warning('Skipping "%s/%s" - unsupported file type inside ZIP', name, de_name)
                        skipped_files_count += 1
            except Exception as e:
                LOGGER.warning('Failed to process ZIP file %s: %s', name, e)
                skipped_files_count += 1
            continue

        # Handle GZ files - decompress and sample
        if lower.endswith('.gz'):
            if lower.endswith('.tar.gz'):
                LOGGER.warning('Skipping "%s" - .tar.gz not supported for sampling', name)
                skipped_files_count += 1
                continue
            try:
                import gzip as gzip_lib
                gz_file_obj = gzip_lib.GzipFile(fileobj=io.BytesIO(data))
                gz_data = gz_file_obj.read()

                # Try to determine the original file name
                try:
                    from tap_google_cloud_storage import utils
                    gz_file_name = utils.get_file_name_from_gzfile(fileobj=io.BytesIO(data))
                except:
                    # If can't get original name, use the .gz filename without extension
                    gz_file_name = name[:-3] if name.endswith('.gz') else name

                gz_lower = gz_file_name.lower()

                # Check for nested compression
                if gz_lower.endswith('.gz'):
                    LOGGER.warning('Skipping "%s" - nested compression not supported', name)
                    skipped_files_count += 1
                    continue

                # Sample the decompressed file
                if gz_lower.endswith('.csv') or gz_lower.endswith('.txt') or gz_lower.endswith('.tsv') or gz_lower.endswith('.psv'):
                    ts = dict(table_spec or {})
                    if 'delimiter' not in ts or ts.get('delimiter') in (None, ''):
                        if gz_lower.endswith('.tsv'):
                            ts['delimiter'] = '\t'
                        elif gz_lower.endswith('.psv'):
                            ts['delimiter'] = '|'
                        else:
                            ts['delimiter'] = ','
                    for rec in _get_records_for_csv(f"{name}/{gz_file_name}", sample_rate, io.BytesIO(gz_data), ts):
                        samples.append(rec)
                elif gz_lower.endswith('.jsonl'):
                    for rec in _get_records_for_jsonl(sample_rate, gz_data):
                        samples.append(rec)
                else:
                    LOGGER.warning('Skipping "%s" - unsupported file type inside GZ', name)
                    skipped_files_count += 1
            except Exception as e:
                LOGGER.warning('Failed to process GZ file %s: %s', name, e)
                skipped_files_count += 1
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
            skipped_files_count += 1

    # If no objects matched the configured prefix/pattern, signal caller to skip stream
    if not found_any:
        return None

    if skipped_files_count:
        LOGGER.warning("%s files got skipped during sampling.", skipped_files_count)

    if not samples:
        LOGGER.info("No samples found, returning empty properties")
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
