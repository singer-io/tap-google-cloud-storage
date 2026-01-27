# tap_google_cloud_storage/gcs.py

import io
import json
import re
import gzip
import struct
import itertools
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
from tap_google_cloud_storage import conversion

LOGGER = singer.get_logger()

# Global GCS filesystem instance for streaming Parquet/Avro files
fs = None

# Global counter for skipped files during discovery and sync
skipped_files_count = 0

SDC_SOURCE_BUCKET_COLUMN = "_sdc_source_bucket"
SDC_SOURCE_FILE_COLUMN = "_sdc_source_file"
SDC_SOURCE_LINENO_COLUMN = "_sdc_source_lineno"
SDC_EXTRA_COLUMN = "_sdc_extra"

def _read_exact(fp, n):
    """Read exactly n bytes from file pointer.
    Helper function for reading gzip headers.
    """
    data = fp.read(n)
    while len(data) < n:
        b = fp.read(n - len(data))
        if not b:
            raise EOFError("Compressed file ended before the "
                           "end-of-stream marker was reached")
        data += b
    return data

def get_file_name_from_gzfile(filename=None, fileobj=None):
    """Read filename from gzip file header.
    Returns the original filename stored in the gzip header,
    or falls back to stripping .gz extension if not present.
    """
    _gz = gzip.GzipFile(filename=filename, fileobj=fileobj)
    _fp = _gz.fileobj

    # Check magic bytes: 0x1f 0x8b
    magic = _fp.read(2)
    if magic == b'':
        return None

    if magic != b'\037\213':
        raise OSError('Not a gzipped file (%r)' % magic)

    (method, flag, _) = struct.unpack("<BBIxx", _read_exact(_fp, 8))
    if method != 8:
        raise OSError('Unknown compression method')

    # Check if filename is stored in header
    if not flag & gzip.FNAME:
        # Not stored in the header, use the filename sans .gz
        fname = _fp.name if hasattr(_fp, 'name') else filename
        if fname:
            return fname[:-3] if fname.endswith('.gz') else fname
        return None

    if flag & gzip.FEXTRA:
        # Read & discard the extra field, if present
        extra_len, = struct.unpack("<H", _read_exact(_fp, 2))
        _read_exact(_fp, extra_len)

    _fname = []  # bytes for fname
    if flag & gzip.FNAME:
        # Read null-terminated string containing the filename
        # RFC 1952 specifies FNAME is encoded in latin1
        while True:
            s = _fp.read(1)
            if not s or s == b'\000':
                break
            _fname.append(s)
        return ''.join([s.decode('latin1') for s in _fname])

    return None

def setup_gcs_client(config):
    """
    Create and return a GCS client using the config directly as service account JSON.
    """
    try:
        # Add token_uri if not present (hardcoded constant for Google OAuth2)
        if 'token_uri' not in config:
            config['token_uri'] = 'https://oauth2.googleapis.com/token'
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

        if modified_since is None or updated >= modified_since:
            matched_files_count += 1
            yield {'key': blob.name, 'last_modified': updated}
    if matched_files_count == 0:
        LOGGER.warning('No files found matching pattern "%s" modified since %s',
                      pattern, modified_since)

def _get_records_for_csv(gcs_path, sample_rate, buffer, table_spec, max_records=1000):
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
            if max_records is not None and sampled_row_count >= max_records:
                break
        current_row += 1

def _get_records_for_jsonl(sample_rate, data_bytes, max_records=1000):
    current_row = 0
    sampled_count = 0
    for row in singer_jsonl.get_row_iterator(io.BytesIO(data_bytes)):
        if (current_row % sample_rate) == 0 and isinstance(row, dict):
            yield row
            sampled_count += 1
            if max_records is not None and sampled_count >= max_records:
                break
        current_row += 1

def _get_records_for_json(sample_rate, data_bytes, max_records=1000):
    try:
        loaded = json.loads(data_bytes.decode('utf-8'))
    except Exception:
        return
    if isinstance(loaded, list):
        sampled_count = 0
        for idx, item in enumerate(loaded):
            if (idx % sample_rate) == 0 and isinstance(item, dict):
                yield item
                sampled_count += 1
                if max_records is not None and sampled_count >= max_records:
                    break
    elif isinstance(loaded, dict):
        # Single JSON object; yield as one sample
        yield loaded

def _get_records_for_parquet(sample_rate, data_bytes, max_records=1000):
    row_idx = 0
    sampled_count = 0
    for row in singer_parquet.get_row_iterator(io.BytesIO(data_bytes)):
        if (row_idx % sample_rate) == 0 and isinstance(row, dict):
            yield row
            sampled_count += 1
            if max_records is not None and sampled_count >= max_records:
                break
        row_idx += 1

def _get_records_for_avro(sample_rate, data_bytes, max_records=1000):
    sampled_count = 0
    for idx, record in enumerate(singer_avro.get_row_iterator(io.BytesIO(data_bytes))):
        if (idx % sample_rate) == 0 and isinstance(record, dict):
            yield record
            sampled_count += 1
            if max_records is not None and sampled_count >= max_records:
                break

def sample_file(table_spec, gcs_path, data, sample_rate, extension, max_records=1000):
    """
    Sample records from a single file based on its extension.

    Args:
        table_spec: Table specification dictionary
        gcs_path: Path to the GCS file
        data: File data as bytes
        sample_rate: Sample every Nth record
        extension: File extension
        max_records: Maximum number of records to sample from this file

    Returns:
        Generator of sampled records
    """
    global skipped_files_count

    lower = gcs_path.lower()

    # Delimited text: CSV/TXT/TSV/PSV
    if extension in ['csv', 'txt', 'tsv', 'psv']:
        ts = dict(table_spec or {})
        if 'delimiter' not in ts or ts.get('delimiter') in (None, ''):
            if extension == 'tsv':
                ts['delimiter'] = '\t'
            elif extension == 'psv':
                ts['delimiter'] = '|'
            else:
                ts['delimiter'] = ','
        return _get_records_for_csv(gcs_path, sample_rate, io.BytesIO(data), ts, max_records)
    elif extension == 'jsonl':
        return _get_records_for_jsonl(sample_rate, data, max_records)
    elif extension == 'json':
        return _get_records_for_json(sample_rate, data, max_records)
    elif extension == 'parquet':
        return _get_records_for_parquet(sample_rate, data, max_records)
    elif extension == 'avro':
        return _get_records_for_avro(sample_rate, data, max_records)
    else:
        LOGGER.warning('"%s" with unsupported extension ".%s" will not be sampled.', gcs_path, extension)
        skipped_files_count += 1
        return []

def sampling_gz_file(table_spec, gcs_path, data, sample_rate, max_records=1000):
    """
    Handle sampling of .gz compressed files.

    Args:
        table_spec: Table specification dictionary
        gcs_path: Path to the GCS file
        data: Compressed file data as bytes
        sample_rate: Sample every Nth record
        max_records: Maximum number of records to sample

    Returns:
        Generator of sampled records or empty list
    """
    global skipped_files_count

    if gcs_path.endswith('.tar.gz'):
        LOGGER.warning('Skipping "%s" file as .tar.gz extension is not supported', gcs_path)
        skipped_files_count += 1
        return []

    try:
        gz_file_obj = gzip.GzipFile(fileobj=io.BytesIO(data))
        gz_data = gz_file_obj.read()

        # Get the original filename from gzip header
        try:
            gz_file_name = get_file_name_from_gzfile(fileobj=io.BytesIO(data))
        except (AttributeError, OSError):
            LOGGER.warning('Skipping "%s" - could not get original file name from gzip header', gcs_path)
            skipped_files_count += 1
            return []

        if not gz_file_name:
            LOGGER.warning('Skipping "%s" - no filename found in gzip header', gcs_path)
            skipped_files_count += 1
            return []

        gz_lower = gz_file_name.lower()

        # Check for nested compression
        if gz_lower.endswith('.gz'):
            LOGGER.warning('Skipping "%s" - nested compression not supported', gcs_path)
            skipped_files_count += 1
            return []

        gz_extension = gz_lower.split('.')[-1]
        full_path = f"{gcs_path}/{gz_file_name}"
        return sample_file(table_spec, full_path, gz_data, sample_rate, gz_extension, max_records)

    except Exception as e:
        LOGGER.warning('Failed to process GZ file %s: %s', gcs_path, e)
        skipped_files_count += 1
        return []

def sampling_zip_file(table_spec, gcs_path, data, sample_rate, max_records=1000):
    """
    Handle sampling of .zip compressed files.

    Args:
        table_spec: Table specification dictionary
        gcs_path: Path to the GCS file
        data: Compressed file data as bytes
        sample_rate: Sample every Nth record
        max_records: Maximum number of records to sample

    Yields:
        dict: Sampled records from files in the zip
    """
    global skipped_files_count

    try:
        decompressed_files = compression.infer(io.BytesIO(data), gcs_path)

        for decompressed_file in decompressed_files:
            de_name = decompressed_file.name
            de_lower = de_name.lower()
            de_extension = de_lower.split('.')[-1]

            # Skip nested compressed files
            if de_extension in ['zip', 'gz', 'tar']:
                LOGGER.warning('Skipping "%s/%s" - nested compression not supported for sampling', gcs_path, de_name)
                skipped_files_count += 1
                continue

            # Sample the extracted file
            full_path = f"{gcs_path}/{de_name}"
            de_data = decompressed_file.read()
            # Yield from each file's samples
            for record in sample_file(table_spec, full_path, de_data, sample_rate, de_extension, max_records):
                yield record

    except Exception as e:
        LOGGER.warning('Failed to process ZIP file %s: %s', gcs_path, e)
        skipped_files_count += 1

def get_files_to_sample(config, gcs_files, max_files):
    """
    Prepare GCS files for sampling, downloading and extracting compressed files.

    Args:
        config: Configuration dictionary
        gcs_files: List of GCS file metadata
        max_files: Maximum number of files to sample

    Returns:
        list: List of file dictionaries ready for sampling
    """
    global skipped_files_count
    sampled_files = []
    bucket_name = config.get('bucket')

    try:
        client = setup_gcs_client(config)
        bucket = client.bucket(bucket_name)
    except Exception as e:
        LOGGER.error('Failed to setup GCS client: %s', e)
        return []

    for gcs_file in gcs_files:
        if len(sampled_files) >= max_files:
            break

        file_key = gcs_file.get('key')
        if not file_key:
            continue

        try:
            blob = bucket.blob(file_key)
            data = blob.download_as_bytes()
        except Exception as e:
            LOGGER.warning('Skipping %s due to download error: %s', file_key, e)
            skipped_files_count += 1
            continue

        file_name = file_key.split("/")[-1]
        lower_name = file_name.lower()

        # Check if file is without extension
        if '.' not in file_name or lower_name == file_name.split('.')[-1]:
            LOGGER.warning('"%s" without extension will not be sampled.', file_key)
            skipped_files_count += 1
            continue

        extension = lower_name.split('.')[-1]

        # Check if file is gzipped even with non-gz extension (magic bytes: 1f 8b)
        is_gzipped = len(data) >= 2 and data[0] == 0x1f and data[1] == 0x8b
        if is_gzipped and extension != 'gz':
            try:
                original_name = get_file_name_from_gzfile(fileobj=io.BytesIO(data))
                if original_name:
                    extension = original_name.lower().split('.')[-1]
                gz_file_obj = gzip.GzipFile(fileobj=io.BytesIO(data))
                data = gz_file_obj.read()
            except Exception as e:
                LOGGER.warning('Failed to decompress gzipped file %s: %s', file_key, e)
                skipped_files_count += 1
                continue

        sampled_files.append({
            'gcs_path': file_key,
            'data': data,
            'extension': extension
        })

    return sampled_files

def sample_files(config, table_spec, gcs_files, sample_rate=5, max_records=1000, max_files=5):
    """
    Sample records from multiple GCS files.

    Args:
        config: Configuration dictionary
        table_spec: Table specification dictionary
        gcs_files: List of GCS file metadata
        sample_rate: Sample every Nth record
        max_records: Maximum total records to sample
        max_files: Maximum number of files to sample

    Yields:
        dict: Sampled records
    """
    global skipped_files_count
    LOGGER.info("Sampling files (max files: %s)", max_files)

    for gcs_file in itertools.islice(get_files_to_sample(config, gcs_files, max_files), max_files):
        gcs_path = gcs_file.get('gcs_path', '')
        data = gcs_file.get('data')
        extension = gcs_file.get('extension')

        LOGGER.info('Sampling %s (max records: %s, sample rate: %s)',
                    gcs_path, max_records, sample_rate)

        try:
            if extension == 'gz':
                yield from itertools.islice(sampling_gz_file(table_spec, gcs_path, data, sample_rate, max_records), max_records)
            elif extension == 'zip':
                yield from itertools.islice(sampling_zip_file(table_spec, gcs_path, data, sample_rate, max_records), max_records)
            else:
                yield from sample_file(table_spec, gcs_path, data, sample_rate, extension, max_records)
        except (UnicodeDecodeError, json.JSONDecodeError):
            LOGGER.warning("Skipping %s file as parsing failed. Verify the extension of the file.", gcs_path)
            skipped_files_count += 1

def get_sampled_schema_for_table(config, table_spec):
    LOGGER.info('Sampling records to determine table schema for table "%s".', table_spec.get('table_name'))

    gcs_files_gen = get_input_files_for_table(config, table_spec)
    samples = [sample for sample in sample_files(config, table_spec, gcs_files_gen)]

    if skipped_files_count:
        LOGGER.warning("%s files got skipped during the last sampling.", skipped_files_count)

    if not samples:
        # Return None to signal no files matched (for discovery to skip this stream)
        # Check if any files were found at all
        if not gcs_files_gen:
            return None
        # Return empty properties for accept everything from data if no samples found
        LOGGER.info("No samples found, returning empty props")
        return {
            'type': 'object',
            'properties': {}
        }

    metadata_schema = {
        SDC_SOURCE_BUCKET_COLUMN: {'type': 'string'},
        SDC_SOURCE_FILE_COLUMN: {'type': 'string'},
        SDC_SOURCE_LINENO_COLUMN: {'type': 'integer'},
        SDC_EXTRA_COLUMN: {
            'type': ['null', 'array'],
            'items': {'type': 'object', 'properties': {}}
        }
    }

    data_schema = conversion.generate_schema(samples, table_spec)

    return {
        'type': 'object',
        'properties': {**data_schema, **metadata_schema}
    }
