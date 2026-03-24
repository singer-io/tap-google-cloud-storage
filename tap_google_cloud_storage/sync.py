import sys
import csv
import io
import json
import gzip

from singer import metadata
from singer import Transformer
from singer import utils as singer_utils

import singer
from singer_encodings import (
    avro,
    compression,
    csv as csv_helper,
    jsonl,
    parquet
)
from tap_google_cloud_storage import gcs
from tap_google_cloud_storage.gcs import get_file_name_from_gzfile
from tap_google_cloud_storage.exceptions import GCSBackoffError, GCSRateLimitError


LOGGER = singer.get_logger()


def stream_is_selected(mdata_map):
    return mdata_map.get((), {}).get('selected', False)


def get_timezone_aware_last_modified(gcs_file):
    """Return a validated, timezone-aware last modified value for a GCS file."""
    last_modified = gcs_file['last_modified']
    if last_modified.tzinfo is None or last_modified.utcoffset() is None:
        raise ValueError(
            f"GCS blob timestamp must be timezone-aware, got: {last_modified}"
        )
    return last_modified


def sync_stream(config, state, table_spec, stream, sync_start_time):
    table_name = table_spec['table_name']
    modified_since = singer_utils.strptime_with_tz(
        singer.get_bookmark(state, table_name, 'modified_since') or config['start_date']
    )

    LOGGER.info('Syncing table "%s".', table_name)
    LOGGER.info('Getting files modified since %s.', modified_since)

    gcs_files = list(gcs.get_input_files_for_table(config, table_spec, modified_since))
    for gcs_file in gcs_files:
        get_timezone_aware_last_modified(gcs_file)

    records_streamed = 0

    for gcs_file in sorted(gcs_files, key=lambda item: item['last_modified']):
        last_modified = get_timezone_aware_last_modified(gcs_file)
        records_streamed += sync_table_file(config, gcs_file['key'], table_spec, stream)
        if last_modified < sync_start_time:
            state = singer.write_bookmark(state, table_name, 'modified_since', last_modified.isoformat())
        else:
            state = singer.write_bookmark(state, table_name, 'modified_since', sync_start_time.isoformat())
        singer.write_state(state)

    if gcs.skipped_files_count:
        LOGGER.warning("%s files got skipped during the last sync.", gcs.skipped_files_count)

    return records_streamed


def sync_table_file(config, gcs_path, table_spec, stream):

    extension = gcs_path.split(".")[-1].lower()

    if not extension or gcs_path.lower() == extension:
        LOGGER.warning('"%s" without extension will not be synced.', gcs_path)
        gcs.skipped_files_count = gcs.skipped_files_count + 1
        return 0
    try:
        if extension == "gz":
            return sync_gz_file(config, gcs_path, table_spec, stream)
        if extension == "zip":
            return sync_compressed_file(config, gcs_path, table_spec, stream)
        if extension in ["csv", "jsonl", "txt", "tsv", "psv", "parquet", "avro"]:
            return handle_file(config, gcs_path, table_spec, stream, extension)
        LOGGER.warning('"%s" having the ".%s" extension will not be synced.', gcs_path, extension)
    except (GCSBackoffError, GCSRateLimitError):
        # Let transient server errors propagate so the caller can surface them.
        # These have already been retried by the backoff decorators in gcs.py.
        raise
    except (UnicodeDecodeError, json.decoder.JSONDecodeError):
        LOGGER.warning("Skipping %s file as parsing failed. Verify an extension of the file.", gcs_path)
        gcs.skipped_files_count = gcs.skipped_files_count + 1
    return 0


def handle_file(config, gcs_path, table_spec, stream, extension, file_handler=None):
    if not extension or gcs_path.lower() == extension:
        LOGGER.warning('"%s" without extension will not be synced.', gcs_path)
        gcs.skipped_files_count = gcs.skipped_files_count + 1
        return 0

    # Check if file is gzipped despite non-gz extension (magic bytes: 1f 8b)
    # This handles files like gz_stored_as_csv.csv which are gzipped but have .csv extension
    if extension in ["csv", "txt", "tsv", "psv", "jsonl"] and not file_handler:
        # For files without a handler (not from zip), check if they're secretly gzipped
        file_handle = gcs.get_gcsfs_file_handle(config, gcs_path)
        if file_handle:
            # Read first 2 bytes to check for gzip magic number
            peek_data = file_handle.read(2)
            file_handle.close()

            if len(peek_data) >= 2 and peek_data[0] == 0x1f and peek_data[1] == 0x8b:
                LOGGER.info('Detected gzipped content in "%s" despite .%s extension, treating as gz file', gcs_path, extension)
                # Treat as a gz file instead
                return sync_gz_file(config, gcs_path, table_spec, stream)

    if extension in ["csv", "txt", "tsv", "psv"]:
        # Use streaming file handle - doesn't load entire file into memory
        file_handle = file_handler if file_handler else gcs.get_file_handle(config, gcs_path)
        if file_handle is None:
            gcs.skipped_files_count = gcs.skipped_files_count + 1
            return 0
        return sync_csv_file(config, file_handle, gcs_path, table_spec, stream)

    if extension == "parquet":
        file_handle = file_handler if file_handler else gcs.get_gcsfs_file_handle(config, gcs_path)
        if file_handle is None:
            gcs.skipped_files_count = gcs.skipped_files_count + 1
            return 0
        return sync_parquet_file(config, file_handle, gcs_path, table_spec, stream)

    if extension == "avro":
        file_handle = file_handler if file_handler else gcs.get_gcsfs_file_handle(config, gcs_path)
        if file_handle is None:
            gcs.skipped_files_count = gcs.skipped_files_count + 1
            return 0
        return sync_avro_file(config, file_handle, gcs_path, table_spec, stream)

    if extension == "jsonl":
        # Use streaming file handle - doesn't load entire file into memory
        file_handle = file_handler if file_handler else gcs.get_file_handle(config, gcs_path)
        if file_handle is None:
            gcs.skipped_files_count = gcs.skipped_files_count + 1
            return 0
        iterator = jsonl.get_row_iterator(file_handle)
        records = sync_jsonl_file(config, iterator, gcs_path, table_spec, stream)
        if records == 0:
            gcs.skipped_files_count = gcs.skipped_files_count + 1
            LOGGER.warning('Skipping "%s" file as it is empty', gcs_path)
        return records

    if extension == "zip" or extension == "gz":
        # If file_handler is provided, it means we're inside a compressed file already
        # Skip nested compression to prevent infinite loops
        if file_handler:
            LOGGER.warning('Skipping "%s" file as it contains nested compression.', gcs_path)
            gcs.skipped_files_count = gcs.skipped_files_count + 1
            return 0
        return sync_compressed_file(config, gcs_path, table_spec, stream)

    LOGGER.warning('"%s" having the ".%s" extension will not be synced.', gcs_path, extension)
    gcs.skipped_files_count = gcs.skipped_files_count + 1
    return 0


def sync_gz_file(config, gcs_path, table_spec, stream, file_handler=None):
    """Handle .gz files by reading the original filename from gzip header."""
    # If file is extracted from zip use file object else get file object from GCS bucket
    file_object = file_handler if file_handler else gcs.get_gcsfs_file_handle(config, gcs_path)
    if file_object is None:
        return 0

    file_bytes = file_object.read()
    gz_file_obj = gzip.GzipFile(fileobj=io.BytesIO(file_bytes))

    try:
        gz_file_name = get_file_name_from_gzfile(fileobj=io.BytesIO(file_bytes))
    except (AttributeError, OSError) as err:
        # If a file is compressed using gzip command with --no-name attribute,
        # It will not return the file name and timestamp. Hence we will skip such files.
        LOGGER.warning('Skipping "%s" file as we did not get the original file name', gcs_path)
        gcs.skipped_files_count = gcs.skipped_files_count + 1
        return 0

    if gz_file_name:
        if gz_file_name.endswith(".gz"):
            LOGGER.warning('Skipping "%s" file as it contains nested compression.', gcs_path)
            gcs.skipped_files_count = gcs.skipped_files_count + 1
            return 0

        gz_file_extension = gz_file_name.split(".")[-1].lower()
        return handle_file(config, gcs_path + "/" + gz_file_name, table_spec, stream, gz_file_extension, io.BytesIO(gz_file_obj.read()))

    LOGGER.warning('Skipping "%s" file - no filename found in gzip header', gcs_path)
    gcs.skipped_files_count = gcs.skipped_files_count + 1
    return 0


def sync_compressed_file(config, gcs_path, table_spec, stream):
    """Handle .zip files by extracting and syncing contents."""
    LOGGER.info('Syncing Compressed file "%s".', gcs_path)

    records_streamed = 0
    # Use gcsfs for streaming compressed files
    file_handle = gcs.get_gcsfs_file_handle(config, gcs_path)
    if file_handle is None:
        return 0

    # Read the file content for decompression
    # Note: compression.infer needs the data, but gcsfs streams it efficiently
    decompressed_files = compression.infer(io.BytesIO(file_handle.read()), gcs_path)

    for decompressed_file in decompressed_files:
        extension = decompressed_file.name.split(".")[-1].lower()

        if extension in ["csv", "jsonl", "gz", "txt", "tsv", "psv"]:
            gcs_file_path = gcs_path + "/" + decompressed_file.name
            records_streamed += handle_file(config, gcs_file_path, table_spec, stream, extension, file_handler=decompressed_file)

    return records_streamed


def sync_csv_file(config, file_handle, gcs_path, table_spec, stream):
    LOGGER.info('Syncing file "%s".', gcs_path)

    bucket = config['bucket']
    table_name = table_spec['table_name']

    try:
        csv.field_size_limit(sys.maxsize)
    except OverflowError:
        # On Windows, C long may be 32-bit; fall back to max 32-bit int
        csv.field_size_limit(2147483647)

    ts = dict(table_spec)
    lower = gcs_path.lower()
    if 'delimiter' not in ts or ts.get('delimiter') in (None, ''):
        if lower.endswith('.tsv'):
            ts['delimiter'] = '\t'
        elif lower.endswith('.psv'):
            ts['delimiter'] = '|'
        else:
            ts['delimiter'] = ','

    if "properties" in stream["schema"]:
        iterator = csv_helper.get_row_iterator(
            file_handle, ts, stream["schema"]["properties"].keys(), True)
    else:
        iterator = csv_helper.get_row_iterator(file_handle, ts, None, True)

    records_synced = 0

    if iterator:
        for row in iterator:
            if len(row) == 0:
                continue

            custom_columns = {
                gcs.SDC_SOURCE_BUCKET_COLUMN: bucket,
                gcs.SDC_SOURCE_FILE_COLUMN: gcs_path,
                gcs.SDC_SOURCE_LINENO_COLUMN: records_synced + 2
            }
            rec = {**row, **custom_columns}

            with Transformer() as transformer:
                to_write = transformer.transform(rec, stream['schema'], metadata.to_map(stream['metadata']))

            singer.write_record(table_name, to_write)
            records_synced += 1
    else:
        LOGGER.warning('Skipping "%s" file as it is empty', gcs_path)
        gcs.skipped_files_count = gcs.skipped_files_count + 1

    return records_synced


def sync_avro_parquet_file(config, iterator, gcs_path, table_spec, stream):
    LOGGER.info('Syncing file "%s".', gcs_path)

    bucket = config['bucket']
    table_name = table_spec['table_name']

    records_synced = 0

    if iterator is not None:
        for row in iterator:

            custom_columns = {
                gcs.SDC_SOURCE_BUCKET_COLUMN: bucket,
                gcs.SDC_SOURCE_FILE_COLUMN: gcs_path,
                gcs.SDC_SOURCE_LINENO_COLUMN: records_synced + 1
            }
            rec = {**row, **custom_columns}

            with Transformer() as transformer:
                to_write = transformer.transform(rec, stream['schema'], metadata.to_map(stream['metadata']))

            singer.write_record(table_name, to_write)
            records_synced += 1
    else:
        LOGGER.warning('Skipping "%s" file as it is empty', gcs_path)
        gcs.skipped_files_count = gcs.skipped_files_count + 1

    return records_synced


def sync_avro_file(config, file_handle, gcs_path, table_spec, stream):
    iterator = avro.get_row_iterator(file_handle)
    return sync_avro_parquet_file(config, iterator, gcs_path, table_spec, stream)


def sync_parquet_file(config, file_handle, gcs_path, table_spec, stream):
    iterator = parquet.get_row_iterator(file_handle)
    return sync_avro_parquet_file(config, iterator, gcs_path, table_spec, stream)


def sync_jsonl_file(config, iterator, gcs_path, table_spec, stream):
    LOGGER.info('Syncing file "%s".', gcs_path)

    bucket = config['bucket']
    table_name = table_spec['table_name']

    records_synced = 0

    for row in iterator:

        custom_columns = {
            gcs.SDC_SOURCE_BUCKET_COLUMN: bucket,
            gcs.SDC_SOURCE_FILE_COLUMN: gcs_path,
            gcs.SDC_SOURCE_LINENO_COLUMN: records_synced + 1
        }
        rec = {**row, **custom_columns}

        with Transformer() as transformer:
            to_write = transformer.transform(rec, stream['schema'], metadata.to_map(stream['metadata']))

        value = [{field: rec[field]} for field in set(rec) - set(to_write)]

        if value:
            # Log only field names, not actual data values
            extra_fields = list(set(rec) - set(to_write))
            LOGGER.warning("File '%s': Fields %s not found in catalog and will be stored in \"_sdc_extra\" field.", gcs_path, extra_fields)
            extra_data = {gcs.SDC_EXTRA_COLUMN: value}
            update_to_write = {**to_write, **extra_data}
        else:
            update_to_write = to_write

        with Transformer() as transformer:
            update_to_write = transformer.transform(update_to_write, stream['schema'], metadata.to_map(stream['metadata']))

        singer.write_record(table_name, update_to_write)
        records_synced += 1

    return records_synced
