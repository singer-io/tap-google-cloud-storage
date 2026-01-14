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


LOGGER = singer.get_logger()
skipped_files_count = 0


def stream_is_selected(mdata_map):
    return mdata_map.get((), {}).get('selected', False)


def sync_stream(config, state, table_spec, stream, sync_start_time):
    table_name = table_spec['table_name']
    modified_since = singer_utils.strptime_with_tz(
        singer.get_bookmark(state, table_name, 'modified_since') or config['start_date']
    )

    LOGGER.info('Syncing table "%s".', table_name)
    LOGGER.info('Getting files modified since %s.', modified_since)

    gcs_files = get_input_files_for_table(config, table_spec, modified_since)

    records_streamed = 0

    for gcs_file in sorted(gcs_files, key=lambda item: item['last_modified']):
        records_streamed += sync_table_file(config, gcs_file['key'], table_spec, stream)
        if gcs_file['last_modified'] < sync_start_time:
            state = singer.write_bookmark(state, table_name, 'modified_since', gcs_file['last_modified'].isoformat())
        else:
            state = singer.write_bookmark(state, table_name, 'modified_since', sync_start_time.isoformat())
        singer.write_state(state)

    if skipped_files_count:
        LOGGER.warn("%s files got skipped during the last sync.", skipped_files_count)

    LOGGER.info('Wrote %s records for table "%s".', records_streamed, table_name)

    return records_streamed


def get_input_files_for_table(config, table_spec, modified_since):
    files = []
    for blob in gcs._iter_matching_blobs(config, table_spec):
        updated = getattr(blob, 'updated', None)
        if not updated:
            continue
        if updated > modified_since:
            files.append({
                'key': blob.name,
                'last_modified': updated
            })
    return files


def _download_blob_bytes(config, gcs_path):
    try:
        client = gcs.setup_gcs_client(config)
        bucket = client.bucket(config['bucket'])
        blob = bucket.blob(gcs_path)
        return blob.download_as_bytes()
    except Exception as exc:
        LOGGER.warning("Skipping %s file due to download error: %s", gcs_path, exc)
        return None


def sync_table_file(config, gcs_path, table_spec, stream):

    extension = gcs_path.split(".")[-1].lower()

    if not extension or gcs_path.lower() == extension:
        LOGGER.warning('"%s" without extension will not be synced.', gcs_path)
        global skipped_files_count
        skipped_files_count = skipped_files_count + 1
        return 0
    try:
        if extension == "zip" or extension == "gz":
            return sync_compressed_file(config, gcs_path, table_spec, stream)
        if extension in ["csv", "jsonl", "txt", "tsv", "psv", "parquet", "avro"]:
            return handle_file(config, gcs_path, table_spec, stream, extension)
        LOGGER.warning('"%s" having the ".%s" extension will not be synced.', gcs_path, extension)
    except (UnicodeDecodeError, json.decoder.JSONDecodeError):
        LOGGER.warning("Skipping %s file as parsing failed. Verify an extension of the file.", gcs_path)
        skipped_files_count = skipped_files_count + 1
    return 0


def handle_file(config, gcs_path, table_spec, stream, extension, file_handler=None):
    if not extension or gcs_path.lower() == extension:
        LOGGER.warning('"%s" without extension will not be synced.', gcs_path)
        global skipped_files_count
        skipped_files_count = skipped_files_count + 1
        return 0
    if extension in ["csv", "txt", "tsv", "psv"]:
        data = file_handler.read() if file_handler else _download_blob_bytes(config, gcs_path)
        if data is None:
            skipped_files_count = skipped_files_count + 1
            return 0
        file_handle = io.BytesIO(data)
        return sync_csv_file(config, file_handle, gcs_path, table_spec, stream)

    if extension == "parquet":
        data = file_handler.read() if file_handler else _download_blob_bytes(config, gcs_path)
        if data is None:
            skipped_files_count = skipped_files_count + 1
            return 0
        file_handle = io.BytesIO(data)
        return sync_parquet_file(config, file_handle, gcs_path, table_spec, stream)

    if extension == "avro":
        data = file_handler.read() if file_handler else _download_blob_bytes(config, gcs_path)
        if data is None:
            skipped_files_count = skipped_files_count + 1
            return 0
        file_handle = io.BytesIO(data)
        return sync_avro_file(config, file_handle, gcs_path, table_spec, stream)

    if extension == "jsonl":
        data = file_handler.read() if file_handler else _download_blob_bytes(config, gcs_path)
        if data is None:
            skipped_files_count = skipped_files_count + 1
            return 0
        file_handle = io.BytesIO(data)
        iterator = jsonl.get_row_iterator(file_handle)
        records = sync_jsonl_file(config, iterator, gcs_path, table_spec, stream)
        if records == 0:
            skipped_files_count = skipped_files_count + 1
            LOGGER.warning('Skipping "%s" file as it is empty', gcs_path)
        return records

    if extension == "zip" or extension == "gz":
        return sync_compressed_file(config, gcs_path, table_spec, stream)

    LOGGER.warning('"%s" having the ".%s" extension will not be synced.', gcs_path, extension)
    skipped_files_count = skipped_files_count + 1
    return 0


def sync_compressed_file(config, gcs_path, table_spec, stream):
    LOGGER.info('Syncing Compressed file "%s".', gcs_path)

    records_streamed = 0
    data = _download_blob_bytes(config, gcs_path)
    if data is None:
        return 0

    decompressed_files = compression.infer(io.BytesIO(data), gcs_path)

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
        global skipped_files_count
        skipped_files_count = skipped_files_count + 1

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
        global skipped_files_count
        skipped_files_count = skipped_files_count + 1

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
            LOGGER.warning("\"%s\" is not found in catalog and its value will be stored in the \"_sdc_extra\" field.", value)
            extra_data = {gcs.SDC_EXTRA_COLUMN: value}
            update_to_write = {**to_write, **extra_data}
        else:
            update_to_write = to_write

        with Transformer() as transformer:
            update_to_write = transformer.transform(update_to_write, stream['schema'], metadata.to_map(stream['metadata']))

        singer.write_record(table_name, update_to_write)
        records_synced += 1

    return records_synced
