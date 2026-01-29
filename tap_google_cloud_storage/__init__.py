# tap_google_cloud_storage/__init__.py

import json
import sys
import singer

from datetime import datetime
from singer import utils as singer_utils
from singer import metadata
from tap_google_cloud_storage import gcs
from tap_google_cloud_storage.discover import discover_streams
from tap_google_cloud_storage.sync import sync_stream, stream_is_selected
from tap_google_cloud_storage.config import CONFIG_CONTRACT

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "project_id",
    "private_key",
    "client_email",
    "bucket",
    "start_date",
    "tables"
]


def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover_streams(config)
    if not streams:
        raise Exception("No streams found")

    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def do_sync(config, catalog, state, sync_start_time):
    LOGGER.info('Starting sync.')

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        mdata = metadata.to_map(stream['metadata'])
        table_spec = next(s for s in config['tables'] if s['table_name'] == stream_name)
        if not stream_is_selected(mdata):
            continue

        singer.write_state(state)
        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream['schema'], key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(config, state, table_spec, stream, sync_start_time)
        LOGGER.info("%s: Completed sync (%s)", stream_name, counter_value)

    LOGGER.info('Done syncing.')


def validate_table_config(config):
    """Validate and normalize table configuration."""
    # Parse the incoming tables config as JSON
    tables = config.get('tables', [])
    if isinstance(tables, str):
        tables = json.loads(tables)

    for table_config in tables:
        # Normalize search_prefix - remove leading slash
        if search_prefix := table_config.get('search_prefix'):
            if search_prefix.startswith('/'):
                table_config['search_prefix'] = search_prefix[1:]
        else:
            table_config.pop('search_prefix', None)

        # Normalize key_properties to list
        if table_config.get('key_properties') == "" or table_config.get('key_properties') is None:
            table_config['key_properties'] = []
        elif isinstance(table_config.get('key_properties'), str):
            table_config['key_properties'] = [s.strip() for s in table_config['key_properties'].split(',')]

        # Normalize date_overrides to list
        if table_config.get('date_overrides') == "" or table_config.get('date_overrides') is None:
            table_config['date_overrides'] = []
        elif isinstance(table_config.get('date_overrides'), str):
            table_config['date_overrides'] = [s.strip() for s in table_config['date_overrides'].split(',')]

    # Validate with schema contract
    return CONFIG_CONTRACT(tables)


@singer_utils.handle_top_exception(LOGGER)
def main():
    args = singer_utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    # Validate and normalize table configuration
    config['tables'] = validate_table_config(config)

    try:
        file_count = 0
        for file in gcs.list_files_in_bucket(config):
            file_count += 1
        LOGGER.info("Successfully connected to GCS bucket - found %d files", file_count)
    except Exception:
        LOGGER.error("Unable to access GCS bucket")
        raise

    now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    sync_start_time = singer_utils.strptime_with_tz(now_str)

    if args.discover:
        do_discover(config)
    elif args.catalog:
        do_sync(config, args.catalog.to_dict(), args.state, sync_start_time)
    elif args.properties:
        do_sync(config, args.properties, args.state, sync_start_time)


if __name__ == "__main__":
    main()
