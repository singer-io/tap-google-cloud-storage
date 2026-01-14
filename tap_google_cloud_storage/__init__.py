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

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "type",
    "project_id",
    "private_key_id",
    "private_key",
    "client_email",
    "bucket"
]


def do_discover(config):
    LOGGER.info("Starting discover")

    cfg = dict(config)
    tables = cfg.get('tables', [])
    if isinstance(tables, str):
        tables = json.loads(tables)

    for table in tables:
        search_prefix = table.get('search_prefix')
        if search_prefix:
            if search_prefix.startswith('/'):
                table['search_prefix'] = search_prefix[1:]
        else:
            table.pop('search_prefix', None)

        key_props = table.get('key_properties')
        if key_props == "" or key_props is None:
            table['key_properties'] = []
        elif isinstance(key_props, str):
            table['key_properties'] = [s.strip() for s in key_props.split(',')]

        date_overrides = table.get('date_overrides')
        if date_overrides == "" or date_overrides is None:
            table['date_overrides'] = []
        elif isinstance(date_overrides, str):
            table['date_overrides'] = [s.strip() for s in date_overrides.split(',')]

    cfg['tables'] = tables

    streams = discover_streams(cfg)
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
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    LOGGER.info('Done syncing.')


@singer_utils.handle_top_exception(LOGGER)
def main():
    args = singer_utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    def _validate_tables(cfg):
        tables = cfg.get('tables', [])
        if isinstance(tables, str):
            try:
                tables = json.loads(tables)
            except Exception:
                pass
        for t in tables:
            sp = t.get('search_prefix')
            if sp:
                if sp.startswith('/'):
                    t['search_prefix'] = sp[1:]
            else:
                t.pop('search_prefix', None)
            kp = t.get('key_properties')
            if kp == "" or kp is None:
                t['key_properties'] = []
            elif isinstance(kp, str):
                t['key_properties'] = [s.strip() for s in kp.split(',')]
            do = t.get('date_overrides')
            if do == "" or do is None:
                t['date_overrides'] = []
            elif isinstance(do, str):
                t['date_overrides'] = [s.strip() for s in do.split(',')]
        cfg['tables'] = tables
        return cfg

    # Normalize tables to ensure key_properties is a list, etc.
    config = _validate_tables(config)

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
