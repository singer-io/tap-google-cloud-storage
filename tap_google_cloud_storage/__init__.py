# tap_google_cloud_storage/__init__.py

import json
import sys
import singer

from singer import utils as singer_utils
from tap_google_cloud_storage import gcs
from tap_google_cloud_storage.discover import discover_streams

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


@singer_utils.handle_top_exception(LOGGER)
def main():
    args = singer_utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    try:
        file_count = 0
        for file in gcs.list_files_in_bucket(config):
            LOGGER.info("Found file: %s", file.name)
            file_count += 1
        LOGGER.info("Successfully connected to GCS bucket - found %d files", file_count)
    except Exception:
        LOGGER.error("Unable to access GCS bucket")
        raise

    if args.discover:
        do_discover(config)
    else:
        LOGGER.info("Sync not implemented yet")


if __name__ == "__main__":
    main()
