import singer
from singer import metadata
from singer import utils as singer_utils
from tap_google_cloud_storage import gcs
from tap_google_cloud_storage.discover import discover_streams


LOGGER = singer.get_logger()


def _normalize_tables_in_config(config):
    cfg = dict(config)
    tables = cfg.get('tables', [])
    if isinstance(tables, str):
        import json
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
    return cfg


def _parse_iso_to_ts(s):
    try:
        from datetime import datetime
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return 0


def metadata_helpers_get_key_props(mdata_list):
    for m in mdata_list or []:
        if not m or 'breadcrumb' not in m:
            continue
        if m['breadcrumb'] == []:
            props = m.get('metadata', {}).get('table-key-properties')
            if props is None:
                return []
            return props
    return []


def find_table_attr(cfg, stream_name, attr):
    for t in cfg.get('tables', []):
        if t.get('table_name') == stream_name:
            return t.get(attr)
    return None


def do_sync(config, state=None, catalog=None):
    LOGGER.info("Starting sync")
    cfg = _normalize_tables_in_config(config)
    # Use provided catalog (with selections) if present; else discover
    try:
        cat = catalog.to_dict() if (catalog and hasattr(catalog, 'to_dict')) else catalog
        streams = cat.get('streams') if isinstance(cat, dict) else None
    except Exception:
        streams = None
    streams = streams or discover_streams(cfg)
    if not streams:
        LOGGER.info("No streams to sync")
        return

    state = state or {}
    from datetime import datetime, timezone
    sync_start_time = datetime.now(timezone.utc)

    for stream in streams:
        stream_name = stream['stream']
        schema = stream['schema']
        key_props = metadata_helpers_get_key_props(stream.get('metadata', []))
        mdata_map = metadata.to_map(stream.get('metadata', []))
        selected_flag = (
            mdata_map.get((), {}).get('selected', False) or
            stream.get('selected', False)
        )
        if not selected_flag:
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)
        singer.write_schema(stream_name, schema, key_props)
        row_count = 0
        table_spec = {
            'table_name': stream_name,
            'search_prefix': find_table_attr(cfg, stream_name, 'search_prefix'),
            'search_pattern': find_table_attr(cfg, stream_name, 'search_pattern'),
            'delimiter': find_table_attr(cfg, stream_name, 'delimiter'),
            'date_overrides': find_table_attr(cfg, stream_name, 'date_overrides'),
            'key_properties': key_props,
        }

        # Determine replication key (record-level) from metadata, else fallback to file-level
        mdata_map = metadata.to_map(stream.get('metadata', []))
        replication_key = (
            mdata_map.get((), {}).get('replication-key') or
            mdata_map.get((), {}).get('replication_key')
        )

        def _parse_iso_maybe_z(s: str):
            if not s:
                return None
            if s.endswith('Z'):
                s = s[:-1] + '+00:00'
            return singer_utils.strptime_with_tz(s)

        def _z(dt):
            return dt.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')

        # Initialize bookmark value depending on mode
        if replication_key:
            # Record-level: read last value from bookmark under the replication_key
            rk_bm_str = singer.get_bookmark(state, stream_name, replication_key)
            if rk_bm_str:
                since_dt = _parse_iso_maybe_z(rk_bm_str)
            else:
                # Back-compat: accept updatedAt/modified_since/last_modified_ts
                updated_at_str = singer.get_bookmark(state, stream_name, 'updatedAt')
                modified_since_str = singer.get_bookmark(state, stream_name, 'modified_since')
                legacy_ts = None
                try:
                    legacy_ts = state.get('bookmarks', {}).get(stream_name, {}).get('last_modified_ts')
                except Exception:
                    legacy_ts = None
                if updated_at_str:
                    since_dt = _parse_iso_maybe_z(updated_at_str)
                elif modified_since_str:
                    since_dt = singer_utils.strptime_with_tz(modified_since_str)
                elif legacy_ts is not None:
                    since_dt = datetime.fromtimestamp(int(legacy_ts), tz=timezone.utc)
                else:
                    start_date = cfg.get('start_date') or '1970-01-01T00:00:00Z'
                    since_dt = _parse_iso_maybe_z(start_date)
            LOGGER.info('Filtering records where %s > %s.', replication_key, since_dt)
        else:
            # File-level: tap-s3-csv semantics using 'modified_since' bookmark
            modified_since_str = singer.get_bookmark(state, stream_name, 'modified_since')
            updated_at_str = singer.get_bookmark(state, stream_name, 'updatedAt')  # back-compat
            legacy_ts = None
            try:
                legacy_ts = state.get('bookmarks', {}).get(stream_name, {}).get('last_modified_ts')
            except Exception:
                legacy_ts = None
            if modified_since_str:
                since_dt = singer_utils.strptime_with_tz(modified_since_str)
            elif updated_at_str:
                since_dt = _parse_iso_maybe_z(updated_at_str)
                # migrate to modified_since
                state = singer.write_bookmark(state, stream_name, 'modified_since', since_dt.isoformat())
                try:
                    if 'bookmarks' in state and stream_name in state['bookmarks']:
                        state['bookmarks'][stream_name].pop('updatedAt', None)
                except Exception:
                    pass
                singer.write_state(state)
            elif legacy_ts is not None:
                since_dt = datetime.fromtimestamp(int(legacy_ts), tz=timezone.utc)
                state = singer.write_bookmark(state, stream_name, 'modified_since', since_dt.isoformat())
                try:
                    if 'bookmarks' in state and stream_name in state['bookmarks']:
                        state['bookmarks'][stream_name].pop('last_modified_ts', None)
                except Exception:
                    pass
                singer.write_state(state)
            else:
                start_date = cfg.get('start_date') or '1970-01-01T00:00:00Z'
                since_dt = _parse_iso_maybe_z(start_date)

            LOGGER.info('Getting files modified since %s.', since_dt)

        # Collect and sort blobs by updated time to mirror tap-s3-csv
        files = []
        for blob in gcs._iter_matching_blobs(cfg, table_spec):
            updated = getattr(blob, 'updated', None)
            if not updated:
                continue
            # Only process if strictly greater than modified_since
            if updated <= since_dt:
                continue
            files.append({'blob': blob, 'updated': updated})

        max_rk_seen = None  # track max replication key observed
        for item in sorted(files, key=lambda x: x['updated']):
            blob = item['blob']
            updated = item['updated']

            if replication_key:
                for record in gcs.iter_records_for_blob(cfg, table_spec, blob):
                    rk_val = record.get(replication_key)
                    # Skip records without replication key
                    if rk_val is None:
                        continue
                    # Attempt to parse value as datetime
                    try:
                        rk_dt = _parse_iso_maybe_z(str(rk_val))
                    except Exception:
                        rk_dt = None
                    if rk_dt is None:
                        continue
                    if rk_dt <= since_dt:
                        continue
                    singer.write_record(stream_name, record)
                    row_count += 1
                    if (max_rk_seen is None) or (rk_dt > max_rk_seen):
                        max_rk_seen = rk_dt
                # After each file, persist current bookmark if advanced
                if max_rk_seen is not None:
                    new_dt = max_rk_seen if max_rk_seen < sync_start_time else sync_start_time
                    state = singer.write_bookmark(state, stream_name, replication_key, _z(new_dt))
                    singer.write_state(state)
            else:
                for record in gcs.iter_records_for_blob(cfg, table_spec, blob):
                    singer.write_record(stream_name, record)
                    row_count += 1
                # Update bookmark after each file, capped by sync_start_time, using 'modified_since'
                new_bm_time = updated if updated < sync_start_time else sync_start_time
                state = singer.write_bookmark(state, stream_name, 'modified_since', new_bm_time.isoformat())
                singer.write_state(state)

        LOGGER.info("Stream '%s' synced %d records", stream_name, row_count)

    LOGGER.info("Finished sync")
