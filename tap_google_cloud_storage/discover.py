import singer
from singer import metadata
from tap_google_cloud_storage import gcs

LOGGER = singer.get_logger()

def discover_streams(config):
    streams = []
    total = 0
    skipped = []

    for table_spec in config.get('tables', []):
        total += 1
        schema = discover_schema(config, table_spec)
        # Skip this stream if no GCS objects matched the spec
        if schema is None:
            LOGGER.info(
                'No objects matched for table "%s" (prefix="%s", pattern="%s"). Skipping.',
                table_spec.get('table_name'),
                table_spec.get('search_prefix', ''),
                table_spec.get('search_pattern', '')
            )
            skipped.append(table_spec.get('table_name'))
            continue
        streams.append({
            'stream': table_spec['table_name'],
            'tap_stream_id': table_spec['table_name'],
            'schema': schema,
            'metadata': load_metadata(table_spec, schema)
        })

    LOGGER.info('Discovery summary: configured=%d, included=%d, skipped=%d%s',
                total, len(streams), len(skipped),
                (f" (skipped: {', '.join(skipped)})" if skipped else ''))
    return streams


def discover_schema(config, table_spec):
    return gcs.get_sampled_schema_for_table(config, table_spec)


def load_metadata(table_spec, schema):
    mdata = metadata.new()

    mdata = metadata.write(
        mdata, (), 'table-key-properties', table_spec.get('key_properties', []))

    # Derive a sensible replication key from schema (date-time fields) or config hints
    props = schema.get('properties', {}) or {}
    date_override_list = table_spec.get('date_overrides') or []
    if isinstance(date_override_list, str):
        date_override_list = [s.strip() for s in date_override_list.split(',') if s.strip()]

    def is_datetime_field(s):
        if not isinstance(s, dict):
            return False
        # direct format
        if s.get('format') == 'date-time':
            return True
        # anyOf contains a date-time option
        for opt in s.get('anyOf', []) or []:
            if isinstance(opt, dict) and opt.get('format') == 'date-time':
                return True
        return False

    candidates = set()
    for name, pschema in props.items():
        if is_datetime_field(pschema):
            candidates.add(name)
    # include configured date_overrides that are present in properties
    for name in date_override_list:
        if name in props:
            candidates.add(name)

    replication_key = None
    if candidates:
        priority = [
            'updatedAt', 'updated_at', 'last_updated', 'lastUpdated',
            'lastModified', 'last_modified', 'modifiedAt', 'modified_at',
            'createdAt', 'created_at'
        ]
        for p in priority:
            if p in candidates:
                replication_key = p
                break
        if replication_key is None:
            replication_key = sorted(candidates)[0]

    if replication_key:
        mdata = metadata.write(mdata, (), 'replication-method', 'INCREMENTAL')

    for field_name in schema.get('properties', {}).keys():
        if table_spec.get('key_properties') and field_name in table_spec.get('key_properties', []):
            mdata = metadata.write(
                mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(
                mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)
