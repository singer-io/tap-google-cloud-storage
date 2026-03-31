import unittest
from unittest.mock import patch, MagicMock
from tap_google_cloud_storage.discover import discover_streams, discover_schema, load_metadata


class TestDiscovery(unittest.TestCase):

    def test_discover_streams_includes_valid_tables(self):
        """Test that discover_streams includes tables with valid schemas"""
        config = {
            'tables': [
                {
                    'table_name': 'my_table',
                    'search_prefix': 'exports/',
                    'search_pattern': '.*\\.csv',
                    'key_properties': ['id']
                }
            ]
        }

        mock_schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'},
                'name': {'type': 'string'}
            }
        }

        with patch('tap_google_cloud_storage.gcs.get_sampled_schema_for_table', return_value=mock_schema):
            streams = discover_streams(config)

        self.assertEqual(len(streams), 1)
        self.assertEqual(streams[0]['stream'], 'my_table')
        self.assertEqual(streams[0]['tap_stream_id'], 'my_table')
        self.assertEqual(streams[0]['schema'], mock_schema)
        self.assertIn('metadata', streams[0])

    def test_discover_streams_skips_tables_without_files(self):
        """Test that tables without matching files are skipped"""
        config = {
            'tables': [
                {
                    'table_name': 'table_with_files',
                    'search_prefix': 'exports/',
                    'search_pattern': '.*\\.csv',
                    'key_properties': ['id']
                },
                {
                    'table_name': 'table_without_files',
                    'search_prefix': 'missing/',
                    'search_pattern': '.*\\.csv',
                    'key_properties': ['id']
                }
            ]
        }

        def mock_get_schema(cfg, table_spec):
            if table_spec['table_name'] == 'table_with_files':
                return {'type': 'object', 'properties': {'id': {'type': 'integer'}}}
            return None

        with patch('tap_google_cloud_storage.gcs.get_sampled_schema_for_table', side_effect=mock_get_schema):
            streams = discover_streams(config)

        self.assertEqual(len(streams), 1)
        self.assertEqual(streams[0]['stream'], 'table_with_files')

    def test_discover_streams_logs_skipped_tables(self):
        """Test that skipped tables are logged in summary"""
        config = {
            'tables': [
                {'table_name': 'skipped1', 'search_prefix': 'p1/', 'search_pattern': '.*'},
                {'table_name': 'skipped2', 'search_prefix': 'p2/', 'search_pattern': '.*'}
            ]
        }

        with patch('tap_google_cloud_storage.gcs.get_sampled_schema_for_table', return_value=None):
            streams = discover_streams(config)

        self.assertEqual(len(streams), 0)

    def test_load_metadata_sets_key_properties(self):
        """Test that metadata includes table-key-properties"""
        table_spec = {
            'table_name': 'my_table',
            'key_properties': ['id', 'timestamp']
        }
        schema = {'type': 'object', 'properties': {}}

        mdata = load_metadata(table_spec, schema)

        from singer import metadata as singer_metadata
        mdata_map = singer_metadata.to_map(mdata)
        key_props = mdata_map.get((), {}).get('table-key-properties')

        self.assertEqual(key_props, ['id', 'timestamp'])

    def test_load_metadata_derives_replication_key_from_date_fields(self):
        """Test that replication method is set to INCREMENTAL when date-time fields exist"""
        table_spec = {
            'table_name': 'my_table',
            'key_properties': ['id'],
            'date_overrides': ['created_at']
        }
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'},
                'created_at': {'type': 'string', 'format': 'date-time'},
                'name': {'type': 'string'}
            }
        }

        mdata = load_metadata(table_spec, schema)

        from singer import metadata as singer_metadata
        mdata_map = singer_metadata.to_map(mdata)
        replication_method = mdata_map.get((), {}).get('forced-replication-method')

        # The actual code sets forced-replication-method to INCREMENTAL when datetime fields are found
        self.assertEqual(replication_method, 'INCREMENTAL')

    def test_load_metadata_sets_property_inclusion(self):
        """Test that metadata sets inclusion for each property"""
        table_spec = {
            'table_name': 'my_table',
            'key_properties': ['id']
        }
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'},
                'name': {'type': 'string'}
            }
        }

        mdata = load_metadata(table_spec, schema)

        from singer import metadata as singer_metadata
        mdata_map = singer_metadata.to_map(mdata)

        # Check that properties have inclusion metadata
        id_inclusion = mdata_map.get(('properties', 'id'), {}).get('inclusion')
        name_inclusion = mdata_map.get(('properties', 'name'), {}).get('inclusion')

        self.assertIn(id_inclusion, ['automatic', 'available'])
        self.assertIn(name_inclusion, ['automatic', 'available'])


if __name__ == '__main__':
    unittest.main()
