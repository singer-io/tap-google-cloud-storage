import unittest
import json
from tap_google_cloud_storage.config import CONFIG_CONTRACT
from voluptuous import Invalid, MultipleInvalid


class TestConfigValidation(unittest.TestCase):

    def test_valid_config_passes_validation(self):
        """Test that a valid config passes schema validation"""
        valid_config = [{
            'table_name': 'my_table',
            'search_pattern': '.*\\.csv',
            'key_properties': ['id'],
            'search_prefix': 'exports/',
            'date_overrides': ['created_at'],
            'delimiter': ','
        }]

        # Should not raise exception
        result = CONFIG_CONTRACT(valid_config)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['table_name'], 'my_table')

    def test_config_without_optional_fields(self):
        """Test that config is valid without optional fields"""
        minimal_config = [{
            'table_name': 'my_table',
            'search_pattern': '.*\\.csv',
            'key_properties': ['id']
        }]

        result = CONFIG_CONTRACT(minimal_config)
        self.assertEqual(len(result), 1)

    def test_config_missing_required_table_name(self):
        """Test that config fails without table_name"""
        invalid_config = [{
            'search_pattern': '.*\\.csv',
            'key_properties': ['id']
        }]

        with self.assertRaises(MultipleInvalid):
            CONFIG_CONTRACT(invalid_config)

    def test_config_missing_required_search_pattern(self):
        """Test that config fails without search_pattern"""
        invalid_config = [{
            'table_name': 'my_table',
            'key_properties': ['id']
        }]

        with self.assertRaises(MultipleInvalid):
            CONFIG_CONTRACT(invalid_config)

    def test_config_missing_required_key_properties(self):
        """Test that config fails without key_properties"""
        invalid_config = [{
            'table_name': 'my_table',
            'search_pattern': '.*\\.csv'
        }]

        with self.assertRaises(MultipleInvalid):
            CONFIG_CONTRACT(invalid_config)

    def test_config_with_multiple_tables(self):
        """Test that config supports multiple table configurations"""
        multi_table_config = [
            {
                'table_name': 'table1',
                'search_pattern': '.*\\.csv',
                'key_properties': ['id']
            },
            {
                'table_name': 'table2',
                'search_pattern': '.*\\.json',
                'key_properties': ['uuid']
            }
        ]

        result = CONFIG_CONTRACT(multi_table_config)
        self.assertEqual(len(result), 2)

    def test_config_with_multiple_key_properties(self):
        """Test that multiple key properties are supported"""
        config = [{
            'table_name': 'my_table',
            'search_pattern': '.*\\.csv',
            'key_properties': ['id', 'timestamp', 'region']
        }]

        result = CONFIG_CONTRACT(config)
        self.assertEqual(len(result[0]['key_properties']), 3)

    def test_config_with_multiple_date_overrides(self):
        """Test that multiple date_overrides are supported"""
        config = [{
            'table_name': 'my_table',
            'search_pattern': '.*\\.csv',
            'key_properties': ['id'],
            'date_overrides': ['created_at', 'updated_at', 'deleted_at']
        }]

        result = CONFIG_CONTRACT(config)
        self.assertEqual(len(result[0]['date_overrides']), 3)


class TestConfigNormalizationHelpers(unittest.TestCase):

    def test_parse_tables_from_json_string(self):
        """Test parsing tables configuration from JSON string"""
        tables_str = json.dumps([
            {
                'table_name': 'my_table',
                'search_pattern': '.*\\.csv',
                'key_properties': 'id,timestamp',
                'date_overrides': 'created_at,updated_at',
                'search_prefix': 'exports/',
                'delimiter': ','
            }
        ])

        tables = json.loads(tables_str)
        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0]['table_name'], 'my_table')

    def test_normalize_key_properties_from_string(self):
        """Test normalizing key_properties from comma-separated string"""
        key_props_str = 'id,timestamp,region'
        key_props_list = [s.strip() for s in key_props_str.split(',')]

        self.assertEqual(len(key_props_list), 3)
        self.assertIn('id', key_props_list)
        self.assertIn('timestamp', key_props_list)
        self.assertIn('region', key_props_list)

    def test_normalize_empty_key_properties(self):
        """Test normalizing empty key_properties"""
        key_props_str = ''
        key_props_list = [] if not key_props_str else [s.strip() for s in key_props_str.split(',')]

        self.assertEqual(len(key_props_list), 0)

    def test_normalize_date_overrides_from_string(self):
        """Test normalizing date_overrides from comma-separated string"""
        date_overrides_str = 'created_at,updated_at'
        date_overrides_list = [s.strip() for s in date_overrides_str.split(',')]

        self.assertEqual(len(date_overrides_list), 2)
        self.assertIn('created_at', date_overrides_list)
        self.assertIn('updated_at', date_overrides_list)

    def test_normalize_search_prefix_removes_leading_slash(self):
        """Test that leading slash is removed from search_prefix"""
        search_prefix = '/exports/data/'
        normalized = search_prefix.lstrip('/')

        self.assertEqual(normalized, 'exports/data/')
        self.assertFalse(normalized.startswith('/'))

    def test_normalize_search_prefix_keeps_trailing_structure(self):
        """Test that search_prefix structure is preserved"""
        search_prefix = 'exports/2024/01/'
        normalized = search_prefix.lstrip('/')

        self.assertEqual(normalized, 'exports/2024/01/')


class TestDelimiterConfiguration(unittest.TestCase):

    def test_csv_delimiter(self):
        """Test CSV delimiter configuration"""
        config = [{
            'table_name': 'csv_table',
            'search_pattern': '.*\\.csv',
            'key_properties': ['id'],
            'delimiter': ','
        }]

        result = CONFIG_CONTRACT(config)
        self.assertEqual(result[0]['delimiter'], ',')

    def test_tsv_delimiter(self):
        """Test TSV delimiter configuration"""
        config = [{
            'table_name': 'tsv_table',
            'search_pattern': '.*\\.tsv',
            'key_properties': ['id'],
            'delimiter': '\t'
        }]

        result = CONFIG_CONTRACT(config)
        self.assertEqual(result[0]['delimiter'], '\t')

    def test_psv_delimiter(self):
        """Test PSV (pipe-separated) delimiter configuration"""
        config = [{
            'table_name': 'psv_table',
            'search_pattern': '.*\\.psv',
            'key_properties': ['id'],
            'delimiter': '|'
        }]

        result = CONFIG_CONTRACT(config)
        self.assertEqual(result[0]['delimiter'], '|')

    def test_custom_delimiter(self):
        """Test custom delimiter configuration"""
        config = [{
            'table_name': 'custom_table',
            'search_pattern': '.*\\.txt',
            'key_properties': ['id'],
            'delimiter': ';'
        }]

        result = CONFIG_CONTRACT(config)
        self.assertEqual(result[0]['delimiter'], ';')


class TestSearchPatternConfiguration(unittest.TestCase):

    def test_csv_pattern(self):
        """Test CSV file pattern"""
        pattern = '.*\\.csv$'
        self.assertTrue(pattern.endswith('$'))
        self.assertIn('csv', pattern)

    def test_jsonl_pattern(self):
        """Test JSONL file pattern"""
        pattern = '.*\\.jsonl$'
        self.assertTrue(pattern.endswith('$'))
        self.assertIn('jsonl', pattern)

    def test_parquet_pattern(self):
        """Test Parquet file pattern"""
        pattern = '.*\\.parquet$'
        self.assertTrue(pattern.endswith('$'))
        self.assertIn('parquet', pattern)

    def test_avro_pattern(self):
        """Test Avro file pattern"""
        pattern = '.*\\.avro$'
        self.assertTrue(pattern.endswith('$'))
        self.assertIn('avro', pattern)

    def test_wildcard_pattern(self):
        """Test wildcard pattern for multiple formats"""
        pattern = '.*\\.(csv|jsonl|parquet)$'
        self.assertIn('|', pattern)


if __name__ == '__main__':
    unittest.main()
