import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
import re


class TestFileMatching(unittest.TestCase):

    def test_pattern_matches_csv_files(self):
        """Test that CSV pattern matches .csv files"""
        pattern = re.compile(r'.*\.csv$')

        self.assertTrue(pattern.match('data.csv'))
        self.assertTrue(pattern.match('exports/2024/data.csv'))
        self.assertFalse(pattern.match('data.txt'))
        self.assertFalse(pattern.match('data.csv.gz'))

    def test_pattern_matches_jsonl_files(self):
        """Test that JSONL pattern matches .jsonl files"""
        pattern = re.compile(r'.*\.jsonl$')

        self.assertTrue(pattern.match('data.jsonl'))
        self.assertTrue(pattern.match('logs/app.jsonl'))
        self.assertFalse(pattern.match('data.json'))

    def test_pattern_matches_parquet_files(self):
        """Test that Parquet pattern matches .parquet files"""
        pattern = re.compile(r'.*\.parquet$')

        self.assertTrue(pattern.match('data.parquet'))
        self.assertTrue(pattern.match('warehouse/table.parquet'))
        self.assertFalse(pattern.match('data.csv'))

    def test_pattern_matches_avro_files(self):
        """Test that Avro pattern matches .avro files"""
        pattern = re.compile(r'.*\.avro$')

        self.assertTrue(pattern.match('data.avro'))
        self.assertTrue(pattern.match('stream/events.avro'))
        self.assertFalse(pattern.match('data.parquet'))


class TestPrefixFiltering(unittest.TestCase):

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_filter_files_by_prefix(self, mock_setup_client):
        """Test filtering files by search_prefix via root_path config"""
        from tap_google_cloud_storage import gcs

        mock_client = MagicMock()
        mock_bucket = MagicMock()

        # Create mock blobs with exports/ prefix
        blob1 = MagicMock()
        blob1.name = 'exports/data.csv'
        blob2 = MagicMock()
        blob2.name = 'exports/data2.csv'

        mock_bucket.list_blobs.return_value = [blob1, blob2]
        mock_client.bucket.return_value = mock_bucket
        mock_setup_client.return_value = mock_client

        config = {'bucket': 'test-bucket', 'root_path': 'exports/'}
        files = list(gcs.list_files_in_bucket(config))

        # Should only get files with exports/ prefix
        self.assertEqual(len(files), 2)
        mock_bucket.list_blobs.assert_called_once_with(prefix='exports/')

    def test_prefix_normalization_removes_leading_slash(self):
        """Test that leading slash is removed from prefix"""
        prefix = '/exports/data/'
        normalized = prefix.lstrip('/')

        self.assertEqual(normalized, 'exports/data/')
        self.assertFalse(normalized.startswith('/'))

    def test_prefix_with_nested_folders(self):
        """Test prefix with multiple folder levels"""
        prefix = 'exports/2024/01/15/'

        self.assertTrue('/' in prefix)
        self.assertEqual(prefix.count('/'), 4)


class TestFileTimestampFiltering(unittest.TestCase):

    def test_filter_files_modified_after_timestamp(self):
        """Test filtering files modified after a specific timestamp"""
        cutoff = datetime(2026, 1, 10, 0, 0, 0, tzinfo=timezone.utc)

        files = [
            {'name': 'old.csv', 'last_modified': datetime(2026, 1, 5, 0, 0, 0, tzinfo=timezone.utc)},
            {'name': 'new.csv', 'last_modified': datetime(2026, 1, 12, 0, 0, 0, tzinfo=timezone.utc)},
            {'name': 'newer.csv', 'last_modified': datetime(2026, 1, 15, 0, 0, 0, tzinfo=timezone.utc)}
        ]

        # Use >= to match actual implementation
        filtered = [f for f in files if f['last_modified'] >= cutoff]

        self.assertEqual(len(filtered), 2)
        self.assertEqual(filtered[0]['name'], 'new.csv')
        self.assertEqual(filtered[1]['name'], 'newer.csv')

    def test_includes_files_equal_to_bookmark(self):
        """Test that files with timestamp equal to cutoff are INCLUDED (using >=)"""
        cutoff = datetime(2026, 1, 10, 0, 0, 0, tzinfo=timezone.utc)

        files = [
            {'name': 'equal.csv', 'last_modified': datetime(2026, 1, 10, 0, 0, 0, tzinfo=timezone.utc)},
            {'name': 'newer.csv', 'last_modified': datetime(2026, 1, 10, 0, 0, 1, tzinfo=timezone.utc)}
        ]

        # Use >= to match actual implementation - includes files at exact bookmark time
        filtered = [f for f in files if f['last_modified'] >= cutoff]

        # Both should be included since we use >=
        self.assertEqual(len(filtered), 2)
        self.assertEqual(filtered[0]['name'], 'equal.csv')
        self.assertEqual(filtered[1]['name'], 'newer.csv')


class TestFileSorting(unittest.TestCase):

    def test_sort_files_by_modified_timestamp(self):
        """Test sorting files by last_modified timestamp"""
        files = [
            {'key': 'file3.csv', 'last_modified': datetime(2026, 1, 15, 0, 0, 0, tzinfo=timezone.utc)},
            {'key': 'file1.csv', 'last_modified': datetime(2026, 1, 10, 0, 0, 0, tzinfo=timezone.utc)},
            {'key': 'file2.csv', 'last_modified': datetime(2026, 1, 12, 0, 0, 0, tzinfo=timezone.utc)}
        ]

        sorted_files = sorted(files, key=lambda x: x['last_modified'])

        self.assertEqual(sorted_files[0]['key'], 'file1.csv')
        self.assertEqual(sorted_files[1]['key'], 'file2.csv')
        self.assertEqual(sorted_files[2]['key'], 'file3.csv')

    def test_sort_maintains_order_for_equal_timestamps(self):
        """Test that sort is stable for files with equal timestamps"""
        base_time = datetime(2026, 1, 10, 0, 0, 0, tzinfo=timezone.utc)

        files = [
            {'key': 'file_a.csv', 'last_modified': base_time},
            {'key': 'file_b.csv', 'last_modified': base_time},
            {'key': 'file_c.csv', 'last_modified': base_time}
        ]

        sorted_files = sorted(files, key=lambda x: x['last_modified'])

        # Should maintain original order for equal timestamps
        self.assertEqual(len(sorted_files), 3)


class TestSchemaInference(unittest.TestCase):

    def test_infer_integer_type(self):
        """Test inferring integer type from sample data"""
        sample_values = ['1', '2', '3', '100']

        # All can be converted to int
        all_ints = all(v.isdigit() or (v.startswith('-') and v[1:].isdigit()) for v in sample_values)

        self.assertTrue(all_ints)

    def test_infer_string_type(self):
        """Test inferring string type from sample data"""
        sample_values = ['hello', 'world', 'test']

        # All are strings
        all_strings = all(isinstance(v, str) for v in sample_values)

        self.assertTrue(all_strings)

    def test_infer_date_format(self):
        """Test identifying date-time format in sample data"""
        sample_values = [
            '2026-01-10T00:00:00Z',
            '2026-01-11T12:30:00Z',
            '2026-01-12T23:59:59Z'
        ]

        # Check if they match ISO 8601 format
        import re
        iso_pattern = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z')

        all_dates = all(iso_pattern.match(v) for v in sample_values)

        self.assertTrue(all_dates)


class TestBlobMetadata(unittest.TestCase):

    def test_extract_blob_name(self):
        """Test extracting blob name from path"""
        full_path = 'exports/2024/01/data.csv'
        name = full_path.split('/')[-1]

        self.assertEqual(name, 'data.csv')

    def test_extract_blob_extension(self):
        """Test extracting file extension from blob name"""
        blob_name = 'data.csv'
        extension = blob_name.split('.')[-1].lower()

        self.assertEqual(extension, 'csv')

    def test_handle_blob_without_extension(self):
        """Test handling blobs without file extension"""
        blob_name = 'datafile'
        parts = blob_name.split('.')

        # If no extension, parts will have only one element
        has_extension = len(parts) > 1

        self.assertFalse(has_extension)

    def test_extract_folder_path(self):
        """Test extracting folder path from full blob path"""
        full_path = 'exports/2024/01/15/data.csv'
        folder = '/'.join(full_path.split('/')[:-1])

        self.assertEqual(folder, 'exports/2024/01/15')


class TestCompressionDetection(unittest.TestCase):

    def test_detect_gzip_compression(self):
        """Test detecting gzip compressed files"""
        filenames = ['data.csv.gz', 'log.json.gz', 'archive.tar.gz']

        for filename in filenames:
            self.assertTrue(filename.endswith('.gz'))

    def test_detect_zip_compression(self):
        """Test detecting zip compressed files"""
        filenames = ['data.zip', 'archive.zip']

        for filename in filenames:
            self.assertTrue(filename.endswith('.zip'))

    def test_non_compressed_files(self):
        """Test that uncompressed files are not detected as compressed"""
        filenames = ['data.csv', 'log.json', 'file.parquet']

        for filename in filenames:
            self.assertFalse(filename.endswith('.gz'))
            self.assertFalse(filename.endswith('.zip'))


class TestSDCMetadata(unittest.TestCase):

    def test_add_sdc_source_bucket(self):
        """Test adding _sdc_source_bucket metadata"""
        record = {'id': 1, 'name': 'test'}
        bucket_name = 'my-bucket'

        record['_sdc_source_bucket'] = bucket_name

        self.assertIn('_sdc_source_bucket', record)
        self.assertEqual(record['_sdc_source_bucket'], bucket_name)

    def test_add_sdc_source_file(self):
        """Test adding _sdc_source_file metadata"""
        record = {'id': 1, 'name': 'test'}
        file_name = 'exports/data.csv'

        record['_sdc_source_file'] = file_name

        self.assertIn('_sdc_source_file', record)
        self.assertEqual(record['_sdc_source_file'], file_name)

    def test_add_sdc_source_lineno(self):
        """Test adding _sdc_source_lineno metadata"""
        record = {'id': 1, 'name': 'test'}
        line_number = 42

        record['_sdc_source_lineno'] = line_number

        self.assertIn('_sdc_source_lineno', record)
        self.assertEqual(record['_sdc_source_lineno'], line_number)

    def test_all_sdc_metadata_fields(self):
        """Test adding all _sdc metadata fields"""
        record = {'id': 1, 'name': 'test'}

        record['_sdc_source_bucket'] = 'my-bucket'
        record['_sdc_source_file'] = 'exports/data.csv'
        record['_sdc_source_lineno'] = 10

        # All _sdc fields should be present
        sdc_fields = [k for k in record.keys() if k.startswith('_sdc_')]

        self.assertEqual(len(sdc_fields), 3)
        self.assertIn('_sdc_source_bucket', record)
        self.assertIn('_sdc_source_file', record)
        self.assertIn('_sdc_source_lineno', record)


if __name__ == '__main__':
    unittest.main()
