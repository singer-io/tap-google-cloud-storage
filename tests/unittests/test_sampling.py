"""
Unit tests for sampling functionality in tap-google-cloud-storage.
Tests sample_rate, max_records, and max_files parameters.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import io


class TestSamplingParameters(unittest.TestCase):
    """Test sampling parameter behavior"""

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_sample_rate_controls_record_selection(self, mock_client):
        """Test that sample_rate=5 samples every 5th record"""
        from tap_google_cloud_storage.gcs import _get_records_for_csv

        # Create CSV data with 20 rows
        csv_data = "id,name\n"
        for i in range(20):
            csv_data += f"{i},name{i}\n"

        table_spec = {'delimiter': ','}
        buffer = io.BytesIO(csv_data.encode('utf-8'))

        # Sample with rate=5, max_records=None
        samples = list(_get_records_for_csv('test.csv', sample_rate=5, buffer=buffer, table_spec=table_spec, max_records=None))

        # Should get rows 0, 5, 10, 15 (4 records)
        self.assertEqual(len(samples), 4)
        self.assertEqual(samples[0]['id'], '0')
        self.assertEqual(samples[1]['id'], '5')
        self.assertEqual(samples[2]['id'], '10')
        self.assertEqual(samples[3]['id'], '15')

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_sample_rate_1_samples_every_record(self, mock_client):
        """Test that sample_rate=1 samples every record"""
        from tap_google_cloud_storage.gcs import _get_records_for_jsonl

        # Create JSONL data with 10 records
        jsonl_data = ""
        for i in range(10):
            jsonl_data += f'{{"id": {i}}}\n'

        samples = list(_get_records_for_jsonl(sample_rate=1, data_bytes=jsonl_data.encode('utf-8'), max_records=None))

        # Should get all 10 records
        self.assertEqual(len(samples), 10)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_max_records_limits_total_samples(self, mock_client):
        """Test that max_records limits the total number of sampled records"""
        from tap_google_cloud_storage.gcs import _get_records_for_csv

        # Create CSV data with 100 rows
        csv_data = "id,name\n"
        for i in range(100):
            csv_data += f"{i},name{i}\n"

        table_spec = {'delimiter': ','}
        buffer = io.BytesIO(csv_data.encode('utf-8'))

        # Sample with rate=5, max_records=3
        samples = list(_get_records_for_csv('test.csv', sample_rate=5, buffer=buffer, table_spec=table_spec, max_records=3))

        # Should stop at 3 records even though more would match
        self.assertEqual(len(samples), 3)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_max_records_with_jsonl(self, mock_client):
        """Test max_records with JSONL format"""
        from tap_google_cloud_storage.gcs import _get_records_for_jsonl

        # Create JSONL data with 50 records
        jsonl_data = ""
        for i in range(50):
            jsonl_data += f'{{"id": {i}, "value": "test{i}"}}\n'

        # Sample every 2nd record, max 5 records
        samples = list(_get_records_for_jsonl(sample_rate=2, data_bytes=jsonl_data.encode('utf-8'), max_records=5))

        # Should get exactly 5 records (rows 0, 2, 4, 6, 8)
        self.assertEqual(len(samples), 5)
        self.assertEqual(samples[0]['id'], 0)
        self.assertEqual(samples[4]['id'], 8)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_max_records_with_json_array(self, mock_client):
        """Test max_records with JSON array format"""
        from tap_google_cloud_storage.gcs import _get_records_for_json
        import json

        # Create JSON array with 30 objects
        json_data = json.dumps([{"id": i, "name": f"item{i}"} for i in range(30)])

        # Sample every 3rd record, max 4 records
        samples = list(_get_records_for_json(sample_rate=3, data_bytes=json_data.encode('utf-8'), max_records=4))

        # Should get exactly 4 records (indices 0, 3, 6, 9)
        self.assertEqual(len(samples), 4)
        self.assertEqual(samples[0]['id'], 0)
        self.assertEqual(samples[3]['id'], 9)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    @patch('tap_google_cloud_storage.gcs.list_files_in_bucket')
    def test_max_files_limits_file_processing(self, mock_list_files, mock_client):
        """Test that max_files limits the number of files processed"""
        from tap_google_cloud_storage.gcs import get_files_to_sample

        # Mock 10 files in bucket
        mock_blobs = []
        for i in range(10):
            blob = Mock()
            blob.name = f'file{i}.csv'
            blob.download_as_bytes = Mock(return_value=b'id,name\n1,test\n')
            mock_blobs.append(blob)

        mock_bucket = Mock()
        mock_bucket.blob = Mock(side_effect=lambda name: next(b for b in mock_blobs if b.name == name))
        mock_client.return_value.bucket = Mock(return_value=mock_bucket)

        config = {'bucket': 'test-bucket'}
        gcs_files = [{'key': f'file{i}.csv'} for i in range(10)]

        # Process with max_files=3
        sampled_files = get_files_to_sample(config, gcs_files, max_files=3)

        # Should only process 3 files
        self.assertEqual(len(sampled_files), 3)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    @patch('tap_google_cloud_storage.gcs.get_input_files_for_table')
    @patch('tap_google_cloud_storage.gcs.get_files_to_sample')
    def test_sample_files_respects_max_files(self, mock_get_files, mock_get_input, mock_client):
        """Test that sample_files respects max_files parameter"""
        from tap_google_cloud_storage.gcs import sample_files

        # Mock 20 files available
        mock_get_input.return_value = [{'key': f'file{i}.csv'} for i in range(20)]

        # Mock get_files_to_sample to return processed files
        mock_get_files.return_value = [
            {'gcs_path': f'file{i}.csv', 'data': b'id\n1\n2\n3\n', 'extension': 'csv'}
            for i in range(20)
        ]

        config = {'bucket': 'test-bucket'}
        table_spec = {'table_name': 'test', 'delimiter': ','}
        gcs_files = [{'key': f'file{i}.csv'} for i in range(20)]

        # Sample with max_files=5
        samples = list(sample_files(config, table_spec, gcs_files, sample_rate=1, max_records=10, max_files=5))

        # Verify max_files was passed correctly
        mock_get_files.assert_called_once_with(config, gcs_files, 5)


class TestSamplingCombinations(unittest.TestCase):
    """Test combinations of sampling parameters"""

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_sample_rate_and_max_records_together(self, mock_client):
        """Test sample_rate and max_records work together correctly"""
        from tap_google_cloud_storage.gcs import _get_records_for_csv

        # Create CSV with 100 rows
        csv_data = "id,value\n"
        for i in range(100):
            csv_data += f"{i},{i*10}\n"

        table_spec = {'delimiter': ','}
        buffer = io.BytesIO(csv_data.encode('utf-8'))

        # Sample every 10th row, max 3 records
        samples = list(_get_records_for_csv('test.csv', sample_rate=10, buffer=buffer, table_spec=table_spec, max_records=3))

        # Should get rows 0, 10, 20 (stopped at 3)
        self.assertEqual(len(samples), 3)
        self.assertEqual(samples[0]['id'], '0')
        self.assertEqual(samples[1]['id'], '10')
        self.assertEqual(samples[2]['id'], '20')

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_max_records_smaller_than_sample_rate_matches(self, mock_client):
        """Test when max_records is smaller than number of matches"""
        from tap_google_cloud_storage.gcs import _get_records_for_jsonl

        # Create 1000 records
        jsonl_data = ""
        for i in range(1000):
            jsonl_data += f'{{"id": {i}}}\n'

        # Sample every 5th, max 10 records
        samples = list(_get_records_for_jsonl(sample_rate=5, data_bytes=jsonl_data.encode('utf-8'), max_records=10))

        # Should stop at 10 records
        self.assertEqual(len(samples), 10)
        # Last sample should be row 45 (0, 5, 10, 15, 20, 25, 30, 35, 40, 45)
        self.assertEqual(samples[9]['id'], 45)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_small_file_with_large_sample_rate(self, mock_client):
        """Test sampling small files with large sample_rate"""
        from tap_google_cloud_storage.gcs import _get_records_for_csv

        # Create CSV with only 5 rows
        csv_data = "id,name\n"
        for i in range(5):
            csv_data += f"{i},name{i}\n"

        table_spec = {'delimiter': ','}
        buffer = io.BytesIO(csv_data.encode('utf-8'))

        # Sample rate 100 (larger than file size)
        samples = list(_get_records_for_csv('test.csv', sample_rate=100, buffer=buffer, table_spec=table_spec, max_records=None))

        # Should only get row 0
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]['id'], '0')


class TestSamplingEdgeCases(unittest.TestCase):
    """Test edge cases for sampling parameters"""

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_max_records_one(self, mock_client):
        """Test behavior when max_records is 1"""
        from tap_google_cloud_storage.gcs import _get_records_for_jsonl

        jsonl_data = '{"id": 1}\n{"id": 2}\n{"id": 3}\n'

        samples = list(_get_records_for_jsonl(sample_rate=1, data_bytes=jsonl_data.encode('utf-8'), max_records=1))

        # Should get exactly 1 record
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]['id'], 1)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_max_records_none_means_unlimited(self, mock_client):
        """Test that max_records=None means unlimited samples"""
        from tap_google_cloud_storage.gcs import _get_records_for_csv

        # Create CSV with 50 rows
        csv_data = "id\n"
        for i in range(50):
            csv_data += f"{i}\n"

        table_spec = {'delimiter': ','}
        buffer = io.BytesIO(csv_data.encode('utf-8'))

        # Sample every 5th record, no limit
        samples = list(_get_records_for_csv('test.csv', sample_rate=5, buffer=buffer, table_spec=table_spec, max_records=None))

        # Should get all matching records (0, 5, 10, 15, 20, 25, 30, 35, 40, 45)
        self.assertEqual(len(samples), 10)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_sample_rate_larger_than_file(self, mock_client):
        """Test when sample_rate is larger than file size"""
        from tap_google_cloud_storage.gcs import _get_records_for_jsonl

        # Only 3 records
        jsonl_data = '{"id": 1}\n{"id": 2}\n{"id": 3}\n'

        # Sample rate 10 (larger than file)
        samples = list(_get_records_for_jsonl(sample_rate=10, data_bytes=jsonl_data.encode('utf-8'), max_records=None))

        # Should only get first record (row 0)
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]['id'], 1)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    @patch('tap_google_cloud_storage.gcs.list_files_in_bucket')
    def test_max_files_zero(self, mock_list_files, mock_client):
        """Test that max_files=0 processes no files"""
        from tap_google_cloud_storage.gcs import get_files_to_sample

        config = {'bucket': 'test-bucket'}
        gcs_files = [{'key': f'file{i}.csv'} for i in range(5)]

        sampled_files = get_files_to_sample(config, gcs_files, max_files=0)

        # Should process no files
        self.assertEqual(len(sampled_files), 0)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    @patch('tap_google_cloud_storage.gcs.list_files_in_bucket')
    def test_max_files_larger_than_available(self, mock_list_files, mock_client):
        """Test when max_files is larger than available files"""
        from tap_google_cloud_storage.gcs import get_files_to_sample

        # Mock 3 files
        mock_blobs = []
        for i in range(3):
            blob = Mock()
            blob.name = f'file{i}.csv'
            blob.download_as_bytes = Mock(return_value=b'id\n1\n')
            mock_blobs.append(blob)

        mock_bucket = Mock()
        mock_bucket.blob = Mock(side_effect=lambda name: next(b for b in mock_blobs if b.name == name))
        mock_client.return_value.bucket = Mock(return_value=mock_bucket)

        config = {'bucket': 'test-bucket'}
        gcs_files = [{'key': f'file{i}.csv'} for i in range(3)]

        # Request 10 files but only 3 available
        sampled_files = get_files_to_sample(config, gcs_files, max_files=10)

        # Should process all 3 available files
        self.assertEqual(len(sampled_files), 3)


class TestSamplingWithCompression(unittest.TestCase):
    """Test sampling parameters with compressed files"""

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_gz_file_respects_max_records(self, mock_client):
        """Test that GZ files respect max_records parameter"""
        from tap_google_cloud_storage.gcs import sampling_gz_file
        import gzip

        # Create CSV content
        csv_content = "id,name\n"
        for i in range(100):
            csv_content += f"{i},name{i}\n"

        # Compress it
        gz_data = io.BytesIO()
        with gzip.GzipFile(fileobj=gz_data, mode='wb', filename='test.csv') as gz:
            gz.write(csv_content.encode('utf-8'))

        table_spec = {'delimiter': ','}

        # Sample with rate=5, max_records=3
        samples = list(sampling_gz_file(table_spec, 'test.csv.gz', gz_data.getvalue(), sample_rate=5, max_records=3))

        # Should respect max_records
        self.assertEqual(len(samples), 3)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    @patch('tap_google_cloud_storage.gcs.compression.infer')
    def test_zip_file_respects_max_records(self, mock_infer, mock_client):
        """Test that ZIP files respect max_records parameter"""
        from tap_google_cloud_storage.gcs import sampling_zip_file

        # Create mock decompressed file
        csv_content = "id\n"
        for i in range(50):
            csv_content += f"{i}\n"

        mock_file = Mock()
        mock_file.name = 'data.csv'
        mock_file.read = Mock(return_value=csv_content.encode('utf-8'))
        mock_infer.return_value = [mock_file]

        table_spec = {'delimiter': ','}

        # Sample with rate=3, max_records=5
        samples = list(sampling_zip_file(table_spec, 'test.zip', b'zipdata', sample_rate=3, max_records=5))

        # Should respect max_records
        self.assertEqual(len(samples), 5)


if __name__ == '__main__':
    unittest.main()
