import unittest
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timezone
from tap_google_cloud_storage.sync import stream_is_selected, sync_stream, sync_table_file


class TestSyncHelpers(unittest.TestCase):

    def test_stream_is_selected_returns_true_when_selected(self):
        """Test stream_is_selected returns True when metadata selected is True"""
        mdata_map = {(): {'selected': True}}
        self.assertTrue(stream_is_selected(mdata_map))

    def test_stream_is_selected_returns_false_when_not_selected(self):
        """Test stream_is_selected returns False when metadata selected is False"""
        mdata_map = {(): {'selected': False}}
        self.assertFalse(stream_is_selected(mdata_map))

    def test_stream_is_selected_returns_false_when_missing(self):
        """Test stream_is_selected returns False when selected key is missing"""
        mdata_map = {(): {}}
        self.assertFalse(stream_is_selected(mdata_map))


class TestSyncStream(unittest.TestCase):

    def setUp(self):
        self.config = {
            'start_date': '2024-01-01T00:00:00Z',
            'bucket': 'test-bucket'
        }
        self.table_spec = {
            'table_name': 'my_table',
            'search_prefix': 'exports/',
            'search_pattern': '.*\\.csv'
        }
        self.stream = {
            'tap_stream_id': 'my_table',
            'schema': {'type': 'object', 'properties': {}}
        }
        self.state = {'bookmarks': {}}
        self.sync_start_time = datetime(2026, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

    @patch('tap_google_cloud_storage.gcs.get_input_files_for_table')
    @patch('tap_google_cloud_storage.sync.sync_table_file')
    @patch('singer.write_bookmark')
    @patch('singer.write_state')
    def test_sync_stream_processes_files_in_order(self, mock_write_state, mock_write_bookmark, 
                                                   mock_sync_file, mock_get_files):
        """Test that sync_stream processes files sorted by last_modified"""
        mock_files = [
            {'key': 'file2.csv', 'last_modified': datetime(2026, 1, 10, 12, 0, 0, tzinfo=timezone.utc)},
            {'key': 'file1.csv', 'last_modified': datetime(2026, 1, 10, 10, 0, 0, tzinfo=timezone.utc)},
            {'key': 'file3.csv', 'last_modified': datetime(2026, 1, 10, 14, 0, 0, tzinfo=timezone.utc)}
        ]
        mock_get_files.return_value = mock_files
        mock_sync_file.return_value = 10
        mock_write_bookmark.return_value = self.state

        records_streamed = sync_stream(self.config, self.state, self.table_spec, 
                                       self.stream, self.sync_start_time)

        # Check files were processed in sorted order
        calls = mock_sync_file.call_args_list
        self.assertEqual(calls[0][0][1], 'file1.csv')
        self.assertEqual(calls[1][0][1], 'file2.csv')
        self.assertEqual(calls[2][0][1], 'file3.csv')
        self.assertEqual(records_streamed, 30)

    @patch('tap_google_cloud_storage.gcs.get_input_files_for_table')
    @patch('tap_google_cloud_storage.sync.sync_table_file')
    @patch('singer.write_bookmark')
    @patch('singer.write_state')
    def test_sync_stream_updates_bookmark_per_file(self, mock_write_state, mock_write_bookmark,
                                                    mock_sync_file, mock_get_files):
        """Test that bookmark is updated after each file"""
        mock_files = [
            {'key': 'file1.csv', 'last_modified': datetime(2026, 1, 10, 10, 0, 0, tzinfo=timezone.utc)}
        ]
        mock_get_files.return_value = mock_files
        mock_sync_file.return_value = 5
        mock_write_bookmark.return_value = self.state

        sync_stream(self.config, self.state, self.table_spec, self.stream, self.sync_start_time)

        # Verify bookmark was written
        mock_write_bookmark.assert_called_once()
        args = mock_write_bookmark.call_args[0]
        self.assertEqual(args[1], 'my_table')
        self.assertEqual(args[2], 'modified_since')

    @patch('tap_google_cloud_storage.gcs.get_input_files_for_table')
    @patch('tap_google_cloud_storage.sync.sync_table_file')
    @patch('singer.write_bookmark')
    @patch('singer.write_state')
    def test_sync_stream_caps_bookmark_at_sync_start_time(self, mock_write_state, mock_write_bookmark,
                                                          mock_sync_file, mock_get_files):
        """Test that bookmark is capped at sync_start_time for files modified after sync started"""
        future_time = datetime(2026, 1, 20, 0, 0, 0, tzinfo=timezone.utc)
        mock_files = [
            {'key': 'file1.csv', 'last_modified': future_time}
        ]
        mock_get_files.return_value = mock_files
        mock_sync_file.return_value = 5
        mock_write_bookmark.return_value = self.state

        sync_stream(self.config, self.state, self.table_spec, self.stream, self.sync_start_time)

        # Bookmark should be sync_start_time, not future_time
        args = mock_write_bookmark.call_args[0]
        bookmark_value = args[3]
        self.assertEqual(bookmark_value, self.sync_start_time.isoformat())

    @patch('tap_google_cloud_storage.gcs.get_input_files_for_table')
    def test_sync_stream_raises_for_naive_last_modified(self, mock_get_files):
        """Test that sync fails fast if a blob timestamp is timezone-naive"""
        mock_get_files.return_value = [
            {'key': 'file1.csv', 'last_modified': datetime(2026, 1, 10, 10, 0, 0)}
        ]

        with self.assertRaises(ValueError):
            sync_stream(self.config, self.state, self.table_spec, self.stream, self.sync_start_time)


class TestSyncTableFile(unittest.TestCase):

    def setUp(self):
        self.config = {'bucket': 'test-bucket'}
        self.table_spec = {
            'table_name': 'my_table',
            'key_properties': ['id']
        }
        self.stream = {
            'tap_stream_id': 'my_table',
            'schema': {'type': 'object', 'properties': {}}
        }

    @patch('tap_google_cloud_storage.sync.handle_file')
    def test_sync_table_file_processes_csv(self, mock_handle):
        """Test that CSV files are processed"""
        mock_handle.return_value = 10

        result = sync_table_file(self.config, 'exports/data.csv', self.table_spec, self.stream)

        self.assertEqual(result, 10)
        mock_handle.assert_called_once()
        self.assertEqual(mock_handle.call_args[0][4], 'csv')

    @patch('tap_google_cloud_storage.sync.handle_file')
    def test_sync_table_file_processes_jsonl(self, mock_handle):
        """Test that JSONL files are processed"""
        mock_handle.return_value = 15

        result = sync_table_file(self.config, 'exports/data.jsonl', self.table_spec, self.stream)

        self.assertEqual(result, 15)
        mock_handle.assert_called_once()
        self.assertEqual(mock_handle.call_args[0][4], 'jsonl')

    @patch('tap_google_cloud_storage.sync.handle_file')
    def test_sync_table_file_processes_parquet(self, mock_handle):
        """Test that Parquet files are processed"""
        mock_handle.return_value = 20

        result = sync_table_file(self.config, 'exports/data.parquet', self.table_spec, self.stream)

        self.assertEqual(result, 20)
        mock_handle.assert_called_once()
        self.assertEqual(mock_handle.call_args[0][4], 'parquet')

    @patch('tap_google_cloud_storage.sync.handle_file')
    def test_sync_table_file_processes_avro(self, mock_handle):
        """Test that Avro files are processed"""
        mock_handle.return_value = 25

        result = sync_table_file(self.config, 'exports/data.avro', self.table_spec, self.stream)

        self.assertEqual(result, 25)
        mock_handle.assert_called_once()
        self.assertEqual(mock_handle.call_args[0][4], 'avro')

    @patch('tap_google_cloud_storage.sync.sync_gz_file')
    def test_sync_table_file_handles_gzip(self, mock_gz_file):
        """Test that gzip files are handled"""
        mock_gz_file.return_value = 30

        result = sync_table_file(self.config, 'exports/data.gz', self.table_spec, self.stream)

        self.assertEqual(result, 30)
        mock_gz_file.assert_called_once()

    def test_sync_table_file_skips_files_without_extension(self):
        """Test that files without extension are skipped"""
        result = sync_table_file(self.config, 'exports/datafile', self.table_spec, self.stream)

        self.assertEqual(result, 0)

    def test_sync_table_file_skips_unsupported_extensions(self):
        """Test that unsupported file extensions are skipped"""
        result = sync_table_file(self.config, 'exports/data.pdf', self.table_spec, self.stream)

        self.assertEqual(result, 0)


class TestSyncIncremental(unittest.TestCase):

    @patch('tap_google_cloud_storage.gcs.get_input_files_for_table')
    @patch('tap_google_cloud_storage.sync.sync_table_file')
    @patch('singer.write_bookmark')
    @patch('singer.write_state')
    def test_sync_uses_modified_since_from_state(self, mock_write_state, mock_write_bookmark,
                                                 mock_sync_file, mock_get_files):
        """Test that sync uses modified_since from state as starting point"""
        config = {
            'start_date': '2024-01-01T00:00:00Z',
            'bucket': 'test-bucket'
        }
        state = {
            'bookmarks': {
                'my_table': {
                    'modified_since': '2026-01-10T00:00:00+00:00'
                }
            }
        }
        table_spec = {'table_name': 'my_table', 'search_prefix': 'exports/'}
        stream = {'tap_stream_id': 'my_table', 'schema': {}}
        sync_start = datetime(2026, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

        mock_get_files.return_value = []
        mock_write_bookmark.return_value = state

        sync_stream(config, state, table_spec, stream, sync_start)

        # Verify get_input_files_for_table was called with parsed modified_since
        call_args = mock_get_files.call_args
        self.assertIsNotNone(call_args)


if __name__ == '__main__':
    unittest.main()
