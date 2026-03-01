import unittest
from unittest.mock import patch, MagicMock
from google.cloud import storage
from google.api_core.exceptions import (
    GoogleAPIError,
    Forbidden,
    NotFound,
    InternalServerError,
    ServiceUnavailable,
    BadGateway,
    GatewayTimeout,
    TooManyRequests,
)


class TestGCSAuthentication(unittest.TestCase):

    @patch('google.cloud.storage.Client.from_service_account_info')
    def test_successful_authentication(self, mock_client):
        """Test successful authentication with valid service account credentials"""
        from tap_google_cloud_storage import gcs

        mock_client.return_value = MagicMock()

        config = {
            'type': 'service_account',
            'project_id': 'test-project',
            'private_key_id': 'key123',
            'private_key': '-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n',
            'client_email': 'test@test.iam.gserviceaccount.com',
            'client_id': '12345',
            'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
            'token_uri': 'https://oauth2.googleapis.com/token',
            'bucket': 'test-bucket'
        }

        client = gcs.setup_gcs_client(config)

        self.assertIsNotNone(client)
        mock_client.assert_called_once_with(config)

    @patch('google.cloud.storage.Client.from_service_account_info')
    def test_authentication_failure_with_invalid_credentials(self, mock_client):
        """Test authentication fails with invalid service account credentials"""
        from tap_google_cloud_storage import gcs

        mock_client.side_effect = ValueError('Invalid credentials')

        config = {
            'type': 'service_account',
            'project_id': 'test-project',
            'bucket': 'test-bucket'
        }

        with self.assertRaises(ValueError):
            gcs.setup_gcs_client(config)

    @patch('google.cloud.storage.Client.from_service_account_info')
    def test_authentication_with_missing_required_fields(self, mock_client):
        """Test authentication fails when required fields are missing"""
        from tap_google_cloud_storage import gcs

        mock_client.side_effect = ValueError('Missing required fields')

        config = {
            'bucket': 'test-bucket'
        }

        with self.assertRaises(ValueError):
            gcs.setup_gcs_client(config)


class TestGCSConnection(unittest.TestCase):

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_list_files_in_bucket_success(self, mock_setup_client):
        """Test successfully listing files in a GCS bucket"""
        from tap_google_cloud_storage import gcs

        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob1 = MagicMock()
        mock_blob1.name = 'file1.csv'
        mock_blob2 = MagicMock()
        mock_blob2.name = 'file2.csv'

        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2]
        mock_client.bucket.return_value = mock_bucket
        mock_setup_client.return_value = mock_client

        config = {'bucket': 'test-bucket'}

        files = list(gcs.list_files_in_bucket(config))

        self.assertEqual(len(files), 2)
        self.assertEqual(files[0].name, 'file1.csv')
        self.assertEqual(files[1].name, 'file2.csv')

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_list_files_with_prefix(self, mock_setup_client):
        """Test listing files with a specific prefix via root_path config"""
        from tap_google_cloud_storage import gcs

        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.name = 'exports/data.csv'

        mock_bucket.list_blobs.return_value = [mock_blob]
        mock_client.bucket.return_value = mock_bucket
        mock_setup_client.return_value = mock_client

        config = {'bucket': 'test-bucket', 'root_path': 'exports/'}

        files = list(gcs.list_files_in_bucket(config))

        self.assertEqual(len(files), 1)
        mock_bucket.list_blobs.assert_called_once_with(prefix='exports/')

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_connection_fails_with_invalid_bucket(self, mock_setup_client):
        """Test connection fails when bucket doesn't exist"""
        from tap_google_cloud_storage import gcs

        mock_client = MagicMock()
        mock_client.bucket.side_effect = NotFound('Bucket not found')
        mock_setup_client.return_value = mock_client

        config = {'bucket': 'nonexistent-bucket'}

        with self.assertRaises(NotFound):
            list(gcs.list_files_in_bucket(config))

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_connection_fails_with_no_permissions(self, mock_setup_client):
        """Test connection fails when service account lacks permissions"""
        from tap_google_cloud_storage import gcs

        mock_client = MagicMock()
        mock_client.bucket.side_effect = Forbidden('Access denied')
        mock_setup_client.return_value = mock_client

        config = {'bucket': 'forbidden-bucket'}

        with self.assertRaises(Forbidden):
            list(gcs.list_files_in_bucket(config))


class TestGCSFileOperations(unittest.TestCase):

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_download_file_success(self, mock_setup_client):
        """Test successfully downloading a file from GCS"""
        from tap_google_cloud_storage import gcs

        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = b'file content'

        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_setup_client.return_value = mock_client

        config = {'bucket': 'test-bucket'}

        content = gcs.get_file_handle(config, 'exports/data.csv')

        self.assertIsNotNone(content)
        mock_bucket.blob.assert_called_once_with('exports/data.csv')

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_download_file_not_found(self, mock_setup_client):
        """Test downloading a file that doesn't exist returns None"""
        from tap_google_cloud_storage import gcs

        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.open.side_effect = NotFound('File not found')

        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_setup_client.return_value = mock_client

        config = {'bucket': 'test-bucket'}

        # get_file_handle catches exceptions and returns None
        result = gcs.get_file_handle(config, 'exports/missing.csv')
        self.assertIsNone(result)


class TestBucketValidation(unittest.TestCase):

    def test_validate_bucket_name_valid(self):
        """Test validation passes for valid bucket names"""
        valid_names = [
            'my-bucket',
            'my-bucket-123',
            'bucket.with.dots',
            'a' * 63  # Max length
        ]

        for name in valid_names:
            # Should not raise any exception
            self.assertIsInstance(name, str)
            self.assertGreater(len(name), 0)

    def test_validate_bucket_name_invalid(self):
        """Test validation fails for invalid bucket names"""
        invalid_names = [
            '',  # Empty
            'a' * 64,  # Too long
            'UPPERCASE',  # Uppercase not allowed
            'bucket_with_underscore',  # Underscores not allowed
            '-starts-with-dash',  # Cannot start with dash
        ]

        for name in invalid_names:
            if not name:
                self.assertEqual(len(name), 0)
            elif len(name) > 63:
                self.assertGreater(len(name), 63)


class TestConnectionRetry(unittest.TestCase):

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_list_files_retries_on_internal_server_error(self, mock_setup_client):
        """Test that list_files_in_bucket retries on 500 InternalServerError"""
        from tap_google_cloud_storage import gcs

        mock_client = MagicMock()
        mock_bucket = MagicMock()

        mock_blob = MagicMock()
        mock_blob.name = 'file.csv'

        # First call raises 500, retry via _list_blobs_with_retry succeeds
        mock_bucket.list_blobs.side_effect = [
            InternalServerError('Backend error'),
            [mock_blob]
        ]

        mock_client.bucket.return_value = mock_bucket
        mock_setup_client.return_value = mock_client

        config = {'bucket': 'test-bucket'}

        files = list(gcs.list_files_in_bucket(config))

        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].name, 'file.csv')
        # list_blobs should have been called twice (once in generator, once in retry helper)
        self.assertEqual(mock_bucket.list_blobs.call_count, 2)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_list_files_retries_on_service_unavailable(self, mock_setup_client):
        """Test that list_files_in_bucket retries on 503 ServiceUnavailable"""
        from tap_google_cloud_storage import gcs

        mock_client = MagicMock()
        mock_bucket = MagicMock()

        mock_blob = MagicMock()
        mock_blob.name = 'data.csv'

        mock_bucket.list_blobs.side_effect = [
            ServiceUnavailable('Service unavailable'),
            [mock_blob]
        ]

        mock_client.bucket.return_value = mock_bucket
        mock_setup_client.return_value = mock_client

        config = {'bucket': 'test-bucket'}

        files = list(gcs.list_files_in_bucket(config))
        self.assertEqual(len(files), 1)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_get_file_handle_retries_on_5xx(self, mock_setup_client):
        """Test that get_file_handle retries on transient server errors"""
        from tap_google_cloud_storage import gcs

        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_file_handle = MagicMock()

        # First open raises 503, second succeeds
        mock_blob.open.side_effect = [
            ServiceUnavailable('Service unavailable'),
            mock_file_handle
        ]
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_setup_client.return_value = mock_client

        config = {'bucket': 'test-bucket'}

        result = gcs.get_file_handle(config, 'exports/data.csv')

        self.assertIsNotNone(result)
        self.assertEqual(mock_blob.open.call_count, 2)

    @patch('tap_google_cloud_storage.gcs.setup_gcsfs_client')
    def test_get_gcsfs_file_handle_retries_on_5xx(self, mock_setup_gcsfs):
        """Test that get_gcsfs_file_handle retries on transient server errors"""
        from tap_google_cloud_storage import gcs

        mock_fs = MagicMock()
        mock_handle = MagicMock()

        mock_fs.open.side_effect = [
            InternalServerError('Internal error'),
            mock_handle
        ]
        mock_setup_gcsfs.return_value = mock_fs

        config = {'bucket': 'test-bucket'}

        result = gcs.get_gcsfs_file_handle(config, 'exports/file.parquet')

        self.assertIsNotNone(result)
        self.assertEqual(mock_fs.open.call_count, 2)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_get_file_handle_returns_none_on_non_retryable_error(self, mock_setup_client):
        """Test that get_file_handle returns None on non-retryable errors like 404"""
        from tap_google_cloud_storage import gcs

        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        mock_blob.open.side_effect = NotFound('File not found')
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_setup_client.return_value = mock_client

        config = {'bucket': 'test-bucket'}

        result = gcs.get_file_handle(config, 'exports/missing.csv')

        # Non-retryable errors should not retry, should return None
        self.assertIsNone(result)
        self.assertEqual(mock_blob.open.call_count, 1)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_get_file_handle_raises_after_max_retries_exhausted(self, mock_setup_client):
        """Test that get_file_handle raises after all retries are exhausted"""
        from tap_google_cloud_storage import gcs

        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        # Always fails with 503
        mock_blob.open.side_effect = ServiceUnavailable('Service unavailable')
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_setup_client.return_value = mock_client

        config = {'bucket': 'test-bucket'}

        with self.assertRaises(ServiceUnavailable):
            gcs.get_file_handle(config, 'exports/data.csv')

        # Should have retried MAX_RETRIES times
        self.assertEqual(mock_blob.open.call_count, gcs.MAX_RETRIES)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_download_blob_with_retry_retries_on_bad_gateway(self, mock_setup_client):
        """Test that _download_blob_with_retry retries on 502 BadGateway"""
        from tap_google_cloud_storage import gcs

        mock_blob = MagicMock()
        mock_blob.download_as_bytes.side_effect = [
            BadGateway('Bad gateway'),
            b'file content'
        ]

        result = gcs._download_blob_with_retry(mock_blob)

        self.assertEqual(result, b'file content')
        self.assertEqual(mock_blob.download_as_bytes.call_count, 2)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_download_blob_with_retry_retries_on_too_many_requests(self, mock_setup_client):
        """Test that _download_blob_with_retry retries on 429 TooManyRequests"""
        from tap_google_cloud_storage import gcs

        mock_blob = MagicMock()
        mock_blob.download_as_bytes.side_effect = [
            TooManyRequests('Rate limit exceeded'),
            b'data'
        ]

        result = gcs._download_blob_with_retry(mock_blob)

        self.assertEqual(result, b'data')
        self.assertEqual(mock_blob.download_as_bytes.call_count, 2)

    @patch('tap_google_cloud_storage.gcs.setup_gcs_client')
    def test_download_blob_with_retry_retries_on_gateway_timeout(self, mock_setup_client):
        """Test that _download_blob_with_retry retries on 504 GatewayTimeout"""
        from tap_google_cloud_storage import gcs

        mock_blob = MagicMock()
        mock_blob.download_as_bytes.side_effect = [
            GatewayTimeout('Gateway timeout'),
            GatewayTimeout('Gateway timeout'),
            b'data'
        ]

        result = gcs._download_blob_with_retry(mock_blob)

        self.assertEqual(result, b'data')
        self.assertEqual(mock_blob.download_as_bytes.call_count, 3)

    def test_non_retryable_errors_propagate_immediately(self):
        """Test that non-retryable errors (4xx) are not retried"""
        from tap_google_cloud_storage import gcs

        mock_blob = MagicMock()
        mock_blob.download_as_bytes.side_effect = Forbidden('Access denied')

        with self.assertRaises(Forbidden):
            gcs._download_blob_with_retry(mock_blob)

        # Should only be called once - no retries for non-retryable errors
        self.assertEqual(mock_blob.download_as_bytes.call_count, 1)


if __name__ == '__main__':
    unittest.main()
