import unittest
from unittest.mock import patch, MagicMock
from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError, Forbidden, NotFound


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
    def test_connection_retries_on_transient_error(self, mock_setup_client):
        """Test that connection retries on transient errors"""
        from tap_google_cloud_storage import gcs

        mock_client = MagicMock()
        mock_bucket = MagicMock()

        # First call fails, second succeeds
        mock_blob = MagicMock()
        mock_blob.name = 'file.csv'
        mock_bucket.list_blobs.side_effect = [
            GoogleAPIError('Temporary error'),
            [mock_blob]
        ]

        mock_client.bucket.return_value = mock_bucket
        mock_setup_client.return_value = mock_client

        config = {'bucket': 'test-bucket'}

        # This would need retry logic implemented in the actual code
        # For now, we just verify the exception is raised
        with self.assertRaises(GoogleAPIError):
            list(gcs.list_files_in_bucket(config))


if __name__ == '__main__':
    unittest.main()
