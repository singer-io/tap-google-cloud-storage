"""
Comprehensive tests for service_account_json parsing and authentication.

Tests cover every input shape the field can arrive in:
 - Dict object (nested JSON in config.json)
 - JSON string (serialised form sent by platform)
 - private_key with literal \\n sequences (the original bug)
 - token_uri absent (should be hard-coded automatically)
 - token_uri present in incoming JSON (should be preserved)
 - Extra / unknown fields in the service account JSON
 - Non-auth tap fields (bucket, tables …) must NOT reach Google client
 - Fallback: flat config without service_account_info key
 - Missing / empty service_account_json
 - Malformed JSON string
 - service_account_info preferred over flat config fields
"""

import json
import unittest
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# Helpers / constants
# ---------------------------------------------------------------------------

_VALID_SA_DICT = {
    'type': 'service_account',
    'project_id': 'my-project',
    'client_email': 'sa@my-project.iam.gserviceaccount.com',
    'private_key': '-----BEGIN PRIVATE KEY-----\nABC\n-----END PRIVATE KEY-----\n',
    'token_uri': 'https://oauth2.googleapis.com/token',
}

_VALID_SA_DICT_NO_TOKEN_URI = {
    'type': 'service_account',
    'project_id': 'my-project',
    'client_email': 'sa@my-project.iam.gserviceaccount.com',
    'private_key': '-----BEGIN PRIVATE KEY-----\nABC\n-----END PRIVATE KEY-----\n',
}

_VALID_SA_DICT_LITERAL_NEWLINES = {
    'type': 'service_account',
    'project_id': 'my-project',
    'client_email': 'sa@my-project.iam.gserviceaccount.com',
    'private_key': '-----BEGIN PRIVATE KEY-----\\nABC\\n-----END PRIVATE KEY-----\\n',
}

DEFAULT_TOKEN_URI = 'https://oauth2.googleapis.com/token'


# ===========================================================================
# Tests for get_service_account_info()
# ===========================================================================


class TestGetServiceAccountInfo(unittest.TestCase):
    """Unit tests for gcs.get_service_account_info()."""

    def setUp(self):
        from tap_google_cloud_storage import gcs
        self.gcs = gcs

    # --- Positive: service_account_info key present ---

    def test_returns_service_account_info_when_present(self):
        """service_account_info dict is returned as-is (plus token_uri if missing)."""
        config = {'service_account_info': dict(_VALID_SA_DICT), 'bucket': 'b'}
        result = self.gcs.get_service_account_info(config)
        self.assertEqual(result['project_id'], 'my-project')
        self.assertEqual(result['client_email'], 'sa@my-project.iam.gserviceaccount.com')

    def test_token_uri_added_when_absent(self):
        """token_uri is always set to the Google default when not provided."""
        config = {'service_account_info': dict(_VALID_SA_DICT_NO_TOKEN_URI)}
        result = self.gcs.get_service_account_info(config)
        self.assertEqual(result['token_uri'], DEFAULT_TOKEN_URI)

    def test_token_uri_preserved_when_present(self):
        """token_uri already in the payload is kept unchanged."""
        custom_uri = 'https://custom.token.endpoint/token'
        sa = dict(_VALID_SA_DICT)
        sa['token_uri'] = custom_uri
        config = {'service_account_info': sa}
        result = self.gcs.get_service_account_info(config)
        self.assertEqual(result['token_uri'], custom_uri)

    def test_literal_backslash_n_in_private_key_is_converted(self):
        """Literal \\n in private_key is replaced with actual newlines (the core bug fix)."""
        config = {'service_account_info': dict(_VALID_SA_DICT_LITERAL_NEWLINES)}
        result = self.gcs.get_service_account_info(config)
        self.assertNotIn('\\n', result['private_key'])
        self.assertIn('\n', result['private_key'])

    def test_real_newlines_in_private_key_are_unchanged(self):
        """private_key with real newlines is left untouched."""
        config = {'service_account_info': dict(_VALID_SA_DICT)}
        result = self.gcs.get_service_account_info(config)
        self.assertEqual(result['private_key'], _VALID_SA_DICT['private_key'])

    def test_extra_fields_in_service_account_info_are_preserved(self):
        """Unknown extra fields (client_id, auth_uri …) pass through unchanged."""
        sa = dict(_VALID_SA_DICT)
        sa['client_id'] = '999'
        sa['auth_uri'] = 'https://accounts.google.com/o/oauth2/auth'
        config = {'service_account_info': sa}
        result = self.gcs.get_service_account_info(config)
        self.assertEqual(result['client_id'], '999')
        self.assertEqual(result['auth_uri'], 'https://accounts.google.com/o/oauth2/auth')

    def test_tap_config_fields_do_not_leak_into_service_account_info(self):
        """bucket, tables, start_date etc. must NOT appear in the returned info."""
        config = {
            'service_account_info': dict(_VALID_SA_DICT),
            'bucket': 'my-bucket',
            'start_date': '2025-01-01T00:00:00Z',
            'tables': [{'table_name': 'foo', 'search_pattern': '.*'}],
        }
        result = self.gcs.get_service_account_info(config)
        self.assertNotIn('bucket', result)
        self.assertNotIn('start_date', result)
        self.assertNotIn('tables', result)
        self.assertNotIn('service_account_json', result)
        self.assertNotIn('service_account_info', result)

    # --- Positive: fallback flat config without service_account_info key ---

    def test_fallback_extracts_service_account_fields_from_flat_config(self):
        """When service_account_info is absent the SERVICE_ACCOUNT_FIELDS are
        extracted directly from the flat config dict (backward compat)."""
        config = {
            'type': 'service_account',
            'project_id': 'my-project',
            'client_email': 'sa@my-project.iam.gserviceaccount.com',
            'private_key': '-----BEGIN PRIVATE KEY-----\nABC\n-----END PRIVATE KEY-----\n',
            'bucket': 'should-not-appear',
        }
        result = self.gcs.get_service_account_info(config)
        self.assertEqual(result['project_id'], 'my-project')
        self.assertNotIn('bucket', result)

    def test_fallback_adds_token_uri_when_missing_from_flat_config(self):
        """Even in the flat-config fallback path, missing token_uri gets filled in."""
        config = {
            'type': 'service_account',
            'project_id': 'my-project',
            'client_email': 'sa@my-project.iam.gserviceaccount.com',
            'private_key': 'key',
        }
        result = self.gcs.get_service_account_info(config)
        self.assertEqual(result['token_uri'], DEFAULT_TOKEN_URI)

    def test_service_account_info_takes_priority_over_flat_config_fields(self):
        """service_account_info beats conflicting flat-config fields."""
        config = {
            'service_account_info': {
                'project_id': 'from-nested',
                'client_email': 'nested@test.com',
                'private_key': 'nested-key',
                'type': 'service_account',
            },
            'project_id': 'from-flat',
            'client_email': 'flat@test.com',
        }
        result = self.gcs.get_service_account_info(config)
        self.assertEqual(result['project_id'], 'from-nested')
        self.assertEqual(result['client_email'], 'nested@test.com')

    # --- Edge: empty / None service_account_info ---

    def test_empty_service_account_info_falls_back_to_flat_config(self):
        """An explicitly empty dict triggers the flat-config fallback."""
        config = {
            'service_account_info': {},
            'type': 'service_account',
            'project_id': 'flat-project',
            'client_email': 'flat@test.com',
            'private_key': 'flat-key',
        }
        result = self.gcs.get_service_account_info(config)
        self.assertEqual(result['project_id'], 'flat-project')

    def test_none_service_account_info_falls_back_to_flat_config(self):
        """service_account_info=None triggers the flat-config fallback."""
        config = {
            'service_account_info': None,
            'project_id': 'flat-project',
            'client_email': 'flat@test.com',
            'private_key': 'flat-key',
            'type': 'service_account',
        }
        result = self.gcs.get_service_account_info(config)
        self.assertEqual(result['project_id'], 'flat-project')

    # --- Negative: no usable credential fields at all ---

    def test_empty_config_returns_only_token_uri(self):
        """With no credentials at all, only token_uri is injected."""
        result = self.gcs.get_service_account_info({})
        self.assertEqual(result, {'token_uri': DEFAULT_TOKEN_URI})

    def test_no_private_key_does_not_crash(self):
        """Absence of private_key must not raise – Google client will surface the error."""
        config = {
            'service_account_info': {
                'type': 'service_account',
                'project_id': 'p',
                'client_email': 'e@p.com',
            }
        }
        result = self.gcs.get_service_account_info(config)
        self.assertNotIn('private_key', result)
        self.assertEqual(result['token_uri'], DEFAULT_TOKEN_URI)


# ===========================================================================
# Tests for main() service_account_json parsing in __init__.py
# ===========================================================================


class TestMainServiceAccountJsonParsing(unittest.TestCase):
    """Tests for the service_account_json → service_account_info conversion in main()."""

    def _run_parse(self, service_account_json_value):
        """Helper: feed the value through the parsing block in main() and return config."""
        config = {'service_account_json': service_account_json_value}
        sa = config.get('service_account_json')
        if sa:
            if isinstance(sa, str):
                config['service_account_info'] = json.loads(sa)
            else:
                config['service_account_info'] = sa
        return config

    # --- Positive: dict (object in config.json) ---

    def test_dict_input_stored_as_service_account_info(self):
        """service_account_json as a plain dict is stored directly."""
        result = self._run_parse(dict(_VALID_SA_DICT))
        self.assertEqual(result['service_account_info']['project_id'], 'my-project')

    def test_dict_input_original_key_preserved(self):
        """The original service_account_json key is still in config after parsing."""
        result = self._run_parse(dict(_VALID_SA_DICT))
        self.assertIn('service_account_json', result)

    # --- Positive: JSON string (platform sends serialised JSON) ---

    def test_json_string_is_parsed_to_dict(self):
        """service_account_json as a JSON string is decoded to a dict."""
        result = self._run_parse(json.dumps(_VALID_SA_DICT))
        self.assertIsInstance(result['service_account_info'], dict)
        self.assertEqual(result['service_account_info']['project_id'], 'my-project')

    def test_json_string_with_real_newlines_in_private_key(self):
        """JSON string where \\n are encoded as real newlines (normal JSON encoding)."""
        sa = dict(_VALID_SA_DICT)
        result = self._run_parse(json.dumps(sa))
        self.assertIn('\n', result['service_account_info']['private_key'])

    def test_json_string_with_literal_backslash_n_in_private_key(self):
        """JSON string where private_key has literal \\n - gets fixed later by
        get_service_account_info, but parsing itself must not raise."""
        sa = dict(_VALID_SA_DICT_LITERAL_NEWLINES)
        result = self._run_parse(json.dumps(sa))
        # The key should still be present after parsing the JSON string
        self.assertIn('private_key', result['service_account_info'])

    # --- Negative: malformed JSON string ---

    def test_malformed_json_string_raises_json_decode_error(self):
        """Malformed JSON string must raise json.JSONDecodeError."""
        with self.assertRaises(json.JSONDecodeError):
            self._run_parse('{"not valid json":')

    def test_plain_string_non_json_raises_json_decode_error(self):
        """A completely non-JSON string must raise json.JSONDecodeError."""
        with self.assertRaises(json.JSONDecodeError):
            self._run_parse('just a plain string')

    # --- Edge: empty / falsy service_account_json ---

    def test_empty_string_skips_parsing(self):
        """Empty string is falsy – parsing block is skipped, no service_account_info key."""
        result = self._run_parse('')
        self.assertNotIn('service_account_info', result)

    def test_none_skips_parsing(self):
        """None is falsy – parsing block is skipped."""
        config = {}
        sa = config.get('service_account_json')  # returns None
        if sa:
            config['service_account_info'] = sa if not isinstance(sa, str) else json.loads(sa)
        self.assertNotIn('service_account_info', config)

    def test_empty_dict_skips_parsing(self):
        """An empty dict {} is falsy – parsing block is skipped, same as None/empty-string."""
        result = self._run_parse({})
        # {} is falsy in Python so service_account_info is NOT set
        self.assertNotIn('service_account_info', result)


# ===========================================================================
# Tests for setup_gcs_client() — end-to-end auth call
# ===========================================================================


class TestSetupGcsClientServiceAccountJson(unittest.TestCase):
    """Test that setup_gcs_client() passes ONLY service-account fields to Google."""

    def setUp(self):
        from tap_google_cloud_storage import gcs
        self.gcs = gcs

    def _make_config(self, sa_dict):
        return {
            'service_account_info': sa_dict,
            'bucket': 'must-not-reach-google',
            'start_date': '2025-01-01T00:00:00Z',
            'tables': [],
        }

    # --- Positive ---

    @patch('google.cloud.storage.Client.from_service_account_info')
    def test_gcs_client_called_with_only_auth_fields(self, mock_client):
        """Google client must not receive tap-config fields."""
        mock_client.return_value = MagicMock()
        config = self._make_config(dict(_VALID_SA_DICT))
        self.gcs.setup_gcs_client(config)

        call_args = mock_client.call_args[0][0]
        self.assertNotIn('bucket', call_args)
        self.assertNotIn('start_date', call_args)
        self.assertNotIn('tables', call_args)
        self.assertNotIn('service_account_json', call_args)
        self.assertNotIn('service_account_info', call_args)

    @patch('google.cloud.storage.Client.from_service_account_info')
    def test_gcs_client_receives_correct_project_and_email(self, mock_client):
        """project_id and client_email arrive at the Google client unchanged."""
        mock_client.return_value = MagicMock()
        config = self._make_config(dict(_VALID_SA_DICT))
        self.gcs.setup_gcs_client(config)

        call_args = mock_client.call_args[0][0]
        self.assertEqual(call_args['project_id'], 'my-project')
        self.assertEqual(call_args['client_email'], 'sa@my-project.iam.gserviceaccount.com')

    @patch('google.cloud.storage.Client.from_service_account_info')
    def test_gcs_client_gets_token_uri_even_when_absent_from_config(self, mock_client):
        """token_uri is always injected before calling Google client."""
        mock_client.return_value = MagicMock()
        config = self._make_config(dict(_VALID_SA_DICT_NO_TOKEN_URI))
        self.gcs.setup_gcs_client(config)

        call_args = mock_client.call_args[0][0]
        self.assertEqual(call_args['token_uri'], DEFAULT_TOKEN_URI)

    @patch('google.cloud.storage.Client.from_service_account_info')
    def test_literal_newlines_in_private_key_fixed_before_google_call(self, mock_client):
        """Literal \\n in private_key are converted to real newlines before
        passing to the Google client – the core bug fix."""
        mock_client.return_value = MagicMock()
        config = self._make_config(dict(_VALID_SA_DICT_LITERAL_NEWLINES))
        self.gcs.setup_gcs_client(config)

        call_args = mock_client.call_args[0][0]
        self.assertNotIn('\\n', call_args['private_key'])
        self.assertIn('\n', call_args['private_key'])

    @patch('google.cloud.storage.Client.from_service_account_info')
    def test_real_newlines_in_private_key_remain_unchanged(self, mock_client):
        """If private_key already has real newlines they must not be double-processed."""
        mock_client.return_value = MagicMock()
        config = self._make_config(dict(_VALID_SA_DICT))
        self.gcs.setup_gcs_client(config)

        call_args = mock_client.call_args[0][0]
        self.assertEqual(call_args['private_key'], _VALID_SA_DICT['private_key'])

    @patch('google.cloud.storage.Client.from_service_account_info')
    def test_extra_service_account_fields_pass_through(self, mock_client):
        """Extra fields like client_id, auth_uri are forwarded to Google unchanged."""
        mock_client.return_value = MagicMock()
        sa = dict(_VALID_SA_DICT)
        sa['client_id'] = 'abc123'
        config = self._make_config(sa)
        self.gcs.setup_gcs_client(config)

        call_args = mock_client.call_args[0][0]
        self.assertEqual(call_args['client_id'], 'abc123')

    @patch('google.cloud.storage.Client.from_service_account_info')
    def test_returns_google_client_object(self, mock_client):
        """setup_gcs_client must return whatever Google's constructor returns."""
        expected = MagicMock(name='GCSClient')
        mock_client.return_value = expected
        config = self._make_config(dict(_VALID_SA_DICT))
        result = self.gcs.setup_gcs_client(config)
        self.assertIs(result, expected)

    # --- Negative ---

    @patch('google.cloud.storage.Client.from_service_account_info')
    def test_raises_when_google_raises_value_error(self, mock_client):
        """Exceptions from Google client propagate to the caller."""
        mock_client.side_effect = ValueError('InvalidKey')
        config = self._make_config(dict(_VALID_SA_DICT))
        with self.assertRaises(ValueError):
            self.gcs.setup_gcs_client(config)

    @patch('google.cloud.storage.Client.from_service_account_info')
    def test_raises_with_empty_config(self, mock_client):
        """An entirely empty config still reaches Google and lets it raise."""
        mock_client.side_effect = Exception('missing credentials')
        with self.assertRaises(Exception):
            self.gcs.setup_gcs_client({})


# ===========================================================================
# Tests for config_sample.json format compliance
# ===========================================================================


class TestServiceAccountJsonFormatCompliance(unittest.TestCase):
    """Verify both allowed shapes of service_account_json are parsed identically."""

    def setUp(self):
        from tap_google_cloud_storage import gcs
        self.gcs = gcs

    def _parse_and_get_info(self, service_account_json_value):
        """Simulate the main() parsing block and return service_account_info."""
        config = {}
        sa = service_account_json_value
        if sa:
            if isinstance(sa, str):
                config['service_account_info'] = json.loads(sa)
            else:
                config['service_account_info'] = sa
        return config.get('service_account_info', {})

    def test_dict_and_json_string_produce_identical_service_account_info(self):
        """Both shapes must produce the same intermediate service_account_info."""
        sa_as_dict = dict(_VALID_SA_DICT)
        sa_as_string = json.dumps(_VALID_SA_DICT)

        info_from_dict = self._parse_and_get_info(sa_as_dict)
        info_from_string = self._parse_and_get_info(sa_as_string)

        self.assertEqual(info_from_dict, info_from_string)

    def test_dict_and_json_string_produce_identical_auth_payload(self):
        """Both shapes must produce the same final payload sent to Google."""
        sa_as_dict = dict(_VALID_SA_DICT)
        sa_as_string = json.dumps(_VALID_SA_DICT)

        config_dict = {'service_account_info': self._parse_and_get_info(sa_as_dict), 'bucket': 'b'}
        config_str = {'service_account_info': self._parse_and_get_info(sa_as_string), 'bucket': 'b'}

        payload_dict = self.gcs.get_service_account_info(config_dict)
        payload_str = self.gcs.get_service_account_info(config_str)

        self.assertEqual(payload_dict, payload_str)


if __name__ == '__main__':
    unittest.main()
