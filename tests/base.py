import json
import os
import time
import unittest
import utils_for_test as utils
from tap_tester import connections, menagerie, runner
from datetime import datetime as dt


class GCSBaseTest(unittest.TestCase):
    """
    Base test class for tap-google-cloud-storage integration tests.
    Provides common setup, configuration, and test orchestration methods.
    """

    table_entry = None
    START_DATE = None

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-google-cloud-storage"

    @staticmethod
    def get_type():
        """The connection type for tap-tester"""
        return "platform.google-cloud-storage"

    @staticmethod
    def get_credentials():
        """
        GCS credentials from environment variables.
        Required environment variables:
        - TAP_GCS_PROJECT_ID
        - TAP_GCS_PRIVATE_KEY
        - TAP_GCS_CLIENT_EMAIL
        """
        # Get private key and replace literal \n with actual newlines
        private_key = os.getenv('TAP_GCS_PRIVATE_KEY', '')
        if private_key and '\\n' in private_key:
            private_key = private_key.replace('\\n', '\n')

        credentials_dict = {
            'project_id': os.getenv('TAP_GCS_PROJECT_ID'),
            'private_key': private_key,
            'client_email': os.getenv('TAP_GCS_CLIENT_EMAIL')
        }

        return credentials_dict

    def get_properties(self, original: bool = True):
        """
        Get the configuration properties for the tap.

        Args:
            original (bool): If True, return default properties. If False, use START_DATE.

        Returns:
            dict: Configuration properties including bucket and tables
        """
        props = {
            'start_date': os.getenv('TAP_GCS_START_DATE', '2021-11-02T00:00:00Z'),
            'bucket': os.getenv('TAP_GCS_BUCKET', 'tap-gcs-test-bucket'),
            'tables': json.dumps(self.table_entry)
        }
        if original:
            return props

        props['start_date'] = self.START_DATE
        return props

    def expected_pks(self):
        """
        Return expected primary keys for each stream.
        Override in subclasses to specify stream-specific primary keys.
        """
        return {}

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode (discovery) and verify it succeeds.
        This should be run prior to field selection and initial sync.

        Args:
            conn_id: Connection ID from tap-tester

        Returns:
            list: Found catalogs from discovery
        """
        # Run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, 
                          msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['stream_name'], found_catalogs))

        subset = self.expected_check_streams().issubset(found_catalog_names)
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")
        print("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id, is_expected_records_zero=False):
        """
        Run a sync job and make sure it exited properly.

        Args:
            conn_id: Connection ID from tap-tester
            is_expected_records_zero (bool): If True, allow zero records to be synced

        Returns:
            dict: Dictionary with keys of streams synced and values of record counts
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        sync_record_count = runner.examine_target_output_file(self,
                                                              conn_id,
                                                              self.expected_check_streams(),
                                                              self.expected_pks())

        if not is_expected_records_zero:
            self.assertGreater(
                sum(sync_record_count.values()), 0,
                msg="failed to replicate any data: {}".format(sync_record_count)
            )
        print("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count

    def select_all_streams_and_fields(self, conn_id, catalogs, select_all_fields: bool = True):
        """
        Select all streams and all fields within streams.

        Args:
            conn_id: Connection ID from tap-tester
            catalogs: List of catalog entries
            select_all_fields (bool): If False, no fields will be selected
        """
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # Get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)

    def get_selected_fields_from_metadata(self, metadata):
        """
        Extract the set of selected field names from catalog metadata.

        Args:
            metadata: Catalog metadata from menagerie

        Returns:
            set: Set of selected field names
        """
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (
                field['metadata'].get('selected') is True or \
                field['metadata'].get('inclusion') == 'automatic'
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields

    def perform_and_verify_table_and_field_selection(self, conn_id, test_catalogs, select_all_fields=True):
        """
        Perform field selection and verify the selection is correct.

        Args:
            conn_id: Connection ID from tap-tester
            test_catalogs: List of catalogs to test
            select_all_fields (bool): If True, select all fields. If False, only automatic fields.
        """
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=test_catalogs,
                                          select_all_fields=select_all_fields)
        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get('stream_name') for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue  # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                    field_selected = field_props.get('selected')
                    print("\tValidating selection on {}.{}: {}".format(
                        cat['stream_name'], field, field_selected))
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat['stream_name'])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
                self.assertEqual(expected_automatic_fields, selected_fields)

    @staticmethod
    def dt_to_ts(dtime, format):
        """
        Converts date string to Unix timestamp.

        Args:
            dtime (str): Date string
            format (str): Format string for strptime

        Returns:
            int: Unix timestamp
        """
        return int(time.mktime(dt.strptime(dtime, format).timetuple()))
