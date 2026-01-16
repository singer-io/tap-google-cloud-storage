import unittest
from tap_tester import menagerie, runner, connections
from datetime import datetime as dt

from base import GCSBaseTest
from utils_for_test import delete_and_push_file


class GCSStartDateTest(GCSBaseTest):

    table_entry = [{'table_name': 'employee_table', 'search_prefix': 'tap_gcs_tester', 'search_pattern': 'start_date_.*.csv'}]

    def setUp(self):
        # Upload both test files to GCS
        delete_and_push_file(self.get_properties(), ["start_date_1.csv", "start_date_2.csv"], None)

    def name(self):
        return "test_start_date"

    def expected_check_streams(self):
        return {'employee_table'}

    def expected_sync_streams(self):
        return {'employee_table'}

    def expected_pks(self):
        return {'employee_table': {}}

    def parse_date(self, value, format):
        return dt.strptime(value, format)

    def test_run(self):
        # NOTE: The two test files "start_date_1.csv" and "start_date_2.csv" are
        # added in different dates, and expecting it never gets changed or modified

        self.START_DATE = '2022-07-07T00:00:00Z'

        ############ First sync ############
        self.conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_check_streams()]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs, True)

        # Run a sync job using orchestrator
        self.run_and_verify_sync(self.conn_id)

        # Verify actual rows were synced
        records_1 = runner.get_records_from_target_output()
        actual_records_1 = [record.get("data") for record in records_1.get("employee_table").get("messages")]
        state_1 = menagerie.get_state(self.conn_id)

        ############ Second sync ############
        self.conn_id = connections.ensure_connection(self, original_properties=False)
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_check_streams()]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs, True)

        # Run a sync job using orchestrator
        self.run_and_verify_sync(self.conn_id)

        # Verify actual rows were synced
        records_2 = runner.get_records_from_target_output()
        actual_records_2 = [record.get("data") for record in records_2.get("employee_table").get("messages")]
        state_2 = menagerie.get_state(self.conn_id)

        # NOTE: Since both files are uploaded during test setup, they have the same modification time
        # Both syncs will get the same files since both pass the start_date filter
        # Verify we synced the same number of records in both syncs
        self.assertEqual(len(actual_records_1), len(actual_records_2),
                        msg=f"Both syncs should get same files. Sync 1: {len(actual_records_1)}, Sync 2: {len(actual_records_2)}")

        # Verify we get same bookmark for both syncs
        self.assertEqual(
            self.parse_date(state_2.get("bookmarks").get("employee_table").get("modified_since"), "%Y-%m-%dT%H:%M:%S.%f+00:00"),
            self.parse_date(state_1.get("bookmarks").get("employee_table").get("modified_since"), "%Y-%m-%dT%H:%M:%S.%f+00:00"))

        # Verify both syncs got all the data
        self.assertEqual(len(actual_records_1), 8, msg="First sync should have 8 records")
        self.assertEqual(len(actual_records_2), 8, msg="Second sync should have 8 records")
