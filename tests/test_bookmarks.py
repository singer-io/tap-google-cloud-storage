from tap_tester import connections, menagerie, runner
from functools import reduce
from singer import metadata
import json
import os

from base import GCSBaseTest
from utils_for_test import delete_and_push_file


class GCSBookmarks(GCSBaseTest):

    table_entry = [{'table_name': 'chickens', 'search_prefix': 'tap_gcs_tester', 'search_pattern': 'bookmarks.*', 'key_properties': ['name']}]

    def setUp(self):
        delete_and_push_file(self.get_properties(), self.resource_name(), None)
        self.conn_id = connections.ensure_connection(self)

    def resource_name(self):
        return ["bookmarks.csv"]

    def name(self):
        return "tap_tester_gcs_bookmarks"

    def expected_check_streams(self):
        return {
            'chickens'
        }

    def expected_sync_streams(self):
        return {
            'chickens'
        }

    def expected_pks(self):
        return {
            'chickens': {"name"}
        }

    def test_run(self):
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        # Run a sync job using orchestrator
        self.run_and_verify_sync(self.conn_id)

        # Verify actual rows were synced
        record_count_by_stream = runner.examine_target_output_file(self, self.conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))

        # Put a new file to GCS
        delete_and_push_file(self.get_properties(), ["bookmarks2.csv"], None)

        # Run another Sync
        sync_job_name = runner.run_sync_mode(self, self.conn_id)
        exit_status = menagerie.get_exit_status(self.conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Check that we synced new records.
        records = runner.get_records_from_target_output()
        messages = records.get('chickens').get('messages')
        self.assertEqual(len(messages), 2, msg="Sync'd incorrect count of messages: {}".format(len(messages)))

        # Run a final sync
        sync_job_name = runner.run_sync_mode(self, self.conn_id)
        exit_status = menagerie.get_exit_status(self.conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Check that we synced new records.
        records = runner.get_records_from_target_output()
        messages = records.get('chickens', {}).get('messages', [])
        self.assertEqual(len(messages), 0, msg="Sync'd incorrect count of messages: {}".format(len(messages)))
