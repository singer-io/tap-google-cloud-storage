from tap_tester import connections, menagerie, runner
from base import GCSBaseTest
from utils_for_test import delete_and_push_file


class GCSCompressedGZFileCSVJSONL(GCSBaseTest):

    table_entry = [{'table_name': 'gz_csv_jsonl_ext', 'search_prefix': 'tap_gcs_tester/gz_ext', 'search_pattern': 'tap_gcs_tester/gz_ext/.*\\.(csv|jsonl)$', 'key_properties': []}]

    def setUp(self):
        delete_and_push_file(self.get_properties(), self.resource_names(), None)
        self.conn_id = connections.ensure_connection(self)

    def resource_names(self):
        return ["gz_stored_as_csv.csv", "gz_stored_as_jsonl.jsonl"]

    def name(self):
        return "test_gz_file_with_csv_and_jsonl_extension"

    def expected_check_streams(self):
        return {'gz_csv_jsonl_ext'}

    def expected_sync_streams(self):
        return {'gz_csv_jsonl_ext'}

    def expected_pks(self):
        return {'gz_csv_jsonl_ext': {}}

    def test_run(self):
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs)

        self.run_and_verify_sync(self.conn_id)

        expected_records = 2000  # Both files contain identical 1000-row CSV data
        # Verify actual rows were synced
        records = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records))
