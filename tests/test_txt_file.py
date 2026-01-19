from tap_tester import connections, menagerie, runner
from base import GCSBaseTest
from utils_for_test import delete_and_push_file


class GCSTxtFile(GCSBaseTest):
    """Test TXT file reading with comma delimiter"""

    table_entry = [{'table_name': 'txt_data', 'search_prefix': 'tap_gcs_tester/txt', 'search_pattern': 'tap_gcs_tester/txt/.*\\.txt', 'key_properties': [], 'delimiter': ','}]

    def setUp(self):
        delete_and_push_file(self.get_properties(), self.resource_names(), None)
        self.conn_id = connections.ensure_connection(self)

    def resource_names(self):
        return ["sample_data.txt"]

    def name(self):
        return "test_txt_file"

    def expected_check_streams(self):
        return {'txt_data'}

    def expected_sync_streams(self):
        return {'txt_data'}

    def expected_pks(self):
        return {'txt_data': {}}

    def test_run(self):
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs)

        self.run_and_verify_sync(self.conn_id)

        expected_records = 5
        # Verify actual rows were synced
        records = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records))
