from tap_tester import connections, menagerie
from base import GCSBaseTest
from utils_for_test import delete_and_push_file


class GCSParquetFileTest(GCSBaseTest):

    table_entry = [{
        'table_name': 'parquet_data',
        'search_prefix': 'tap_gcs_tester/parquet_files',
        'search_pattern': 'tap_gcs_tester/parquet_files/.*\\.parquet',
        'key_properties': []
    }]

    def setUp(self):
        delete_and_push_file(self.get_properties(), self.resource_names())
        self.conn_id = connections.ensure_connection(self)

    def resource_names(self):
        return ['my_table_parquet_fresh.parquet']

    def name(self):
        return 'test_parquet_file'

    def expected_check_streams(self):
        return {'parquet_data'}

    def expected_sync_streams(self):
        return {'parquet_data'}

    def expected_pks(self):
        return {'parquet_data': {}}

    def test_run(self):
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        menagerie.set_state(self.conn_id, {})

        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs)

        sync_record_count = self.run_and_verify_sync(self.conn_id)

        self.assertIn('parquet_data', sync_record_count)
        self.assertGreater(sync_record_count['parquet_data'], 0)
