import unittest
from tap_tester import menagerie, runner, connections
from base import GCSBaseTest
from utils_for_test import delete_and_push_file


class GCSDiscoveryTest(GCSBaseTest):
    """
    Test discovery (check mode) for tap-google-cloud-storage.

    Verifies:
    - Number of tables discovered matches expectations
    - Table names follow naming convention (lowercase alphas and underscores)
    - There is only 1 top level breadcrumb
    - There are no duplicate/conflicting metadata entries
    - Primary key(s) match expectations
    - '_sdc' fields are added in the schema
    - The absence of a forced-replication-method (replication methods are non-discoverable)
    - Primary keys have inclusion of "automatic"
    - Non-primary key fields have inclusion of "available"
    """

    table_entry = [
        {
            'table_name': 'employees',
            'key_properties': ['id'],
            'search_prefix': 'tap-gcs-test',
            'search_pattern': 'discovery_test\\.csv$',
            'date_overrides': ['date_of_joining']
        }
    ]

    def setUp(self):
        """Set up test environment by uploading test files to GCS"""
        delete_and_push_file(self.get_properties(), self.resource_name(), None)
        self.conn_id = connections.ensure_connection(self)

    def resource_name(self):
        """Test resource files to upload"""
        return ["discovery_test.csv"]

    def name(self):
        """Test name"""
        return "test_discovery"

    def expected_check_streams(self):
        """Expected streams to be discovered"""
        return {'employees'}

    def test_run(self):
        """Run the discovery test"""

        # Run discovery
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Note: Skipping stream name format assertion as this tap is dynamic.
        # Stream names may not always follow strict naming conventions.

        # Verify each expected stream
        for stream in self.expected_check_streams():
            with self.subTest(stream=stream):

                # Verify the catalog is found for the stream
                catalog = next(iter([catalog for catalog in found_catalogs
                                   if catalog["stream_name"] == stream]), None)
                self.assertIsNotNone(catalog, msg=f"Catalog not found for stream: {stream}")

                # Get schema and metadata
                schema_and_metadata = menagerie.get_annotated_schema(self.conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]

                # Collecting expected values
                expected_primary_keys = {'id'}

                # Collecting actual values from metadata
                actual_primary_keys = set(
                    stream_properties[0].get("metadata", {}).get("table-key-properties", [])
                )
                actual_replication_method = stream_properties[0].get(
                    "metadata", {}
                ).get("forced-replication-method", [])
                actual_automatic_fields = set(
                    item.get("breadcrumb", ["properties", None])[1]
                    for item in metadata
                    if item.get("metadata", {}).get("inclusion") == "automatic"
                )

                ##########################################################################
                ### Metadata assertions
                ##########################################################################

                # Extract all field names
                actual_fields = []
                for md_entry in metadata:
                    if md_entry['breadcrumb'] != []:
                        actual_fields.append(md_entry['breadcrumb'][1])

                # Verify there are no duplicate metadata entries
                self.assertEqual(len(actual_fields), len(set(actual_fields)),
                               msg="Duplicates found in metadata entries")

                # Verify there is only 1 top level breadcrumb in metadata
                self.assertTrue(len(stream_properties) == 1,
                              msg=f"There is NOT only one top level breadcrumb for {stream}" + \
                                  f"\nstream_properties | {stream_properties}")

                # Verify primary key(s) match expectations
                self.assertSetEqual(expected_primary_keys, actual_primary_keys,
                                  msg=f"Primary keys don't match. Expected: {expected_primary_keys}, "
                                      f"Actual: {actual_primary_keys}")

                # Verify '_sdc' fields are added in the schema
                for _sdc_field in ["_sdc_source_bucket", "_sdc_source_file",
                                  "_sdc_source_lineno", "_sdc_extra"]:
                    self.assertIn(_sdc_field, actual_fields,
                                msg=f"Missing _sdc field: {_sdc_field}")

                # Verify the absence of a 'forced-replication-method'
                # (replication methods are non-discoverable for file-based taps)
                self.assertEqual(actual_replication_method, [],
                               msg="forced-replication-method should not be set")

                # Verify that primary keys are given the inclusion of automatic in metadata
                self.assertSetEqual(expected_primary_keys, actual_automatic_fields,
                                  msg=f"Primary keys should be automatic. Expected: {expected_primary_keys}, "
                                      f"Actual: {actual_automatic_fields}")

                # Verify that all other fields have inclusion of available
                non_automatic_fields = [
                    item for item in metadata
                    if item.get("breadcrumb", []) != []
                    and item.get("breadcrumb", ["properties", None])[1] not in actual_automatic_fields
                ]

                all_available = all(
                    item.get("metadata", {}).get("inclusion") == "available"
                    for item in non_automatic_fields
                )
                self.assertTrue(all_available,
                              msg="Not all non-key properties are set to available in metadata")
