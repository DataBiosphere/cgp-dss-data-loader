import datetime
import logging
import unittest
import uuid
from pathlib import Path

import requests

from tests import eventually, ignore_resource_warnings, message
from tests.abstract_loader_test import AbstractLoaderTest

logging.getLogger(__name__)

TEST_DATA_PATH = Path(__file__).parents[1] / 'tests' / 'test_data'


class TestGen3Loader(AbstractLoaderTest, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.loader_type = 'gen3'

    def test_gen3_loading_nested_metadata(self):
        self._test_gen3_loading_from_cli(TEST_DATA_PATH / 'gen3_sample_input_nested_metadata.json')

    def test_gen3_loading_flat_metadata(self):
        self._test_gen3_loading_from_cli(TEST_DATA_PATH / 'gen3_sample_input_flat_metadata.json')

    def test_gen3_loading_transformer_output(self):
        self._test_gen3_loading_from_cli(TEST_DATA_PATH / 'transformer_sample_output.json')

    @ignore_resource_warnings
    def _test_gen3_loading_from_cli(self, test_json):
        """
        Test that a Gen3 JSON format input file can be uploaded to the DSS,
        and that all of the data files loaded are loaded by reference
        and set to not be indexed.

        1. Generates a Gen3 JSON input file from a template with a new unique 'bundle_did'
           and a new 'did' for the first file in the bundle.
        2. Searches the DSS to make sure it doesn't already exist using the HCA CLI Python bindings.
        3. Uploads the gen3 json to the DSS.
        4. Searches the DSS to verify the bundle was uploaded and indexed successfully.
        5. Assert data files are loaded by reference and set to not be indexed.
        6. Assert that the new 'did' for the first file in the bundle was found in the results.
        """

        message("Test that initial loading works successfully")
        # mint a new 'bundle_did'
        guid = str(uuid.uuid4())
        # make new guid for first file
        file_guid = str(uuid.uuid4())
        # we want a new version of the file to be uploaded
        file_version = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self._test_gen3_loading(test_json, guid, file_guid, file_version)

        message("Test that uploading again will be handled successfully")
        guid = str(uuid.uuid4())
        self._test_gen3_loading(test_json, guid, file_guid, file_version)

    def _test_gen3_loading(self, test_json, bundle_guid, file_guid, file_version):

        @eventually(timeout_seconds=5.0, retry_interval_seconds=1.0)
        def _search_for_bundle(bundle_uuid):
            # Search for the bundle uuid in the DSS and make sure it now exists and uploading was successful
            search_results = self.dss_client.post_search(es_query={'query': {'term': {'uuid': bundle_uuid}}}, replica='aws')
            assert search_results['total_hits'] > 0
            return search_results

        message("Search for the bundle uuid in the DSS to make sure it does not exist yet")
        search_results = self.dss_client.post_search(es_query={'query': {'term': {'uuid': bundle_guid}}}, replica='aws')
        assert search_results['total_hits'] == 0

        message("Prepare test input file to load")
        with self._tmp_json_file(test_json, bundle_guid, file_guid, file_version) as tmp_json:
            message("Load the test input file")
            self._load_file(tmp_json)

            message("Wait for newly loaded bundle to appear in search results")
            search_results = _search_for_bundle(bundle_guid)

            message("Verify that all of the results (except metadata.json) are file references "
                    "and set to not be indexed")
            found_matching_file = False
            for r in search_results['results']:
                response = requests.get(r['bundle_url'])
                returned_json = response.json()
                for f in returned_json['bundle']['files']:
                    if f['name'] != 'metadata.json':
                        assert f['indexed'] is False
                        assert 'dss-type=fileref' in f['content-type']

                        message("Verify that the file guid is stored")
                        file_ref_json = self.dss_client.get_file(uuid=f['uuid'], version=f['version'], replica='aws')
                        found_matching_file = found_matching_file or file_ref_json['aliases'][0] == file_guid
            assert found_matching_file


if __name__ == '__main__':
    unittest.main()
