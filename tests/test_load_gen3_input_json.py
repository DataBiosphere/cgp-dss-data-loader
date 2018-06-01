import datetime
import json
import os
import tempfile
import time
import unittest
import uuid
from contextlib import contextmanager
from pathlib import Path

import hca
import requests

from tests import eventually, ignore_resource_warnings, message

from scripts.cgp_data_loader import main as cgp_data_loader_main


class TestGen3InputFormatLoading(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.dss_client = hca.dss.DSSClient()
        cls.dss_client.host = 'https://hca-dss-4.ucsc-cgp-dev.org/v1'
        cls.dss_endpoint = os.getenv('TEST_DSS_ENDPOINT', cls.dss_client.host)
        cls.staging_bucket = os.getenv('DSS_S3_STAGING_BUCKET', 'mbaumann-dss-staging')
        cls.project_path = Path(__file__).parents[1]
        cls.test_files = [f'{cls.project_path}/tests/test_data/{test_file}'
                          for test_file in ['gen3_sample_input.json',
                                            'gen3_sample_input2.json',
                                            'transformer_sample_output.json']
                          ]

    @staticmethod
    @contextmanager
    def _tmp_json_file(json_input_file, guid, file_guid, file_version):
        # copy the contents of json_input_file to tmp_json
        # but change 'bundle_did' to a new guid
        with open(json_input_file, 'r') as jsonFile:
            json_contents = json.load(jsonFile)
        json_contents[0]['bundle_did'] = guid
        json_contents[0]['manifest'][0]['did'] = file_guid
        for file_info in json_contents[0]['manifest']:
            file_info['updated_datetime'] = file_version
        with tempfile.NamedTemporaryFile() as jsonFile:
            with open(jsonFile.name, 'w') as fh:
                json.dump(json_contents, fh)
            yield jsonFile.name

    def _load_file(self, tmp_json):
        """run the load script and clean up after ourselves"""
        # upload the data bundle to the DSS
        args = ['--no-dry-run',
                '--dss-endpoint',
                f'{self.dss_endpoint}',
                '--staging-bucket',
                f'{self.staging_bucket}',
                'gen3',
                '--json-input-file',
                f'{tmp_json}']
        cgp_data_loader_main(args)

    def test_gen3_input_format_loading_from_cli(self):
        self._test_gen3_input_format_loading_from_cli(self.test_files[0])

    def test_gen3_input_format2_loading_from_cli(self):
        self._test_gen3_input_format_loading_from_cli(self.test_files[1])

    def test_transformer_gen3_input_loading_from_cli(self):
        self._test_gen3_input_format_loading_from_cli(self.test_files[2])

    @ignore_resource_warnings
    def _test_gen3_input_format_loading_from_cli(self, test_json):
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
