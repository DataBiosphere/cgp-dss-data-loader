import datetime
import json
import os
import tempfile
import unittest
import uuid
import logging

from contextlib import contextmanager
from pathlib import Path

import hca
import jsonschema
import requests

from loader.schemas import standard_schema
from loader.standard_loader import SCHEMA_URL
from scripts.cgp_data_loader import main as cgp_data_loader_main
from tests import eventually, ignore_resource_warnings, message

logger = logging.getLogger(__name__)

TEST_DATA_PATH = Path(__file__).parents[1] / 'tests' / 'test_data'


class TestStandardInputFormatLoading(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.dss_client = hca.dss.DSSClient()
        cls.dss_client.host = 'https://hca-dss-4.ucsc-cgp-dev.org/v1'
        cls.dss_endpoint = os.getenv("TEST_DSS_ENDPOINT", "https://hca-dss-4.ucsc-cgp-dev.org/v1")
        cls.staging_bucket = os.getenv("DSS_S3_STAGING_BUCKET", "mbaumann-dss-staging")
        cls.test_file = TEST_DATA_PATH / 'gen3_sample_input_standard_metadata.json'

    def test_data_matches_schema(self):
        """This is a sanity check to make sure that the test data matches the agreed upon schema"""
        test_json = json.loads(self.test_file.read_text())
        for bundle in test_json:
            jsonschema.validate(bundle, standard_schema)

    def test_basic_input_format_loading_from_cli(self):
        self._test_gen3_loading_from_cli(self.test_file)

    @staticmethod
    @contextmanager
    def _tmp_json_file(json_input_file, guid, file_guid, file_version):
        """Yields a temporary test file with identifying information changed"""

        def change_info(in_json, guid, file_guid, file_version):
            only_bundle = in_json[0]
            data_bundle = only_bundle['data_bundle']
            data_objects = only_bundle['data_objects']

            data_bundle['id'] = guid

            # change all references to the file guid
            object_key = data_bundle['data_object_ids'][0]
            data_bundle['data_object_ids'][0] = file_guid
            data_objects[file_guid] = data_objects[object_key]
            del data_objects[object_key]
            data_objects[file_guid]['id'] = file_guid

            # update versions for all files
            for file_ in data_objects.values():
                file_['updated'] = file_version

            return [only_bundle]

        with open(json_input_file, 'r') as jsonFile:
            json_contents = json.load(jsonFile)
        fixed_json = change_info(json_contents, guid, file_guid, file_version)
        with tempfile.NamedTemporaryFile() as jsonFile:
            with open(jsonFile.name, 'w') as fh:
                json.dump(fixed_json, fh)
            yield jsonFile.name

    def _load_file(self, tmp_json):
        """run the load script and clean up after ourselves"""
        # upload the data bundle to the DSS
        args = ['--no-dry-run',
                '--dss-endpoint',
                f'{self.dss_endpoint}',
                '--staging-bucket',
                f'{self.staging_bucket}',
                'standard',
                '--json-input-file',
                f'{tmp_json}']
        cgp_data_loader_main(args)

    @ignore_resource_warnings
    def _test_gen3_loading_from_cli(self, test_json):
        """A wrapper for the actual test"""

        message("Test that initial loading works successfully")
        # mint a new 'bundle_did'
        guid = str(uuid.uuid4())
        # make new guid for first file
        file_guid = f'dg.4503/{str(uuid.uuid4())}'
        # we want a new version of the file to be uploaded
        file_version = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self._test_gen3_loading(test_json, guid, file_guid, file_version)

        message("Test that uploading again will be handled successfully")
        guid = str(uuid.uuid4())
        self._test_gen3_loading(test_json, guid, file_guid, file_version)

    def _test_gen3_loading(self, test_json, bundle_guid, file_guid, file_version):
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
                    else:
                        message("Check that metadata file is indexed and matches the output schema")
                        assert f['indexed'] is True
                        file_ref_json = self.dss_client.get_file(uuid=f['uuid'], version=f['version'], replica='aws')
                        schema = requests.get(SCHEMA_URL).json()
                        jsonschema.validate(file_ref_json, schema)

            assert found_matching_file


if __name__ == '__main__':
    unittest.main()
