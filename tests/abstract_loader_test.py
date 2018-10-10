import json
import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path

from hca import HCAConfig
from hca.dss import DSSClient

from scripts.cgp_data_loader import main as cgp_data_loader_main
from tests import eventually
from util import monkey_patch_hca_config

TEST_DATA_PATH = Path(__file__).parents[1] / 'tests' / 'test_data'


class AbstractLoaderTest(unittest.TestCase):

    dss_client: DSSClient

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.dss_endpoint = os.getenv("TEST_DSS_ENDPOINT", "https://hca-dss-4.ucsc-cgp-dev.org/v1")
        cls.staging_bucket = os.getenv('DSS_S3_STAGING_BUCKET', 'commons-dss-upload')

        # Work around problems with DSSClient initialization when there is
        # existing HCA configuration. The following issue has been submitted:
        # Problems accessing an alternate DSS from user scripts or unit tests #170
        # https://github.com/HumanCellAtlas/dcp-cli/issues/170
        monkey_patch_hca_config()
        HCAConfig._user_config_home = '/tmp/'
        dss_config = HCAConfig(name='loader-test', save_on_exit=False, autosave=False)
        dss_config['DSSClient'].swagger_url = f'{cls.dss_endpoint}/swagger.json'
        cls.dss_client = DSSClient(config=dss_config)

    @eventually(timeout_seconds=5.0, retry_interval_seconds=1.0)
    def _search_for_bundle(self, bundle_uuid):
        # Search for the bundle uuid in the DSS and make sure it now exists and uploading was successful
        search_results = self.dss_client.post_search(es_query={'query': {'term': {'uuid': bundle_uuid}}}, replica='aws')
        assert search_results['total_hits'] > 0, 'Not found'
        return search_results

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

    def _load_bundle(self, tmp_json):
        """ run the load script """
        args = ['--no-dry-run',
                '--dss-endpoint',
                f'{self.dss_endpoint}',
                '--staging-bucket',
                f'{self.staging_bucket}',
                f'{self.loader_type}',
                '--json-input-file',
                f'{tmp_json}']
        cgp_data_loader_main(args)


if __name__ == '__main__':
    unittest.main()
