import json
import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path

import hca

from scripts.cgp_data_loader import main as cgp_data_loader_main


TEST_DATA_PATH = Path(__file__).parents[1] / 'tests' / 'test_data'


class AbstractLoaderTest:

    @classmethod
    def setUpClass(cls):
        cls.dss_client = hca.dss.DSSClient()
        cls.dss_client.host = 'https://hca-dss-4.ucsc-cgp-dev.org/v1'
        cls.dss_endpoint = os.getenv('TEST_DSS_ENDPOINT', cls.dss_client.host)
        cls.staging_bucket = os.getenv('DSS_S3_STAGING_BUCKET', 'mbaumann-dss-staging')

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
