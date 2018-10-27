import ast
import copy
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
        cls.google_project_id = 'platform-dev-178517'
        cls.dss_endpoint = os.getenv("TEST_DSS_ENDPOINT", "https://hca-dss-4.ucsc-cgp-dev.org/v1")
        cls.staging_bucket = os.getenv('DSS_S3_STAGING_BUCKET', 'commons-dss-upload')

        # Buckets/files for the base loader tests.
        cls.base_loader_aws_bucket = 'travis-test-loader-dont-delete'
        cls.base_loader_aws_key = 'pangur.txt'
        cls.base_loader_gcp_bucket = 'travis-test-loader-dont-delete'
        cls.base_loader_gcp_key = 'drinking.txt'

        # Work around problems with DSSClient initialization when there is
        # existing HCA configuration. The following issue has been submitted:
        # Problems accessing an alternate DSS from user scripts or unit tests #170
        # https://github.com/HumanCellAtlas/dcp-cli/issues/170
        monkey_patch_hca_config()
        HCAConfig._user_config_home = '/tmp/'
        dss_config = HCAConfig(name='loader-test', save_on_exit=False, autosave=False)
        dss_config['DSSClient'].swagger_url = f'{cls.dss_endpoint}/swagger.json'
        cls.dss_client = DSSClient(config=dss_config)

    @staticmethod
    def set_underprivileged_google_client():
        # Service account: travis-underpriveleged-tester@platform-dev-178517.iam.gserviceaccount.com
        # Has only viewer level permissions, and can revoke access if a bucket requires at least editor level.
        # Call this function to run as this service account.  Return default credentials (allowing one to
        # reset when done).
        underprivileged_credentials = os.path.abspath('underprivileged_credentials.json')
        with open(underprivileged_credentials, 'w') as f:
            f.write(os.environ['UNDERPRIVILEGED_TRAVIS_APP_CREDENTIALS'])
        stored_credentials = copy.deepcopy(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = underprivileged_credentials
        return stored_credentials

    @staticmethod
    def create_metadata_files():
        """Creates 2 files appropriate for use as metadata credentialing inputs."""
        # file containing a valid AWS AssumedRole ARN
        aws_meta_cred = os.path.abspath('tests/test_data/aws.config')
        with open(aws_meta_cred, 'w') as f:
            f.write('arn:aws:iam::719818754276:role/travis_access_test_bucket')
        # file containing valid GCP credentials (travis.platform.dev@gmail.com; editor permissions)
        gcp_meta_cred = os.path.abspath('tests/test_data/gcp.json')
        with open(gcp_meta_cred, 'w') as f:
            json.dump(ast.literal_eval(os.environ['TRAVISUSER_GOOGLE_CREDENTIALS']), f)
        return aws_meta_cred, gcp_meta_cred

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
