import ast
import copy
import json
import logging
import os
import sys
import uuid

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from loader import base_loader
from tests.abstract_loader_test import AbstractLoaderTest

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class TestBaseLoaderIntegration(AbstractLoaderTest):
    """Integration tests for base_loader.py."""
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.google_project_id = 'platform-dev-178517'

        # Service account: travis-underpriveleged-tester@platform-dev-178517.iam.gserviceaccount.com
        # Has only viewer level permissions, while the bucket requires at least editor level.
        # Only use these permissions for the tests in this file.
        underprivileged_credentials = os.path.abspath('underprivileged_credentials.json')
        with open(underprivileged_credentials, 'w') as f:
            f.write(os.environ['UNDERPRIVILEGED_TRAVIS_APP_CREDENTIALS'])
        cls.stored_credentials = copy.deepcopy(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = underprivileged_credentials

        # file containing a valid AWS AssumedRole ARN
        cls.aws_meta_cred = os.path.abspath('tests/test_data/aws.config')
        with open(cls.aws_meta_cred, 'w') as f:
            f.write('arn:aws:iam::719818754276:role/travis_access_test_bucket')
        # file containing valid GCP credentials (travis.platform.dev@gmail.com; editor permissions)
        cls.gcp_meta_cred = os.path.abspath('tests/test_data/gcp.json')
        with open(cls.gcp_meta_cred, 'w') as f:
            json.dump(ast.literal_eval(os.environ['TRAVISUSER_GOOGLE_CREDENTIALS']), f)

        cls.aws_bucket = 'travis-test-loader-dont-delete'
        cls.aws_key = 'pangur.txt'

        cls.gcp_bucket = 'travis-test-loader-dont-delete'
        cls.gcp_key = 'drinking.txt'

        cls.dss_uploader = base_loader.DssUploader(cls.dss_endpoint, cls.staging_bucket, cls.google_project_id,
                                                   False, cls.aws_meta_cred, cls.gcp_meta_cred)

    @classmethod
    def tearDownClass(cls):
        # Switch permissions back from the underprivileged service account to the default that travis was set to.
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = cls.stored_credentials
        if os.path.exists(cls.aws_meta_cred):
            os.remove(cls.aws_meta_cred)
        if os.path.exists(cls.gcp_meta_cred):
            os.remove(cls.gcp_meta_cred)

    def test_aws_fetch_file_with_metadata_credentials_needed(self):
        """
        Make sure that the self.dss_uploader object properly supplied with the optional metadata
        credentials can fetch metadata it couldn't otherwise fetch from AWS.
        """
        self.dss_uploader.dry_run = True
        self.dss_uploader.upload_cloud_file_by_reference('pangur.txt',
                                                         uuid.uuid4(),
                                                         {'s3://travis-test-loader-dont-delete/pangur.txt'},
                                                         395,
                                                         uuid.uuid4(),
                                                         1)

    def test_gcp_fetch_file_with_metadata_credentials_needed(self):
        """
        Make sure that the self.dss_uploader object properly supplied with the optional metadata
        credentials can fetch metadata it couldn't otherwise fetch from GCP.
        """
        self.dss_uploader.dry_run = True
        self.dss_uploader.upload_cloud_file_by_reference('drinking.txt',
                                                         uuid.uuid4(),
                                                         {'gs://travis-test-loader-dont-delete/drinking.txt'},
                                                         439,
                                                         uuid.uuid4(),
                                                         1)