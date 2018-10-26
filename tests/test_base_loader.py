import ast
import copy
import json
import logging
import os
import sys

from google.api_core.exceptions import Forbidden

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from loader import base_loader
from tests.abstract_loader_test import AbstractLoaderTest

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class TestBaseLoader(AbstractLoaderTest):
    """Unittests for base_loader.py."""
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
        # file containing valid GCP credentials
        cls.gcp_meta_cred = os.path.abspath('tests/test_data/gcp.json')
        with open(cls.gcp_meta_cred, 'w') as f:
            json.dump(ast.literal_eval(os.environ['TRAVISUSER_GOOGLE_CREDENTIALS']), f)

        cls.aws_bucket = 'travis-test-loader-dont-delete'
        cls.aws_key = 'pangur.txt'

        cls.gcp_bucket = 'travis-test-loader-dont-delete'
        cls.gcp_key = 'drinking.txt'

        cls.dss_uploader = base_loader.DssUploader(cls.dss_endpoint, cls.staging_bucket, cls.google_project_id, False)

    @classmethod
    def tearDownClass(cls):
        # Switch permissions back from the underprivileged service account to the default that travis was set to.
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = cls.stored_credentials
        if os.path.exists(cls.aws_meta_cred):
            os.remove(cls.aws_meta_cred)
        if os.path.exists(cls.gcp_meta_cred):
            os.remove(cls.gcp_meta_cred)

    def aws_metadata(self, credentials):
        """Fetches a credentialed client using the get_gs_metadata_client() function."""
        metaclient = self.dss_uploader.get_s3_metadata_client(credentials, session='travis', duration=3600)
        response = metaclient.head_object(Bucket=self.aws_bucket, Key=self.aws_key, RequestPayer="requester")
        return response

    def google_metadata(self, credentials):
        """Fetches a credentialed client using the get_s3_metadata_client() function."""
        metaclient = self.dss_uploader.get_gs_metadata_client(credentials)
        gs_bucket = metaclient.bucket(self.gcp_bucket, self.google_project_id)
        return gs_bucket.get_blob(self.gcp_key)

    def test_fetch_private_google_metadata_size(self):
        """Fetch file size.  Tests: get_gs_metadata_client()."""
        assert self.google_metadata(self.gcp_meta_cred).size

    def test_fetch_private_google_metadata_hash(self):
        """Fetch file hash.  Tests: get_gs_metadata_client()."""
        assert self.google_metadata(self.gcp_meta_cred).crc32c

    def test_fetch_private_google_metadata_type(self):
        """Fetch file content-type.  Tests: get_gs_metadata_client()."""
        assert self.google_metadata(self.gcp_meta_cred).content_type

    def test_fetch_private_aws_metadata_size(self):
        """Fetch file size.  Tests: get_s3_metadata_client()."""
        assert self.aws_metadata(self.aws_meta_cred)['ContentLength']

    def test_fetch_private_aws_metadata_hash(self):
        """Fetch file etag hash.  Tests: get_s3_metadata_client()."""
        assert self.aws_metadata(self.aws_meta_cred)['ETag']

    def test_fetch_private_aws_metadata_type(self):
        """Fetch file content-type.  Tests: get_s3_metadata_client()."""
        assert self.aws_metadata(self.aws_meta_cred)['ContentType']

    def test_bad_google_metadata_fetch(self):
        """Assert that using the default credentials will fail."""
        try:
            self.dss_uploader.get_gs_file_metadata(self.gcp_bucket, self.gcp_key)
            raise RuntimeError('User should be forbidden and somehow has access anyway.')  # skipped if running properly
        except Forbidden:
            pass

    def test_bad_aws_metadata_fetch(self):
        """Assert that using the default credentials will fail."""
        assert not self.dss_uploader.get_s3_file_metadata(self.aws_bucket, self.aws_key)
