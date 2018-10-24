import logging
import os
import sys
from botocore.exceptions import ClientError

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from loader import base_loader
from tests.abstract_loader_test import AbstractLoaderTest

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class TestBaseLoader(AbstractLoaderTest):
    """Unittests for base_loader.py."""
    # TODO: add a test in the integration tests
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.google_project_id = 'platform-dev-178517'
        cls.dss_uploader = base_loader.DssUploader(cls.dss_endpoint, cls.staging_bucket,
                                                   cls.google_project_id, False)
        # file containing a valid AWS AssumedRole ARN
        cls.aws_meta_cred = os.path.abspath('test_data/aws.config')
        # file containing valid GCP credentials
        cls.gcp_meta_cred = os.path.abspath('test_data/gcp.json')

        # file containing AWS AssumedRole ARN that can't access the data
        cls.bad_aws_meta_cred = os.path.abspath('test_data/aws_bad.config')
        # file containing GCP credentials that can't access the data
        cls.bad_gcp_meta_cred = os.path.abspath('test_data/gcp_bad.json')

        cls.aws_key = ''
        cls.aws_bucket = ''

        cls.gcp_key = ''
        cls.gcp_bucket = ''

    def aws_metadata(self, credentials):
        """Fetches a credentialed client using the get_gs_metadata_client() function."""
        metaclient = self.dss_uploader.get_s3_metadata_client(credentials)
        response = metaclient.head_object(Bucket=self.aws_bucket, Key=self.aws_key, RequestPayer="requester")
        return response

    def google_metadata(self, credentials):
        """Fetches a credentialed client using the get_s3_metadata_client() function."""
        metaclient = self.dss_uploader.get_gs_metadata_client(credentials)
        gs_bucket = metaclient.bucket(self.gcp_bucket, self.google_project_id)
        return gs_bucket.get_blob(self.gcp_key)

    # def test_fetch_private_google_metadata_size(self):
    #     assert self.google_metadata(self.gcp_meta_cred).size
    #
    # def test_fetch_private_google_metadata_hash(self):
    #     assert self.google_metadata(self.gcp_meta_cred).crc32c
    #
    # def test_fetch_private_google_metadata_type(self):
    #     assert self.google_metadata(self.gcp_meta_cred).content_type
    #
    # def test_fetch_private_aws_metadata_size(self):
    #     assert self.aws_metadata(self.aws_meta_cred)['ContentLength']
    #
    # def test_fetch_private_aws_metadata_hash(self):
    #     assert self.aws_metadata(self.aws_meta_cred)['ETag']
    #
    # def test_fetch_private_aws_metadata_type(self):
    #     assert self.aws_metadata(self.aws_meta_cred)['ContentType']
    #
    # def test_bad_google_metadata_fetch(self):
    #     assert self.google_metadata(self.bad_gcp_meta_cred) is None
    #
    # def test_bad_aws_metadata_fetch(self):
    #     self.assertRaises(self.aws_metadata(self.bad_aws_meta_cred), ClientError)