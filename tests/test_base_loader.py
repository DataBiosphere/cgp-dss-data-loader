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
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.google_project_id = 'platform-dev-178517'
        cls.dss_uploader = base_loader.DssUploader(cls.dss_endpoint, cls.staging_bucket,
                                                   cls.google_project_id, False)
        # file containing a valid AWS AssumedRole ARN
        cls.aws_meta_cred = os.path.abspath('test_data/aws.json')
        # file containing valid GCE credentials
        cls.gce_meta_cred = os.path.abspath('test_data/gce.json')

        # file containing AWS AssumedRole ARN that can't access the data
        cls.bad_aws_meta_cred = os.path.abspath('test_data/aws.json')
        # file containing GCE credentials that can't access the data
        cls.bad_gce_meta_cred = os.path.abspath('test_data/gce.json')

        cls.aws_key = ''
        cls.aws_bucket = ''

        cls.gce_key = ''
        cls.gce_bucket = ''

    def aws_metadata(self, credentials):
        metaclient = self.dss_uploader.mk_s3_metadata_client(credentials)
        response = metaclient.head_object(Bucket=self.aws_bucket, Key=self.aws_key, RequestPayer="requester")
        return response

    def google_metadata(self, credentials):
        metaclient = self.dss_uploader.mk_gs_metadata_client(credentials)
        gs_bucket = metaclient.bucket(self.gce_bucket, self.google_project_id)
        return gs_bucket.get_blob(self.gce_key)

    def test_fetch_private_google_metadata_size(self):
        assert self.google_metadata(self.gce_meta_cred).size

    def test_fetch_private_google_metadata_hash(self):
        assert self.google_metadata(self.gce_meta_cred).crc32c

    def test_fetch_private_google_metadata_type(self):
        assert self.google_metadata(self.gce_meta_cred).content_type

    def test_fetch_private_aws_metadata_size(self):
        assert self.aws_metadata(self.aws_meta_cred)['ContentLength']

    def test_fetch_private_aws_metadata_hash(self):
        assert self.aws_metadata(self.aws_meta_cred)['ETag']

    def test_fetch_private_aws_metadata_type(self):
        assert self.aws_metadata(self.aws_meta_cred)['ContentType']

    def test_bad_google_metadata_fetch(self):
        assert self.google_metadata(self.bad_gce_meta_cred) is None

    def test_bad_aws_metadata_fetch(self):
        self.assertRaises(self.aws_metadata(self.bad_aws_meta_cred), ClientError)
