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

        # run as a google service account with only viewer access
        cls.stored_credentials = cls.set_underprivileged_google_client()

        # turn travis env vars into input files and return the paths
        cls.aws_meta_cred, cls.gcp_meta_cred = cls.create_metadata_files()

        cls.dss_uploader = base_loader.DssUploader(cls.dss_endpoint, cls.staging_bucket, cls.google_project_id,
                                                   False)

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
        response = metaclient.head_object(Bucket=self.base_loader_aws_bucket, Key=self.base_loader_aws_key, RequestPayer="requester")
        return response

    def google_metadata(self, credentials):
        """Fetches a credentialed client using the get_s3_metadata_client() function."""
        metaclient = self.dss_uploader.get_gs_metadata_client(credentials)
        gs_bucket = metaclient.bucket(self.base_loader_gcp_bucket, self.google_project_id)
        return gs_bucket.get_blob(self.base_loader_gcp_key)

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
            self.dss_uploader.get_gs_file_metadata(self.base_loader_gcp_bucket, self.base_loader_gcp_key)
            raise RuntimeError('User should be forbidden and somehow has access anyway.')  # skipped if running properly
        except Forbidden:
            pass

    def test_bad_aws_metadata_fetch(self):
        """Assert that using the default credentials will fail."""
        assert not self.dss_uploader.get_s3_file_metadata(self.base_loader_aws_bucket, self.base_loader_aws_key)
