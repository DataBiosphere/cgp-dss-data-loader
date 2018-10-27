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

        # run as a google service account with only viewer access
        cls.stored_credentials = cls.set_underprivileged_google_client()

        # turn travis env vars into input files and return the paths
        cls.aws_meta_cred, cls.gcp_meta_cred = cls.create_metadata_files()

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
        self.dss_uploader.upload_cloud_file_by_reference(f'{self.base_loader_aws_key}',
                                                         uuid.uuid4(),
                                                         {f's3://{self.base_loader_aws_bucket}/{self.base_loader_aws_key}'},
                                                         395,
                                                         uuid.uuid4(),
                                                         1)

    def test_gcp_fetch_file_with_metadata_credentials_needed(self):
        """
        Make sure that the self.dss_uploader object properly supplied with the optional metadata
        credentials can fetch metadata it couldn't otherwise fetch from GCP.
        """
        self.dss_uploader.dry_run = True
        self.dss_uploader.upload_cloud_file_by_reference(f'{self.base_loader_gcp_key}',
                                                         uuid.uuid4(),
                                                         {f'gs://{self.base_loader_gcp_bucket}/{self.base_loader_gcp_key}'},
                                                         439,
                                                         uuid.uuid4(),
                                                         1)
