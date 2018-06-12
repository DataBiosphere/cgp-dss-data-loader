import os
import unittest



class TestStandardInputFormatLoading(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.dss_client = hca.dss.DSSClient()
        cls.dss_client.host = 'https://hca-dss-4.ucsc-cgp-dev.org/v1'
        cls.dss_endpoint = os.getenv('TEST_DSS_ENDPOINT', cls.dss_client.host)
        cls.staging_bucket = os.getenv('DSS_S3_STAGING_BUCKET', 'mbaumann-dss-staging')

    @unittest.skip("Support for the basic import format is not implemented.")
    def test_basic_input_format_loading_from_cli(self):
        # TODO Implement this test
        # json_input_file = f"{self.project_path}/tests/test_data/standard_sample_input.json"
        pass


if __name__ == '__main__':
    unittest.main()
