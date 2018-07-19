import datetime
import io
import logging
import os
import typing
import unittest
import uuid
from pathlib import Path

import boto3
import hca

from loader import base_loader
from loader.base_loader import FileURLError
from loader.standard_loader import StandardFormatBundleUploader, ParsedBundle, ParseError, ParsedDataFile
from scripts.cgp_data_loader import GOOGLE_PROJECT_ID
from util import load_json_from_file

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logging.getLogger(__name__)

TEST_DATA_PATH = Path(__file__).parents[1] / 'tests' / 'test_data'


def tz_utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def utc_now():
    return datetime.datetime.utcnow().isoformat()


class TestLoader(unittest.TestCase):
    """unit tests for loader parsing components"""

    @classmethod
    def setUpClass(cls):
        cls.dss_client = hca.dss.DSSClient()
        cls.dss_client.host = 'https://hca-dss-4.ucsc-cgp-dev.org/v1'
        cls.dss_endpoint = os.getenv("TEST_DSS_ENDPOINT", "https://hca-dss-4.ucsc-cgp-dev.org/v1")
        cls.staging_bucket = os.getenv("DSS_S3_STAGING_BUCKET", "mbaumann-dss-staging")
        cls.dss_uploader = base_loader.DssUploader(cls.dss_endpoint, cls.staging_bucket,
                                                   GOOGLE_PROJECT_ID, False)
        cls.metadata_uploader = base_loader.MetadataFileUploader(cls.dss_uploader)

        # create test bucket and upload test files
        cls.s3 = boto3.resource('s3')
        cls.bucket_name = f'loader-test-bucket-{uuid.uuid4()}'
        cls.s3.create_bucket(Bucket=cls.bucket_name, CreateBucketConfiguration={
            'LocationConstraint': 'us-west-2'})
        cls.bucket = cls.s3.Bucket(cls.bucket_name)
        cls.bucket.Acl().put(ACL='public-read')

    def setUp(self):
        self.loader = StandardFormatBundleUploader(self.dss_uploader, self.metadata_uploader)

    @classmethod
    def tearDownClass(cls):
        # empty and delete aws bucket
        for key in cls.bucket.objects.all():
            key.delete()
        cls.bucket.delete()

    def test_bucket_exists(self):
        """Just a sanity check to make sure test files are uploaded"""
        self.s3.meta.client.head_bucket(Bucket=self.bucket_name)

    def test_get_file_uuid(self):
        """
        Currently uuids are in the guid, but who knows what we may get
        """
        file_guid = 'what\'s a guid anyway?'
        self.assertRaises(ParseError, self.loader._get_file_uuid, file_guid)
        file_guid = str(uuid.uuid4())
        result = self.loader._get_file_uuid(file_guid)
        self.assertEqual(file_guid, result)
        input_uuid = str(uuid.uuid4())
        file_guid = f'dg.4056/{input_uuid}'
        result = self.loader._get_file_uuid(file_guid)
        self.assertEqual(input_uuid, result)
        file_guid = f'dg.4056/{uuid.uuid4()}/important/{uuid.uuid4()}'
        self.assertRaises(ParseError, self.loader._get_file_uuid, file_guid)

    def test_get_file_version(self):
        """
        Check that the version is obtainable, and if it's not, then the correct error is thrown
        """
        file_info = {}
        self.assertRaises(ParseError, self.loader._get_file_version, file_info)
        file_info['updated'] = 'not a rfc compliant datetime'
        self.assertRaises(ParseError, self.loader._get_file_version, file_info)
        file_info['created'] = 'not a rfc compliant datetime'
        self.assertRaises(ParseError, self.loader._get_file_version, file_info)
        u_compliant_datetime = tz_utc_now()
        file_info['updated'] = u_compliant_datetime
        self.assertTrue(self.loader._get_file_version(file_info) == u_compliant_datetime)
        # what if we don't have timezone info?
        u_compliant_datetime = utc_now()
        file_info['updated'] = u_compliant_datetime
        self.assertTrue(self.loader._get_file_version(file_info) == u_compliant_datetime)
        # get new datetime just in case
        c_compliant_datetime = tz_utc_now()
        file_info['created'] = c_compliant_datetime
        self.assertTrue(self.loader._get_file_version(file_info) == u_compliant_datetime)
        file_info['updated'] = 'not compliant again'
        self.assertTrue(self.loader._get_file_version(file_info) == c_compliant_datetime)

    def test_get_cloud_urls(self):
        """
        test that cloud urls are parsed properly, and if not then the correct error is thrown
        """
        file_info = {}
        self.assertRaises(ParseError, self.loader._get_cloud_urls, file_info)
        urls = []
        file_info['urls'] = urls
        self.assertRaises(ParseError, self.loader._get_cloud_urls, file_info)
        urls.append('not the expected dictionary')
        self.assertRaises(ParseError, self.loader._get_cloud_urls, file_info)
        urls.pop()
        good_url = 's3://a/beautiful/url'
        urls.append({'url': good_url})
        self.assertTrue(good_url in self.loader._get_cloud_urls(file_info))

    def test_parse_bundle(self):
        """
        Check that our basic parsing of the bundle works.

        This doesn't validate a bundle's urls however
        """
        minimal_file_info_guid = f'dg.405/{uuid.uuid4()}'
        minimal_file_info = {'name': 'buried_treasure_map',
                             'created': tz_utc_now(),
                             'urls': [{'url': 's3://desert/island/under/palm'},
                                      {'url': 'gs://captains/quarters/bottom/drawer'}]}
        bundle = {}
        self.assertRaises(ParseError, self.loader._parse_bundle, bundle)
        data_bundle = {}
        bundle['data_bundle'] = data_bundle
        self.assertRaises(ParseError, self.loader._parse_bundle, bundle)
        data_bundle['id'] = 'anything'
        self.assertRaises(ParseError, self.loader._parse_bundle, bundle)
        data_bundle['user_metadata'] = 'a thing'
        self.assertRaises(ParseError, self.loader._parse_bundle, bundle)
        bundle['data_objects'] = 'not a list of objects'
        self.assertRaises(ParseError, self.loader._parse_bundle, bundle)
        bundle['data_objects'] = [1, 2, 3]
        self.assertRaises(ParseError, self.loader._parse_bundle, bundle)
        incomplete_file_info = {'name': 'buried_treasure_map'}
        bundle['data_objects'] = {minimal_file_info_guid: incomplete_file_info}
        self.assertRaises(ParseError, self.loader._parse_bundle, bundle)
        incomplete_file_info.update(minimal_file_info)
        self.assertTrue(type(self.loader._parse_bundle(bundle)) == ParsedBundle)

    def test_parse_all_bundles(self):
        """
        Try and parse multiple bundles.

        The sample file is just an already transformed standard format version of the TopMed public 107
        """
        self.assertRaises(ParseError, self.loader._parse_all_bundles, 'not a json')

        input_json = load_json_from_file(str(TEST_DATA_PATH / 'multiple_bundles.json'))
        self.loader._parse_all_bundles(input_json)
        self.assertEqual(len(self.loader.bundles_parsed), len(input_json))
        self.assertEqual(len(self.loader.bundles_failed_unparsed), 0)

    # TODO add some tests for credentials and stuff so that we get nice error messages

    def _test_loading_bundles_dict(self, bundles: typing.List[dict]):
        # Nothing should have been processed at this point
        self.assertEqual(len(self.loader.bundles_parsed), 0)
        self.assertEqual(len(self.loader.bundles_loaded), 0)
        self.assertEqual(len(self.loader.bundles_failed_unparsed), 0)
        self.assertEqual(len(self.loader.bundles_failed_parsed), 0)

        self.loader.load_all_bundles(bundles)

        self.assertEqual(len(self.loader.bundles_loaded), len(bundles))
        self.assertEqual(len(self.loader.bundles_parsed), len(bundles))
        self.assertEqual(len(self.loader.bundles_failed_unparsed), 0)
        self.assertEqual(len(self.loader.bundles_failed_parsed), 0)

    def _make_minimal_bundle(self, parsed=True):
        bundle_uuid = str(uuid.uuid4())
        metadata_dict = {'some': 'stuff', 'more': 'stuff'}

        file_contents = io.BytesIO(b'This is a very important file.\n'
                                   b'The content is pretty important but is self referential\n')
        filename = f'minimal-file-{uuid.uuid4()}'
        file_uuid = str(uuid.uuid4())
        file_guid = f'dg.405/{file_uuid}'
        file_version = tz_utc_now()
        # we only need one URL, but it needs to be valid
        cloud_urls = [f's3://{self.bucket_name}/{filename}']

        # do the cloud upload
        file = self.s3.Object(self.bucket_name, filename)
        file.put(Body=file_contents)
        file.Acl().put(ACL='public-read')

        if parsed:
            return ParsedBundle(bundle_uuid,
                                metadata_dict,
                                [ParsedDataFile(filename, file_uuid, cloud_urls, bundle_uuid, file_guid, file_version)])
        else:
            minimal = {
                'data_bundle': {
                    'id': bundle_uuid,
                    'data_object_ids': file_guid,
                    'created': tz_utc_now(),  # FIXME is this necessary?????
                    'user_metadata': {'some': 'stuff',
                                      'more': 'stuff'}
                },
                'data_objects': {
                    file_guid: {
                        'name': 'minimal_bundle_test_file',
                        'created': file_version,
                        'id': file_guid,
                        'urls': [{'url': url} for url in cloud_urls]  # FIXME fail if url is bad
                    }
                }
            }
            return minimal

    def test_no_bundles_dict(self):
        self._test_loading_bundles_dict([])

    def test_minimal_bundle_dict(self):
        """Try and load a minimally formed bundle"""
        self._test_loading_bundles_dict([self._make_minimal_bundle(parsed=False)])

    def test_multiple_bundles_dict(self):
        """If one works, how about a few?"""
        self._test_loading_bundles_dict([self._make_minimal_bundle(parsed=False) for _ in range(5)])

    def test_minimal_bundle_parsed(self):
        """Try and load a minimally formed bundle"""
        self.loader._load_bundle(*self._make_minimal_bundle(parsed=True))

    def test_duplicate_file_upload(self):
        """
        We don't want data files to be re-uploaded if a new bundle uses a file that's already in another bundle

        TODO: eventually this test should be moved to a different suite that tests the base_loader directly
        """
        _, _, data_files = self._make_minimal_bundle()
        data_file = data_files[0]
        filename, file_uuid, cloud_urls, bundle_uuid, file_guid, file_version, = data_file

        _, _, _, already_present = \
            self.dss_uploader.upload_cloud_file_by_reference(filename,
                                                             file_uuid,
                                                             cloud_urls,
                                                             bundle_uuid,
                                                             file_guid,
                                                             file_version=file_version)
        # make sure the file hasn't already been uploaded
        self.assertFalse(already_present)
        _, _, _, already_present = \
            self.dss_uploader.upload_cloud_file_by_reference(filename,
                                                             file_uuid,
                                                             cloud_urls,
                                                             bundle_uuid,
                                                             file_guid,
                                                             file_version=file_version)
        # make sure the file HAS already been uploaded
        self.assertTrue(already_present)

    def test_bad_URL(self):
        """Make sure a bundle with a invalid URL fails"""
        bundle = self._make_minimal_bundle(parsed=True)
        bundle.data_files[0].cloud_urls[0] = 'https://example.com'
        self.assertRaises(FileURLError, self.loader._load_bundle, *bundle)
        bundle.data_files[0].cloud_urls[0] = 's3://definatelynotavalidbucketorfile'
        self.assertRaises(FileURLError, self.loader._load_bundle, *bundle)
        bundle.data_files[0].cloud_urls[0] = 'gs://definatelynotavalidbucketorfile'
        self.assertRaises(FileURLError, self.loader._load_bundle, *bundle)
