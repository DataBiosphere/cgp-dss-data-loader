import datetime
import unittest
import uuid

from loader.standard_loader import StandardFormatBundleUploader, ParsedBundle, ParseError


class TestStandardLoader(unittest.TestCase):
    """unit tests for loader components"""

    def test_parse_bundle(self):
        """
        Check that our basic parsing of the bundle works.

        This won't cover everything though because there are still some implicit assumptions
        about a bundle as its being loaded such as ...?? version info? name? not sure, should double check.
        """
        bundle = {}
        self.assertRaises(ParseError, StandardFormatBundleUploader._parse_bundle, bundle)
        data_bundle = {}
        bundle['data_bundle'] = data_bundle
        self.assertRaises(ParseError, StandardFormatBundleUploader._parse_bundle, bundle)
        data_bundle['id'] = 'anything'
        self.assertRaises(ParseError, StandardFormatBundleUploader._parse_bundle, bundle)
        data_bundle['user_metadata'] = 'a thing'
        self.assertRaises(ParseError, StandardFormatBundleUploader._parse_bundle, bundle)
        bundle['data_objects'] = [1, 2, 3]
        self.assertTrue(type(StandardFormatBundleUploader._parse_bundle(bundle)) == ParsedBundle)

    def test_get_file_uuid(self):
        """
        Currently uuids are in the guid, but who knows what we may get
        """
        file_guid = 'what\'s a guid anyway?'
        self.assertRaises(ParseError, StandardFormatBundleUploader._get_file_uuid, file_guid)
        file_guid = str(uuid.uuid4())
        result = StandardFormatBundleUploader._get_file_uuid(file_guid)
        self.assertEqual(file_guid, result)
        input_uuid = str(uuid.uuid4())
        file_guid = f'dg.4056/{input_uuid}'
        result = StandardFormatBundleUploader._get_file_uuid(file_guid)
        self.assertEqual(input_uuid, result)
        file_guid = f'dg.4056/{uuid.uuid4()}/important/{uuid.uuid4()}'
        self.assertRaises(ParseError, StandardFormatBundleUploader._get_file_uuid, file_guid)

    def test_get_file_version(self):
        """
        Check that the version is obtainable, and if it's not, then the correct error is thrown
        """
        file_info = {}
        self.assertRaises(ParseError, StandardFormatBundleUploader._get_file_version, file_info)
        file_info['updated'] = 'not a rfc compliant datetime'
        self.assertRaises(ParseError, StandardFormatBundleUploader._get_file_version, file_info)
        file_info['created'] = 'not a rfc compliant datetime'
        self.assertRaises(ParseError, StandardFormatBundleUploader._get_file_version, file_info)
        u_compliant_datetime = datetime.datetime.now(datetime.timezone.utc).isoformat()
        file_info['updated'] = u_compliant_datetime
        self.assertTrue(StandardFormatBundleUploader._get_file_version(file_info) == u_compliant_datetime)
        # get new datetime just in case
        c_compliant_datetime = datetime.datetime.now(datetime.timezone.utc).isoformat()
        file_info['created'] = c_compliant_datetime
        self.assertTrue(StandardFormatBundleUploader._get_file_version(file_info) == u_compliant_datetime)
        file_info['updated'] = 'not compliant again'
        self.assertTrue(StandardFormatBundleUploader._get_file_version(file_info) == c_compliant_datetime)

    def test_get_cloud_urls(self):
        """
        test that cloud urls are parsed properly, and if not then the correct error is thrown
        """
        file_info = {}
        self.assertRaises(ParseError, StandardFormatBundleUploader._get_cloud_urls, file_info)
        urls = []
        file_info['urls'] = urls
        self.assertRaises(ParseError, StandardFormatBundleUploader._get_cloud_urls, file_info)
        urls.append('not the expected dictionary')
        self.assertRaises(ParseError, StandardFormatBundleUploader._get_cloud_urls, file_info)
        urls.pop()
        good_url = 's3://a/beautiful/url'
        urls.append({'url': good_url})
        self.assertTrue(good_url in StandardFormatBundleUploader._get_cloud_urls(file_info))


