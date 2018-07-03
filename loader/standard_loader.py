import logging
import pprint
import re
import typing
from collections import namedtuple
from typing import Set

from loader.base_loader import DssUploader, MetadataFileUploader

logger = logging.getLogger(__name__)

SCHEMA_URL = ('https://raw.githubusercontent.com/DataBiosphere/metadata-schema/master/'
              'json_schema/cgp/gen3/2.0.0/cgp_gen3_metadata.json')


class ParseError(Exception):
    """To be thrown any time a bundle doesn't contain an expected field"""


# local representation of data necessary to upload a single file
ParsedDataFile = namedtuple('ParsedDataFile', ['filename',
                                               'file_uuid',
                                               'cloud_urls',
                                               'bundle_uuid',
                                               'file_guid',
                                               'file_version'])


class ParsedBundle(namedtuple('ParsedBundle', ['bundle_uuid', 'metadata_dict', 'data_files'])):

    def pprint(self):
        return pprint.pformat(self, indent=4)


class StandardFormatBundleUploader:
    _uuid_regex = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
    # adapted from http://mattallan.org/posts/rfc3339-date-time-validation/
    _rfc3339_regex = re.compile('^(?P<fullyear>\d{4})'
                                '-(?P<month>0[1-9]|1[0-2])'
                                '-(?P<mday>0[1-9]|[12][0-9]|3[01])'
                                'T(?P<hour>[01][0-9]|2[0-3]):(?P<minute>[0-5][0-9]):(?P<second>[0-5][0-9]|60)'
                                '(?P<secfrac>\.[0-9]+)'
                                '?(Z|(\+|-)(?P<offset_hour>[01][0-9]|2[0-3]):(?P<offset_minute>[0-5][0-9]))$')

    def __init__(self, dss_uploader: DssUploader, metadata_file_uploader: MetadataFileUploader) -> None:
        self.dss_uploader = dss_uploader
        self.metadata_file_uploader = metadata_file_uploader

    @classmethod
    def _get_file_uuid(cls, file_guid: str):
        result = cls._uuid_regex.findall(file_guid.lower())
        if result is None:
            raise ParseError(f'Misformatted file_guid: {file_guid} should contain a uuid.')
        if len(result) != 1:
            raise ParseError(f'Misformatted file_guid: {file_guid} contains multiple uuids. Only one was expected.')
        return result[0]

    @classmethod
    def _get_file_version(cls, file_info: dict):
        """Since date updated is optional, we default to date created when it's not updated"""
        def parse_version_key(file_info_, key):
            """return None if version cannot be found"""
            try:
                match = cls._rfc3339_regex.fullmatch(file_info_[key])
                if match is None:
                    logger.warning(f'Failed to parse file version from date {key}: {file_info_[key]}')
                    return None
                return file_info_[key]
            except KeyError:
                return None
        version = parse_version_key(file_info, 'updated')
        if version is None:
            version = parse_version_key(file_info, 'created')
        if version is None:
            raise ParseError('Either bundle had no updated / created time or it was not rfc3339 compliant')
        return version

    @staticmethod
    def _get_cloud_urls(file_info: dict):
        if 'urls' not in file_info:
            raise ParseError(f'URL field not present in file_info: \n{file_info}')
        urls = file_info['urls']
        if len(urls) < 1:
            # FIXME: How many cloud URLs do we ACTUALLY need / expect?
            raise ParseError(f'Expected at least one cloud url in file_info: \n{file_info}')
        for url in urls:
            if 'url' not in url:
                raise ParseError(f"Expected 'url' as key for urls in file_info: \n{file_info}")
        return {url_dict['url'] for url_dict in urls}

    @classmethod
    def _parse_bundle(cls, bundle: dict) -> ParsedBundle:
        try:
            data_bundle = bundle['data_bundle']
            bundle_uuid = data_bundle['id']
            metadata_dict = data_bundle['user_metadata']
            data_objects = bundle['data_objects']
        except KeyError:
            logger.exception('Failed to parse bundle')
            raise ParseError(f'Failed to parse bundle')

        # parse the files within the bundle
        parsed_files = []
        for file_guid in data_objects:
            try:
                file_info = data_objects[file_guid]
                filename = file_info['name']
            except TypeError or KeyError:
                logger.exception('Failed to parse bundle')
                raise ParseError(f'Failed to parse bundle')
            file_uuid = cls._get_file_uuid(file_guid)
            file_version = cls._get_file_version(file_info)
            cloud_urls = cls._get_cloud_urls(file_info)
            parsed_file = ParsedDataFile(filename, file_uuid, cloud_urls, bundle_uuid, file_guid, file_version)
            parsed_files.append(parsed_file)

        return ParsedBundle(bundle_uuid, metadata_dict, parsed_files)

    def _load_bundle(self, bundle_uuid, metadata_dict, data_files):
        """Do the actual loading for an already parsed bundle"""
        logger.info(f'Attempting to load bundle with uuid {bundle_uuid}')
        file_info_list = []

        # load metadata
        metadata_file_uuid, metadata_file_version, metadata_filename = \
            self.metadata_file_uploader.load_dict(metadata_dict,
                                                  "metadata.json",
                                                  SCHEMA_URL,
                                                  bundle_uuid)
        logger.debug(f'Uploaded metadata file: {metadata_filename} with '
                     f'uuid:version {metadata_file_uuid}:{metadata_file_version}')
        file_info_list.append(dict(uuid=metadata_file_uuid, version=metadata_file_version,
                                   name=metadata_filename, indexed=True))

        for data_file in data_files:
            filename, file_uuid, cloud_urls, bundle_uuid, file_guid, file_version, = data_file
            logger.debug(f'Attempting to upload data file: {filename} with uuid:version {file_uuid}:{file_version}...')
            file_uuid, file_version, filename = \
                self.dss_uploader.upload_cloud_file_by_reference(filename,
                                                                 file_uuid,
                                                                 cloud_urls,
                                                                 bundle_uuid,
                                                                 file_guid,
                                                                 file_version=file_version)
            logger.debug(f'...Successfully uploaded data file: {filename} with uuid:version {file_uuid}:{file_version}')
            file_info_list.append(dict(uuid=file_uuid, version=file_version, name=filename, indexed=False))

        # load bundle
        self.dss_uploader.load_bundle(file_info_list, bundle_uuid)

    def _parse_all_bundles(self, input_json,
                           bundles_parsed: typing.List[ParsedBundle],
                           bundles_failed_unparsed: typing.List[dict]):
        """
        Parses all raw json bundles

        :param input_json: The freshly loaded json
        :param bundles_parsed: will contain all of the bundles that were successfully parsed
        :param bundles_failed_unparsed: will contain all of the bundles that were not successfully parsed
        """
        if type(input_json) is not list:
            raise ParseError(f"Json file is misformatted. Expected type: list, actually type {type(input_json)}")

        for count, bundle in enumerate(input_json):
            try:
                parsed_bundle = self._parse_bundle(bundle)
                bundles_parsed.append(parsed_bundle)
            except ParseError:
                logger.exception(f'Could not parse bundle {count + 1}')
                logger.debug(f'Bundle details: \n{pprint.pformat(bundle)}')
                bundles_failed_unparsed.append(bundle)

    def _load_all_bundles(self, bundles_parsed: typing.List[ParsedBundle],
                          bundles_loaded: typing.List[ParsedBundle],
                          bundles_failed_parsed: typing.List[ParsedBundle]):
        """
        Loads already parsed bundles

        :param bundles_parsed: the already parsed bundles
        :param bundles_loaded: will contain all of the bundles that were successfully loaded
        :param bundles_failed_parsed: will contain all of the bundles that were not successfully loaded
        """
        for count, parsed_bundle in enumerate(bundles_parsed):
            logger.info(f'Attempting to load bundle {count + 1}')
            try:
                self._load_bundle(*parsed_bundle)
            except Exception:
                logger.exception(f'Error loading bundle {parsed_bundle.bundle_uuid}')
                logger.debug(f'Bundle details: \n{parsed_bundle.pprint()}')
                bundles_failed_parsed.append(parsed_bundle)
                continue
            bundles_loaded.append(parsed_bundle)
            logger.info(f'Successfully loaded bundle {parsed_bundle.bundle_uuid}')
        return bundles_loaded, bundles_failed_parsed

    def load_all_bundles(self, input_json: typing.List[dict]):
        logger.info(f'Going to load {len(input_json)} bundle{"" if len(input_json) == 1 else "s"}')
        bundles_parsed: typing.List[ParsedBundle] = []
        bundles_failed_unparsed: typing.List[dict] = []
        bundles_loaded: typing.List[ParsedBundle] = []
        bundles_failed_parsed: typing.List[ParsedBundle] = []
        try:
            self._parse_all_bundles(input_json, bundles_parsed, bundles_failed_unparsed)
            self._load_all_bundles(bundles_parsed, bundles_loaded, bundles_failed_parsed)
        except KeyboardInterrupt:
            # The bundle that was being processed during the interrupt isn't recorded anywhere
            logger.exception('Loading canceled with keyboard interrupt')
        finally:
            bundles_unattempted = len(input_json) \
                - len(bundles_failed_unparsed) \
                - len(bundles_failed_parsed) \
                - len(bundles_loaded)
            if bundles_unattempted:
                logger.warning(f'Did not yet attempt to load {bundles_unattempted} bundles')
            if len(bundles_failed_parsed) > 0 or len(bundles_failed_unparsed) > 0:
                logger.error(f'Could not parse {len(bundles_failed_unparsed)} bundles')
                logger.error(f'Could not load {len(bundles_failed_parsed)} bundles')
                # TODO: ADD COMMAND LINE OPTION TO SAVE ERROR LOG TO FILE https://stackoverflow.com/a/11233293/7830612
                logger.info(f'Successfully loaded {len(bundles_loaded)} bundles')
            else:
                logger.info('Successfully loaded all bundles!')
