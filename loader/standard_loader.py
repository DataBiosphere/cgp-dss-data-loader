import concurrent.futures
import logging
import pprint
import re
import typing

from loader.base_loader import DssUploader, MetadataFileUploader
from util import patch_connection_pools, tz_utc_now

logger = logging.getLogger(__name__)

SCHEMA_URL = ('https://raw.githubusercontent.com/DataBiosphere/metadata-schema/master/'
              'json_schema/cgp/gen3/2.0.0/cgp_gen3_metadata.json')


class ParseError(Exception):
    """To be thrown any time a bundle doesn't contain an expected field"""


class ParsedDataFile(typing.NamedTuple):
    """local representation of data necessary to upload a single file"""
    filename: str
    file_uuid: str
    cloud_urls: typing.List[str]  # list of urls
    bundle_uuid: str
    file_guid: str
    file_version: str  # rfc3339


class ParsedBundle(typing.NamedTuple):
    bundle_uuid: str
    metadata_dict: dict
    data_files: typing.List[ParsedDataFile]

    def pprint(self):
        return pprint.pformat(self, indent=4)


class StandardFormatBundleUploader:
    _uuid_regex = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
    # adapted from http://mattallan.org/posts/rfc3339-date-time-validation/
    _rfc3339_regex = re.compile('^(?P<fullyear>\d{4})'
                                '-(?P<month>0[1-9]|1[0-2])'
                                '-(?P<mday>0[1-9]|[12][0-9]|3[01])'
                                'T(?P<hour>[01][0-9]|2[0-3]):(?P<minute>[0-5][0-9]):(?P<second>[0-5][0-9]|60)'
                                '(?P<secfrac>\.[0-9]+)?'
                                '(Z|(\+|-)(?P<offset_hour>[01][0-9]|2[0-3]):(?P<offset_minute>[0-5][0-9]))?$')

    def __init__(self, dss_uploader: DssUploader, metadata_file_uploader: MetadataFileUploader) -> None:
        self.dss_uploader = dss_uploader
        self.metadata_file_uploader = metadata_file_uploader
        # these will probably need to be made into queues for parallelization
        self.bundles_parsed: typing.List[ParsedBundle] = []
        self.bundles_failed_unparsed: typing.List[dict] = []
        self.bundles_loaded: typing.List[ParsedBundle] = []
        self.bundles_failed_parsed: typing.List[ParsedBundle] = []

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
            raise ParseError(f'Expected at least one cloud url in file_info: \n{file_info}')
        for url in urls:
            if 'url' not in url:
                raise ParseError(f"Expected 'url' as key for urls in file_info: \n{file_info}")
        return [url_dict['url'] for url_dict in urls]

    @classmethod
    def _parse_bundle(cls, bundle: dict) -> ParsedBundle:
        try:
            data_bundle = bundle['data_bundle']
            bundle_uuid = data_bundle['id']
            metadata_dict = data_bundle['user_metadata']
            data_objects = bundle['data_objects']
        except KeyError as e:
            raise ParseError(f'Failed to parse bundle') from e

        # parse the files within the bundle
        parsed_files = []
        for file_guid in data_objects:
            try:
                file_info = data_objects[file_guid]
                filename = file_info['name']
            except TypeError or KeyError as e:
                raise ParseError(f'Failed to parse bundle') from e
            file_uuid = cls._get_file_uuid(file_guid)
            file_version = cls._get_file_version(file_info)
            cloud_urls = cls._get_cloud_urls(file_info)
            parsed_file = ParsedDataFile(filename, file_uuid, cloud_urls, bundle_uuid, file_guid, file_version)
            parsed_files.append(parsed_file)

        return ParsedBundle(bundle_uuid, metadata_dict, parsed_files)

    def _load_bundle(self, bundle_uuid, metadata_dict, data_files, bundle_num):
        """Do the actual loading for an already parsed bundle"""
        logger.info(f'Bundle {bundle_num}: Attempting to load. UUID: {bundle_uuid}')
        file_info_list = []

        # load metadata, ignore whether the file was already present
        metadata_file_uuid, metadata_file_version, metadata_filename, _ = \
            self.metadata_file_uploader.load_dict(metadata_dict,
                                                  "metadata.json",
                                                  SCHEMA_URL,
                                                  bundle_uuid,
                                                  # just use current time since there is no better source :/
                                                  file_version=tz_utc_now())
        logger.debug(f'Bundle {bundle_num}: Uploaded metadata file: {metadata_filename} with '
                     f'uuid:version {metadata_file_uuid}:{metadata_file_version}')
        file_info_list.append(dict(uuid=metadata_file_uuid, version=metadata_file_version,
                                   name=metadata_filename, indexed=True))

        for data_file in data_files:
            filename, file_uuid, cloud_urls, bundle_uuid, file_guid, file_version, = data_file
            logger.debug(f'Bundle {bundle_num}: Attempting to upload data file: {filename} '
                         f'with uuid:version {file_uuid}:{file_version}...')
            file_uuid, file_version, filename, already_present = \
                self.dss_uploader.upload_cloud_file_by_reference(filename,
                                                                 file_uuid,
                                                                 cloud_urls,
                                                                 bundle_uuid,
                                                                 file_guid,
                                                                 file_version=file_version)
            if already_present:
                logger.debug('Bundle {bundle_num}: File {filename} already present. No upload necessary.')
            logger.debug(f'Bundle {bundle_num}: ...Successfully uploaded data file: {filename} '
                         f'with uuid:version {file_uuid}:{file_version}')
            file_info_list.append(dict(uuid=file_uuid, version=file_version, name=filename, indexed=False))

        # load bundle
        self.dss_uploader.load_bundle(file_info_list, bundle_uuid)

    def _parse_all_bundles(self, input_json):
        """Parses all raw json bundles"""
        if type(input_json) is not list:
            raise ParseError(f"Json file is misformatted. Expected type: list, actually type {type(input_json)}")

        for count, bundle in enumerate(input_json):
            try:
                parsed_bundle = self._parse_bundle(bundle)
                self.bundles_parsed.append(parsed_bundle)
            except ParseError:
                logger.exception(f'Could not parse bundle {count}')
                logger.debug(f'Bundle details: \n{pprint.pformat(bundle)}')
                self.bundles_failed_unparsed.append(bundle)

    def _load_bundle_concurrent(self, count, parsed_bundle):
        logger.info(f'Bundle {count}: Attempting to load ')
        try:
            self._load_bundle(*parsed_bundle, count)
        except Exception:
            logger.exception(f'Bundle {count}: Error loading. ID: {parsed_bundle.bundle_uuid}')
            logger.debug(f'Bundle {count} details: \n{parsed_bundle.pprint()}')
            self.bundles_failed_parsed.append(parsed_bundle)
            return
        self.bundles_loaded.append(parsed_bundle)
        logger.info(f'Bundle {count}: Successfully loaded. ID: {parsed_bundle.bundle_uuid}')

    def _load_parsed_bundles_concurrent(self):
        """Loads already parsed bundles concurrently using threads"""
        patch_connection_pools(maxsize=256)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self._load_bundle_concurrent, count, parsed_bundle)
                       for count, parsed_bundle in enumerate(self.bundles_parsed)]
            concurrent.futures.wait(futures)

    def _load_parsed_bundles(self):
        """Loads already parsed bundles"""
        for count, parsed_bundle in enumerate(self.bundles_parsed):
            logger.info(f'Attempting to load bundle {count}')
            try:
                self._load_bundle(*parsed_bundle, count)
            except Exception:
                logger.exception(f'Error loading bundle {parsed_bundle.bundle_uuid}')
                logger.debug(f'Bundle details: \n{parsed_bundle.pprint()}')
                self.bundles_failed_parsed.append(parsed_bundle)
                continue
            self.bundles_loaded.append(parsed_bundle)
            logger.info(f'Successfully loaded bundle {parsed_bundle.bundle_uuid}')

    def load_all_bundles(self, input_json: typing.List[dict], concurrently: bool=False) -> bool:
        success = True
        logger.info(f'Going to load {len(input_json)} bundle{"" if len(input_json) == 1 else "s"}')
        try:
            self._parse_all_bundles(input_json)
            if concurrently:
                self._load_parsed_bundles_concurrent()
            else:
                self._load_parsed_bundles()
        except KeyboardInterrupt:
            # The bundle that was being processed during the interrupt isn't recorded anywhere
            logger.exception('Loading canceled with keyboard interrupt')
        finally:
            bundles_unattempted = len(input_json) \
                - len(self.bundles_failed_unparsed) \
                - len(self.bundles_failed_parsed) \
                - len(self.bundles_loaded)
            if bundles_unattempted:
                logger.warning(f'Did not yet attempt to load {bundles_unattempted} bundles')
                success = False
            if len(self.bundles_failed_unparsed) > 0:
                logger.error(f'Could not parse {len(self.bundles_failed_unparsed)} bundles')
                success = False
            if len(self.bundles_failed_parsed) > 0:
                logger.error(f'Could not load {len(self.bundles_failed_parsed)} bundles')
                success = False
                # TODO: ADD COMMAND LINE OPTION TO SAVE ERROR LOG TO FILE https://stackoverflow.com/a/11233293/7830612
            if success:
                logger.info(f'Successfully loaded all {len(self.bundles_loaded)} bundles!')
            else:
                logger.info(f'Successfully loaded {len(self.bundles_loaded)} bundles')
            return success
