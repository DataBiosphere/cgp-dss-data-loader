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


class ParsedBundle(namedtuple('ParsedBundle', ['bundle_uuid', 'metadata_dict', 'data_objects'])):

    def pprint(self):
        return pprint.pformat(self, indent=4)


class StandardFormatBundleUploader:
    _uuid_regex = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')

    def __init__(self, dss_uploader: DssUploader, metadata_file_uploader: MetadataFileUploader) -> None:
        self.dss_uploader = dss_uploader
        self.metadata_file_uploader = metadata_file_uploader

    @staticmethod
    def _parse_bundle(bundle: dict) -> ParsedBundle:
        try:
            data_bundle = bundle['data_bundle']
            bundle_uuid = data_bundle['id']
            metadata_dict = data_bundle['user_metadata']
            data_objects = bundle['data_objects']
        except KeyError:
            # TODO: Make a bundle parse error that spits back whatever info we can get about the bundle
            raise
        return ParsedBundle(bundle_uuid, metadata_dict, data_objects)

    @classmethod
    def _get_file_uuid(cls, file_guid: str):
        result = cls._uuid_regex.findall(file_guid.lower())
        if result is None:
            raise ValueError(f'Misformatted file_guid: {file_guid} should contain a uuid.')
        if len(result) != 1:
            raise ValueError(f'Misformatted file_guid: {file_guid} contains multiple uuids. Only one was expected.')
        return result[0]

    @staticmethod
    def _get_file_version(file_info: dict):
        """Since date updated is optional, we default to date created when it's not updated"""
        try:
            return file_info['updated']
        except KeyError:
            return file_info['created']

    @staticmethod
    def _get_cloud_urls(file_info: dict):
        return {url_dict['url'] for url_dict in file_info['urls']}

    def _load_bundle(self, bundle_uuid, metadata_dict, data_objects):
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

        # load data files by reference
        for file_guid in data_objects:
            file_info = data_objects[file_guid]
            filename = file_info['name']
            file_uuid = self._get_file_uuid(file_guid)
            file_version = self._get_file_version(file_info)
            cloud_urls = self._get_cloud_urls(file_info)
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

    def load_all_bundles(self, input_json: typing.List[dict]):
        logger.info(f'Going to load {len(input_json)} bundle{"" if len(input_json) == 1 else "s"}')
        bundles_loaded: typing.List[dict] = []
        bundles_failed_unparsed: typing.List[dict] = []
        bundles_failed_parsed: typing.List[ParsedBundle] = []
        try:
            for count, bundle in enumerate(input_json):
                logger.info(f'Attempting to load bundle {count + 1}')
                try:
                    parsed_bundle = self._parse_bundle(bundle)
                except Exception:
                    logger.exception(f'Could not parse bundle {count + 1}')
                    logger.debug(f'Bundle details: \n{pprint.pformat(bundle)}')
                    bundles_failed_unparsed.append(bundle)
                    continue
                try:
                    self._load_bundle(*parsed_bundle)
                except Exception:
                    logger.exception(f'Error loading bundle {parsed_bundle.bundle_uuid}')
                    logger.debug(f'Bundle details: \n{parsed_bundle.pprint()}')
                    bundles_failed_parsed.append(parsed_bundle)
                    continue
                bundles_loaded.append(bundle)
                logger.info(f'Successfully loaded bundle {parsed_bundle.bundle_uuid}')
        except KeyboardInterrupt:
            # The bundle that was being processed durng the iterrupt isn't recorded anywhere
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
