import logging

from loader.base_loader import DssUploader, MetadataFileUploader

logger = logging.getLogger(__name__)

SCHEMA_URL = ('https://raw.githubusercontent.com/DataBiosphere/metadata-schema/master/'
              'json_schema/cgp/gen3/2.0.0/cgp_gen3_metadata.json')


class StandardFormatBundleUploader:
    def __init__(self, dss_uploader: DssUploader, metadata_file_uploader: MetadataFileUploader) -> None:
        self.dss_uploader = dss_uploader
        self.metadata_file_uploader = metadata_file_uploader

    @staticmethod
    def _parse_bundle(bundle: dict) -> tuple:
        data_bundle = bundle['data_bundle']
        bundle_uuid = data_bundle['id']
        metadata_dict = data_bundle['user_metadata']
        data_objects = bundle['data_objects']
        return bundle_uuid, metadata_dict, data_objects

    @staticmethod
    def _get_cloud_urls(file_info: dict):
        return {url_dict['url'] for url_dict in file_info['urls']}

    @staticmethod
    def _get_file_ids(file_guid: str):
        return file_guid.split('/')[1]

    def _load_bundle(self, bundle: dict):
        bundle_uuid, metadata_dict, data_objects = self._parse_bundle(bundle)
        logger.info(f'Loading bundle with uuid {bundle_uuid}')
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
            file_uuid = self._get_file_ids(file_guid)
            file_version = file_info['updated']
            cloud_urls = self._get_cloud_urls(file_info)
            logger.debug(f'Uploading data file: {filename} with uuid:version {file_uuid}:{file_version}...')
            file_uuid, file_version, filename = \
                self.dss_uploader.upload_cloud_file_by_reference(filename,
                                                                 # use did for uuid for now. will probably have to
                                                                 # extract from guid (did) in the future
                                                                 file_uuid,
                                                                 cloud_urls,
                                                                 bundle_uuid,
                                                                 file_guid,
                                                                 file_version=file_version)
            logger.debug(f'...Uploaded data file: {filename} with uuid:version {file_uuid}:{file_version}')
            file_info_list.append(dict(uuid=file_uuid, version=file_version, name=filename, indexed=False))

        # load bundle
        self.dss_uploader.load_bundle(file_info_list, bundle_uuid)

    def load_all_bundles(self, input_json: list):
        logger.info(f'Going to load {len(input_json)} bundles')
        for bundle in input_json:
            self._load_bundle(bundle)
