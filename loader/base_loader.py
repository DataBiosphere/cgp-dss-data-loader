"""
Classes to load data files and bundles into the HCA Data Storage System (DSS).
The data files may be located in AWS and/or GCP buckets, which may require
authentication and may be requester pays buckets.

These classes support a means of "loading files by reference" even though the current
HCA DSS does not. For more information see:
"Commons HCA DSS Data Loading by Reference"
https://docs.google.com/document/d/1QSa7Ubw-muyD_u0X_dq9WeKyK_dCJXi4Ex7S_pil1uk/edit#heading=h.exnqjy2n2q78

Note: The TOPMed Google controlled access buckets are based on ACLs for user accounts
Before running this loader, configure use of Google user account, run: gcloud auth login
"""
import base64
import binascii
import json
import logging
import mimetypes
import os
import time
import uuid
from io import open
from tempfile import mkdtemp
from typing import Any, Dict
from urllib.parse import urlparse
from warnings import warn

import boto3
import botocore
import requests
from boto3.s3.transfer import TransferConfig
from google.oauth2.credentials import Credentials
from cloud_blobstore import s3
from dcplib import s3_multipart
from dcplib.checksumming_io import ChecksummingBufferedReader
from google.cloud.storage import Client
from hca import HCAConfig
from hca.dss import DSSClient
from hca.util import SwaggerAPIException

from util import tz_utc_now, monkey_patch_hca_config

logger = logging.getLogger(__name__)

CREATOR_ID = 20


class CloudUrlAccessWarning(Warning):
    """Warning when a cloud URL could not be accessed for any reason"""


class CloudUrlAccessForbidden(CloudUrlAccessWarning):
    """Warning when a cloud URL could not be accessed due to authorization issues"""


class CloudUrlNotFound(CloudUrlAccessWarning):
    """Warning when a cloud URL was not found"""


class FileURLError(Exception):
    """Thrown when a file cannot be accessed by the given URl"""


class InconsistentFileSizeValues(Exception):
    """Thrown when the input file size does not match the actual file size of a file being loaded by reference"""


class MissingInputFileSize(Exception):
    """Thrown when the input file size is not available for a data file being loaded by reference"""


class UnexpectedResponseError(Exception):
    """Thrown when DSS gives an unexpected response"""


class DssUploader:
    def __init__(self, dss_endpoint: str, staging_bucket: str, google_project_id: str, dry_run: bool,
                 aws_meta_cred: str = None, gcp_meta_cred: str = None) -> None:
        """
        Functions for uploading files to a given DSS.

        :param dss_endpoint: The URL to a Swagger DSS API.  e.g. "https://commons-dss.ucsc-cgp-dev.org/v1"
        :param staging_bucket: The name of the AWS S3 bucket to be used when staging files for uploading
        to the DSS. As an example, local files are uploaded to the staging bucket, then file metadata tags
        required by the DSS are assigned to it, then the file is loaded into the DSS (by copy).
        The bucket must be accessible by the DSS. .e.g. 'commons-dss-upload'
        :param google_project_id: A Google `Project ID` to be used when accessing GCP requester pays buckets.
                                  e.g. "platform-dev-178517"
                                  One way to find a `Project ID` is provided here:
                                  https://console.cloud.google.com/cloud-resource-manager
        :param dry_run: If True, log the actions that would be performed yet don't actually execute them.
                        Otherwise, actually perform the operations.
        :param aws_meta_cred: Optional credentials used to fetch metadata from a private bucket.
        :param gcp_meta_cred: Optional credentials used to fetch metadata from a private bucket.
        """
        os.environ['GOOGLE_CLOUD_PROJECT'] = google_project_id
        self.dss_endpoint = dss_endpoint
        self.staging_bucket = staging_bucket
        self.google_project_id = google_project_id
        self.dry_run = dry_run
        self.s3_client = boto3.client("s3")
        self.s3_blobstore = s3.S3BlobStore(self.s3_client)
        self.gs_client = Client(project=self.google_project_id)

        # optional clients for fetching protected metadata that the
        # main credentials may not have access to
        self.aws_meta_cred = aws_meta_cred
        self.gcp_meta_cred = gcp_meta_cred
        self.s3_metadata_client = self.get_s3_metadata_client(self.aws_meta_cred)
        self.gs_metadata_client = self.get_gs_metadata_client(self.gcp_meta_cred)

        # Work around problems with DSSClient initialization when there is
        # existing HCA configuration. The following issue has been submitted:
        # Problems accessing an alternate DSS from user scripts or unit tests #170
        # https://github.com/HumanCellAtlas/dcp-cli/issues/170
        monkey_patch_hca_config()
        HCAConfig._user_config_home = '/tmp/'
        dss_config = HCAConfig(name='loader', save_on_exit=False, autosave=False)
        dss_config['DSSClient'].swagger_url = f'{self.dss_endpoint}/swagger.json'
        self.dss_client = DSSClient(config=dss_config)

    @staticmethod
    def get_s3_metadata_client(aws_meta_cred, session='NIH-Test', duration=43199):
        """
        Access AWS credentials from a file and supply a client for them.

        :param aws_meta_cred: File containing an AWS ARN for an AssumedRole, e.g.:
                              'arn:aws:iam::************:role/ROLE_NAME_HERE'
        :param duration: How long, in seconds, the AssumedRole will be valid for.
        :return: An AWS s3 client object authorized with the above credentials or None.
        """
        if not aws_meta_cred:
            return None

        sts_client = boto3.client('sts')
        with open(aws_meta_cred, 'r') as f:
            role_arn = f.read().strip()

        # DurationSeconds can have a value from 900s to 43200s (as of 10.23.2018).
        # 900s = 15 min; 43200s = 12 hours
        # https://docs.aws.amazon.com/cli/latest/reference/sts/assume-role.html
        assumed_role = sts_client.assume_role(RoleArn=role_arn, RoleSessionName=session, DurationSeconds=duration)

        credentials = assumed_role['Credentials']
        return boto3.client('s3',
                            aws_access_key_id=credentials['AccessKeyId'],
                            aws_secret_access_key=credentials['SecretAccessKey'],
                            aws_session_token=credentials['SessionToken'])

    def get_gs_metadata_client(self, gcp_meta_cred):
        """
        Access Google credentials from a file and supply a client for them.

        :param gcp_meta_cred: File containing user credentials, usually generated via:
                                gcloud auth application-default login
                              Default location:
                                '/home/<user>/.config/gcloud/application_default_credentials.json'
        :return: A google storage client object authorized with the above credentials or None.
        """
        if not gcp_meta_cred:
            return None

        credentials = Credentials(token=None).from_authorized_user_file(gcp_meta_cred)
        return Client(project=self.google_project_id, credentials=credentials)

    def handle_s3_client_error(self, err_code: str, bucket: str, key: str, attempt_refresh=True):
        """
        Will log warnings and consume the exception it was passed.  If the credentials get an
        unauthorized/forbidden error, it will attempt to refresh metadata credentials (if they exist)
        and try (only) once more.

        :param bucket: Name of an S3 bucket
        :param key: S3 file to upload.  err_code.g. 'output.txt' or 'data/output.txt'
        :param attempt_refresh: Ensures attempting to refresh the metadata credentials happens only once per file.
        :return: Returns a head response containing a dictionary of metadata values, or an empty dict in the case of an error.
        """
        if err_code == str(requests.codes.not_found):
            warn(f'Could not find \"s3://{bucket}/{key}\" Error: {err_code}'
                 ' The S3 file metadata for this file reference will be missing.',
                 CloudUrlNotFound)
        # refresh the metadata credentials if blocked and if they exist
        elif (err_code in (str(requests.codes.forbidden), str(requests.codes.unauthorized))) and self.aws_meta_cred and attempt_refresh:
            self.s3_metadata_client = self.get_s3_metadata_client(self.aws_meta_cred)
            return self.get_s3_file_head_response(bucket, key, attempt_refresh=False)
        else:
            warn(f'Could not find \"s3://{bucket}/{key}\" Error: {err_code}'
                 ' The S3 file metadata for this file reference will be missing.',
                 CloudUrlAccessWarning)
        return dict()

    def get_s3_file_head_response(self, bucket: str, key: str, attempt_refresh=True) -> dict:
        """
        Attempt to fetch a head response from an s3 file containing metadata about that file.

        :param bucket: Name of an S3 bucket
        :param key: S3 file to upload.  e.g. 'output.txt' or 'data/output.txt'
        :param attempt_refresh: Ensures attempting to refresh the metadata credentials happens only once per file.
        :return: Returns a head response containing a dictionary of metadata values, or an empty dict in the case of an error.
        """
        client = self.s3_metadata_client if self.s3_metadata_client else self.s3_client
        try:
            return client.head_object(Bucket=bucket, Key=key, RequestPayer="requester")
        except botocore.exceptions.ClientError as e:
            return self.handle_s3_client_error(e.response['Error']['Code'], bucket, key, attempt_refresh)

    def get_s3_file_metadata(self, bucket: str, key: str) -> dict:
        """
        Format an S3 file's metadata into a dictionary for uploading as a json.

        :param bucket: Name of an S3 bucket
        :param key: S3 file to upload.  e.g. 'output.txt' or 'data/output.txt'
        :return: Returns a dictionary of metadata values (or an empty dictionary in the case of an error).
        """
        response = self.get_s3_file_head_response(bucket, key)
        metadata = dict()
        try:
            metadata['size'] = response['ContentLength']
            metadata['content-type'] = response['ContentType']
            metadata['s3_etag'] = response['ETag']
        except KeyError as e:
            # These standard metadata should always be present.
            logging.error(f'Could not find "s3://{bucket}/{key}" file metadata field. Error: {e}.\n'
                          f'The S3 file metadata for this file is inaccessible with your current credentials.  '
                          f'Please supply additional metadata credentials using the --aws-metadata-cred option.')
        return metadata

    def get_gs_file_metadata(self, bucket: str, key: str) -> dict:
        """
        Format a GS file's metadata into a dictionary for uploading as a JSON file.

        :param bucket: Name of a GS bucket.
        :param key: GS file to upload.  e.g. 'output.txt' or 'data/output.txt'
        :return: A dictionary of metadata values.
        """
        metadata = dict()
        client = self.gs_metadata_client if self.gs_metadata_client else self.gs_client
        gs_bucket = client.bucket(bucket, self.google_project_id)
        blob_obj = gs_bucket.get_blob(key)
        if blob_obj is not None:
            metadata['size'] = blob_obj.size
            metadata['content-type'] = blob_obj.content_type
            metadata['crc32c'] = binascii.hexlify(base64.b64decode(blob_obj.crc32c)).decode("utf-8").lower()
            return metadata
        else:
            # These standard metadata should always be present.
            warn(f'Could not find "gs://{bucket}/{key}".  The S3 file metadata for this file is inaccessible '
                 f'with your current credentials.  Please supply metadata credentials using the '
                 f'--gcp-metadata-cred option.',
                 CloudUrlNotFound)
            return metadata

    def upload_cloud_file_by_reference(self,
                                       filename: str,
                                       file_uuid: str,
                                       file_cloud_urls: set,
                                       size: int,
                                       guid: str,
                                       file_version: str = None) -> tuple:
        """
        Loads the given cloud file into the DSS by reference, rather than by copying it into the DSS.
        Because the HCA DSS per se does not support loading by reference, this is currently implemented
        using the approach described here:
        https://docs.google.com/document/d/1QSa7Ubw-muyD_u0X_dq9WeKyK_dCJXi4Ex7S_pil1uk/edit#heading=h.exnqjy2n2q78

        This is conceptually similar to creating a "symbolic link" to the cloud file rather than copying the
        source file into the DSS.
        The file's metadata is obtained, formatted as a dictionary, then this dictionary is uploaded as
        as a json file with content type `dss-type=fileref` into the DSS.

        A request has been made for the HCA data-store to support loading by reference as a feature of the
        data store, here: https://github.com/HumanCellAtlas/data-store/issues/912

        :param filename: The name of the file in the bucket.
        :param file_uuid: An RFC4122-compliant UUID to be used to identify the file
        :param file_cloud_urls: A set of 'gs://' and 's3://' bucket links.
                                e.g. {'gs://broad-public-datasets/g.bam', 's3://ucsc-topmed-datasets/a.bam'}
        :param size: size of the file in bytes, as provided by the input data to be loaded.
         An attempt will be made to access the `file_cloud_objects` to obtain the
         basic file metadata, and if successful, the size is verified to be consistent.
        :param guid: An optional additional/alternate data identifier/alias to associate with the file
        e.g. "dg.4503/887388d7-a974-4259-86af-f5305172363d"
        :param file_version: a RFC3339 compliant datetime string
        :return: file_uuid: str, file_version: str, filename: str, already_present: bool
        :raises MissingFileSize: If no input file size is available for file to be loaded by reference
        :raises InconsistentFileSizeValues: If file sizes are inconsistent for file to be loaded by reference
        """
        def _create_file_reference(file_cloud_urls: set, size: int, guid: str) -> dict:
            """
            Format a file's metadata into a dictionary for uploading as a json to support the approach
            described here:
            https://docs.google.com/document/d/1QSa7Ubw-muyD_u0X_dq9WeKyK_dCJXi4Ex7S_pil1uk/edit#heading=h.exnqjy2n2q78

            :param file_cloud_urls: A set of 'gs://' and 's3://' bucket links.
                                    e.g. {'gs://broad-public-datasets/g.bam', 's3://ucsc-topmed-datasets/a.bam'}
            :param guid: An optional additional/alternate data identifier/alias to associate with the file
            e.g. "dg.4503/887388d7-a974-4259-86af-f5305172363d"
            :param size: file size in bytes from input data
            :return: A dictionary of metadata values.
            """
            input_metadata = dict(size=size)
            s3_metadata: Dict[str, Any] = dict()
            gs_metadata: Dict[str, Any] = dict()
            for cloud_url in file_cloud_urls:
                url = urlparse(cloud_url)
                bucket = url.netloc
                key = url.path[1:]
                if not (bucket and key):
                    raise FileURLError(f'Invalid URL {cloud_url}')
                if url.scheme == "s3":
                    s3_metadata = self.get_s3_file_metadata(bucket, key)
                elif url.scheme == "gs":
                    gs_metadata = self.get_gs_file_metadata(bucket, key)
                else:
                    raise FileURLError("Unsupported cloud URL scheme: {cloud_url}")
            return _consolidate_metadata(file_cloud_urls, input_metadata, s3_metadata, gs_metadata, guid)

        def _consolidate_metadata(file_cloud_urls: set,
                                  input_metadata: Dict[str, Any],
                                  s3_metadata: Dict[str, Any],
                                  gs_metadata: Dict[str, Any],
                                  guid: str) -> dict:
            """
            Consolidates cloud file metadata to create the JSON used to load by reference
            into the DSS.

            :param input_metadata: An initial dictionary containing metadata about the file
                                   provided by the input file to the loader.
            :param file_cloud_urls: A set of 'gs://' and 's3://' bucket URLs.
                                    e.g. {'gs://broad-public-datasets/g.bam', 's3://ucsc-topmed-datasets/a.bam'}
            :param s3_metadata: Dictionary of meta data produced by self.get_s3_file_metadata().
            :param gs_metadata: Dictionary of meta data produced by self.get_gs_file_metadata().
            :param guid: An optional additional/alternate data identifier/alias to associate with the file
                         e.g. "dg.4503/887388d7-a974-4259-86af-f5305172363d"
            :return: A dictionary of cloud file metadata values
            """

            def _check_file_size_consistency(input_metadata, s3_metadata, gs_metadata):
                input_size = input_metadata.get('size', None)
                if input_size is not None:
                    input_size = int(input_size)
                else:
                    raise MissingInputFileSize('No input file size is available for file being loaded by reference.')
                s3_size = s3_metadata.get('size', None)
                gs_size = gs_metadata.get('size', None)
                if s3_size and input_size != s3_size:
                    raise InconsistentFileSizeValues(
                        f'Input file size does not match actual S3 file size: '
                        f'input size: {input_size}, S3 actual size: {s3_size}')
                if gs_size and input_size != gs_size:
                    raise InconsistentFileSizeValues(
                        f'Input file size does not match actual GS actual file size: '
                        f'input size: {input_size}, GS actual size: {gs_size}')
                return input_size

            consolidated_metadata: Dict[str, Any] = dict()
            consolidated_metadata.update(input_metadata)
            consolidated_metadata.update(s3_metadata)
            consolidated_metadata.update(gs_metadata)
            consolidated_metadata['size'] = _check_file_size_consistency(input_metadata, s3_metadata, gs_metadata)
            consolidated_metadata['url'] = list(file_cloud_urls)
            consolidated_metadata['aliases'] = [str(guid)]
            return consolidated_metadata

        if self.dry_run:
            logger.info(f'DRY RUN: upload_cloud_file_by_reference: '
                        f'{filename} {file_uuid} {str(file_cloud_urls)} {size} {guid}')

        file_reference = _create_file_reference(file_cloud_urls, size, guid)
        return self.upload_dict_as_file(file_reference,
                                        filename,
                                        file_uuid,
                                        file_version=file_version,
                                        content_type="application/json; dss-type=fileref")

    def upload_dict_as_file(self, value: dict,
                            filename: str,
                            file_uuid: str,
                            file_version: str = None,  # RFC3339
                            content_type: str = None):
        """
        Create a JSON file in the DSS containing the given dict.

        :param value: A dictionary representing the JSON content of the file to be created.
        :param filename: The basename of the file in the bucket.
        :param file_uuid: An RFC4122-compliant UUID to be used to identify the file
        :param content_type: Content description e.g. "application/json; dss-type=fileref".
        :param file_version: a RFC3339 compliant datetime string
        :return: file_uuid: str, file_version: str, filename: str, already_present: bool
        """
        tempdir = mkdtemp()
        file_path = "/".join([tempdir, filename])
        with open(file_path, "w") as fh:
            fh.write(json.dumps(value, indent=4))
        result = self.upload_local_file(file_path,
                                        file_uuid,
                                        file_version=file_version,
                                        content_type=content_type)
        os.remove(file_path)
        os.rmdir(tempdir)
        return result

    def upload_local_file(self, path: str,
                          file_uuid: str,
                          file_version: str = None,
                          content_type: str = None):
        """
        Upload a file from the local file system to the DSS.

        :param path: Path to a local file.
        :param file_uuid: An RFC4122-compliant UUID to be used to identify the file
        :param content_type: Content type identifier, for example: "application/json; dss-type=fileref".
        :param file_version: a RFC3339 compliant datetime string
        :return: file_uuid: str, file_version: str, filename: str, already_present: bool
        """
        file_uuid, key = self._upload_local_file_to_staging(path, file_uuid, content_type)
        return self._upload_tagged_cloud_file_to_dss_by_copy(self.staging_bucket,
                                                             key,
                                                             file_uuid,
                                                             file_version=file_version)

    def load_bundle(self, file_info_list: list, bundle_uuid: str):
        """
        Loads a bundle to the DSS that contains the specified files.

        :param file_info_list:
        :param bundle_uuid: An RFC4122-compliant UUID to be used to identify the bundle containing the file
        :return: A full qualified bundle id e.g. "{bundle_uuid}.{version}"
        """
        kwargs = dict(replica="aws",
                      creator_uid=CREATOR_ID,
                      files=file_info_list,
                      uuid=bundle_uuid,
                      version=tz_utc_now())

        if self.dry_run:
            logger.info("DRY RUN: DSS put bundle: " + str(kwargs))
            return f"{bundle_uuid}.{kwargs['version']}"

        response = self.dss_client.put_bundle(**kwargs)
        version = response['version']
        bundle_fqid = f"{bundle_uuid}.{version}"
        logger.info(f"Loaded bundle: {bundle_fqid}")
        return bundle_fqid

    @staticmethod
    def get_filename_from_key(key: str):
        assert not key.endswith('/'), 'Please specify a filename, not a directory ({} cannot end in "/").'.format(key)
        return key.split("/")[-1]

    def _upload_local_file_to_staging(self, path: str, file_uuid: str, content_type):
        """
        Upload a local file to the staging bucket, computing the DSS-required checksums
        in the process, then tag the file in the staging bucket with the checksums.
        This is in preparation from subsequently uploading the file from the staging
        bucket into the DSS.

        :param path: Path to a local file.
        :param file_uuid: An RFC4122-compliant UUID to be used to identify the file.
        :param content_type: Content description, for example: "application/json; dss-type=fileref".
        :return: file_uuid: str, key_name: str
        """

        def _encode_tags(tags):
            return [dict(Key=k, Value=v) for k, v in tags.items()]

        def _mime_type(filename):
            type_, encoding = mimetypes.guess_type(filename)
            if encoding:
                return encoding
            if type_:
                return type_
            return "application/octet-stream"

        file_size = os.path.getsize(path)
        multipart_chunksize = s3_multipart.get_s3_multipart_chunk_size(file_size)
        tx_cfg = TransferConfig(multipart_threshold=s3_multipart.MULTIPART_THRESHOLD,
                                multipart_chunksize=multipart_chunksize)
        s3 = boto3.resource("s3")

        destination_bucket = s3.Bucket(self.staging_bucket)
        with open(path, "rb") as file_handle, ChecksummingBufferedReader(file_handle, multipart_chunksize) as fh:
            key_name = "{}/{}".format(file_uuid, os.path.basename(fh.raw.name))
            destination_bucket.upload_fileobj(
                fh,
                key_name,
                Config=tx_cfg,
                ExtraArgs={
                    'ContentType': content_type if content_type is not None else _mime_type(fh.raw.name)
                }
            )
            sums = fh.get_checksums()
            metadata = {
                "hca-dss-s3_etag": sums["s3_etag"],
                "hca-dss-sha1": sums["sha1"],
                "hca-dss-sha256": sums["sha256"],
                "hca-dss-crc32c": sums["crc32c"],
            }

            s3.meta.client.put_object_tagging(Bucket=destination_bucket.name,
                                              Key=key_name,
                                              Tagging=dict(TagSet=_encode_tags(metadata))
                                              )
        return file_uuid, key_name

    def _upload_tagged_cloud_file_to_dss_by_copy(self, source_bucket: str,
                                                 source_key: str,
                                                 file_uuid: str,
                                                 file_version: str = None,
                                                 timeout_seconds: int = 1200):
        """
        Uploads a tagged file contained in a cloud bucket to the DSS by copy.
        This is typically used to update a tagged file from a staging bucket into the DSS.

        :param source_bucket: Name of an S3 bucket.  e.g. 'commons-dss-upload'
        :param source_key: S3 file to upload.  e.g. 'output.txt' or 'data/output.txt'
        :param file_uuid: An RFC4122-compliant UUID to be used to identify the file.
        :param file_version: a RFC3339 compliant datetime string
        :param timeout_seconds:  Amount of time to continue attempting an async copy.
        :return: file_uuid: str, file_version: str, filename: str, file_present: bool
        """
        source_url = f"s3://{source_bucket}/{source_key}"
        filename = self.get_filename_from_key(source_key)

        request_parameters = dict(uuid=file_uuid, version=file_version, creator_uid=CREATOR_ID,
                                  source_url=source_url)
        if self.dry_run:
            logger.info("DRY RUN: put file: " + str(request_parameters))
            return file_uuid, file_version, filename, False

        copy_start_time = time.time()
        response = self.dss_client.put_file._request(request_parameters)

        # the version we get back here is formatted in the way DSS likes
        # and we need this format update when doing load bundle
        file_version = response.json().get('version', "blank")

        # from dss swagger docs:
        # 200 Returned when the file is already present and is identical to the file being uploaded.
        already_present = response.status_code == requests.codes.ok
        if response.status_code == requests.codes.ok:
            logger.info("File %s: Already exists -> %s (%d seconds)",
                        source_url, file_version, (time.time() - copy_start_time))
        elif response.status_code == requests.codes.created:
            logger.info("File %s: Sync copy -> %s (%d seconds)",
                        source_url, file_version, (time.time() - copy_start_time))
        elif response.status_code == requests.codes.accepted:
            logger.info("File %s: Starting async copy -> %s", source_url, file_version)

            timeout = time.time() + timeout_seconds
            wait = 1.0
            # TODO: busy wait could hopefully be replaced with asyncio
            while time.time() < timeout:
                try:
                    self.dss_client.head_file(uuid=file_uuid, replica="aws", version=file_version)
                    logger.info("File %s: Finished async copy -> %s (approximately %d seconds)",
                                source_url, file_version, (time.time() - copy_start_time))
                    break
                except SwaggerAPIException as e:
                    if e.code != requests.codes.not_found:
                        msg = "File {}: Unexpected server response during registration"
                        raise RuntimeError(msg.format(source_url))
                    time.sleep(wait)
                    wait = min(10.0, wait * self.dss_client.UPLOAD_BACKOFF_FACTOR)
            else:
                # timed out. :(
                raise RuntimeError("File {}: registration FAILED".format(source_url))
            logger.debug("Successfully uploaded file")
        else:
            raise UnexpectedResponseError(f'Received unexpected response code {response.status_code}')

        return file_uuid, file_version, filename, already_present


class MetadataFileUploader:
    def __init__(self, dss_uploader: DssUploader) -> None:
        self.dss_uploader = dss_uploader

    def load_cloud_file(self, bucket: str, key: str, filename: str, schema_url: str) -> tuple:
        metadata_string = self.dss_uploader.s3_blobstore.get(bucket, key).decode("utf-8")
        metadata = json.loads(metadata_string)
        return self.load_dict(metadata, filename, schema_url)

    def load_local_file(self, local_filename: str, filename: str, schema_url: str) -> tuple:
        with open(local_filename, "r") as fh:
            metadata = json.load(fh)
        return self.load_dict(metadata, filename, schema_url)

    def load_dict(self, metadata: dict, filename: str, schema_url: str, file_version=None) -> tuple:
        metadata['describedBy'] = schema_url
        # metadata files don't have file_uuids which is why we have to make it up on the spot
        return self.dss_uploader.upload_dict_as_file(metadata, filename, str(uuid.uuid4()), file_version=file_version)
