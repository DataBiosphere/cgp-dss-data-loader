#!/usr/bin/env python

"""
Script to load files and bundles into the HCA DSS.
"""

import logging
import os
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from loader import base_loader
from loader.standard_loader import StandardFormatBundleUploader
from util import load_json_from_file, suppress_verbose_logging

# Google Cloud Access
# TODO Make GOOGLE_PROJECT_ID configurable via a command-line option
GOOGLE_PROJECT_ID = "platform-dev-178517"  # For requester pays buckets


def main(argv=sys.argv[1:]):
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    dry_run_group = parser.add_mutually_exclusive_group(required=True)
    dry_run_group.add_argument("--dry-run", dest="dry_run", action="store_true",
                               help="Output actions that would otherwise be performed.")
    dry_run_group.add_argument("--no-dry-run", dest="dry_run", action="store_false",
                               help="Perform the actions.")
    parser.add_argument("--dss-endpoint", metavar="DSS_ENDPOINT", required=True,
                        help="HCA Data Storage System endpoint to use")
    parser.add_argument("--staging-bucket", metavar="STAGING_BUCKET", required=True,
                        help="Bucket to stage local files for uploading to DSS")
    parser.add_argument("-l", "--log", dest="log_level",
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default="INFO", help="Set the logging level")
    parser.add_argument('--serial', action='store_true', default=False,
                        help='Upload bundles serially. This can be useful for debugging')
    parser.add_argument('input_json', metavar='INPUT_JSON',
                        help="Path to the standard JSON format input file")
    parser.add_argument('--aws_metadata_cred', required=False, default=None,
                        help="The loader by default needs no additional credentials to "
                             "access public references, but when attempting to access "
                             "private cloud files in order to determine size and hash "
                             "metadata it may be blocked.  This field supplies a "
                             "path to a file containing additional credentials "
                             "needed to access the referenced files directly.")
    parser.add_argument('--gce_metadata_cred', required=False, default=None,
                        help="The loader by default needs no additional credentials to "
                             "access public references, but when attempting to access "
                             "private cloud files in order to determine size and hash "
                             "metadata it may be blocked.  This field supplies a "
                             "path to a file containing additional credentials "
                             "needed to access the referenced files directly.")

    options = parser.parse_args(argv)

    # The ACLs on the TOPMed Google buckets are based on user accounts.
    # Clear configured Google credentials, which are likely for service accounts.
    # os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    # os.environ.pop('GOOGLE_APPLICATION_SECRETS', None)

    logging.basicConfig(level=logging.getLevelName(options.log_level),
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logging.getLogger(__name__)
    suppress_verbose_logging()

    if bool(options.aws_metadata_cred) != bool(options.gce_metadata_cred):
        logging.warning(f'Additional credentials are only specified for one cloud '
                        '(if both are not supplied, things may not go as planned): '
                        '\n{options.aws_metadata_cred}'
                        '\n{options.gce_metadata_cred}\n')

    dss_uploader = base_loader.DssUploader(options.dss_endpoint, options.staging_bucket,
                                           GOOGLE_PROJECT_ID, options.dry_run,
                                           options.aws_metadata_cred, options.gce_metadata_cred)
    metadata_file_uploader = base_loader.MetadataFileUploader(dss_uploader)

    if not sys.warnoptions:
        import warnings
        # Log each unique cloud URL access warning once by default.
        # This can be overridden using the "PYTHONWARNINGS" environment variable.
        # See: https://docs.python.org/3/library/warnings.html
        warnings.simplefilter('default', 'CloudUrlAccessWarning', append=True)

    bundle_uploader = StandardFormatBundleUploader(dss_uploader, metadata_file_uploader)
    logging.info(f'Uploading {"serially" if options.serial else "concurrently"}')
    return bundle_uploader.load_all_bundles(load_json_from_file(options.input_json), not options.serial)


if __name__ == '__main__':
    success = main()
    if not success:
        exit(1)
