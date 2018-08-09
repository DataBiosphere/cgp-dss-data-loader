import datetime
import json
import logging
from hca import HCAConfig


def load_json_from_file(input_file_path: str):
    with open(input_file_path) as fh:
        return json.load(fh)


def suppress_verbose_logging():
    for logger_name in logging.Logger.manager.loggerDict:  # type: ignore
        if (logger_name.startswith("botocore") or
                logger_name.startswith("boto3.resources")):
            logging.getLogger(logger_name).setLevel(logging.WARNING)


def patch_connection_pools(**constructor_kwargs):
    """
    Increase pool size used by requests. Useful for when threading

    adapted from https://stackoverflow.com/a/22253656/7830612
    """
    from urllib3 import connectionpool, poolmanager

    class MyHTTPConnectionPool(connectionpool.HTTPConnectionPool):
        def __init__(self, *args, **kwargs):
            kwargs.update(constructor_kwargs)
            super(MyHTTPConnectionPool, self).__init__(*args, **kwargs)
    poolmanager.pool_classes_by_scheme['http'] = MyHTTPConnectionPool

    class MyHTTPSConnectionPool(connectionpool.HTTPSConnectionPool):
        def __init__(self, *args, **kwargs):
            kwargs.update(constructor_kwargs)
            super(MyHTTPSConnectionPool, self).__init__(*args, **kwargs)
    poolmanager.pool_classes_by_scheme['https'] = MyHTTPSConnectionPool


def tz_utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def utc_now():
    return datetime.datetime.utcnow().isoformat()


def monkey_patch_hca_config():
    HCAConfig.__init__ = HCAConfig.__bases__[0].__init__
