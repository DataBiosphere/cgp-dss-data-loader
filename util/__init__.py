import json
import logging


def load_json_from_file(input_file_path: str):
    with open(input_file_path) as fh:
        return json.load(fh)


def suppress_verbose_logging():
    for logger_name in logging.Logger.manager.loggerDict:  # type: ignore
        if (logger_name.startswith("botocore") or
                logger_name.startswith("boto3.resources")):
            logging.getLogger(logger_name).setLevel(logging.WARNING)