import datetime
import functools
import time
import warnings


def eventually(timeout_seconds: float, retry_interval_seconds: float, errors: set = {AssertionError}):
    """
    @eventually runs a test until all assertions are satisfied or a timeout is reached.
    :param timeout_seconds: time in seconds until the test fails
    :param retry_interval_seconds: time in seconds between attempts of the test
    :param errors: the exceptions to catch and retry on
    :return: the result of the function or a raised assertion error
    """
    def decorate(func):
        @functools.wraps(func)
        def call(*args, **kwargs):
            timeout_time = time.time() + timeout_seconds
            error_tuple = tuple(errors)
            while True:
                try:
                    return func(*args, **kwargs)
                except error_tuple:
                    if time.time() >= timeout_time:
                        raise
                    time.sleep(retry_interval_seconds)

        return call

    return decorate


def message(message: str):
    print("{}: {}".format(datetime.datetime.now(), message))


def ignore_resource_warnings(test_func):
    # see https://stackoverflow.com/q/26563711/7830612 for justification
    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            test_func(self, *args, **kwargs)

    return do_test
