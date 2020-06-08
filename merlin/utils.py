import time
from functools import wraps

from merlin.logger import get_logger

LOGGER = get_logger(level="INFO", name=__name__)


def timed(func):
    """This decorator prints the execution time for the decorated function."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        LOGGER.info("{} ran in {}s".format(func.__name__, round(end - start, 2)))
        return result

    return wrapper


def get_schema_for_env(env) -> str:
    """
    Get the default schema for the enviroment
    :param env Enviroment
    """
    if env == "prd":
        return "grab_x"

    return "grab_x_{}".format(env)
