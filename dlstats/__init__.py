from .configuration import configuration
from .elasticsearch_client import elasticsearch_client
from .logger import logger
import os

if os.environ.get('DLSTATS_TEST_ENVIRONMENT') is None:
    os.environ['DLSTATS_TEST_ENVIRONMENT'] = 'False'

from .mongo_client import mongo_client
from . import fetchers, misc_func
