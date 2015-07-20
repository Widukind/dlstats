import ming
import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from . import configuration
from dlstats.logger import logger

if os.environ['DLSTATS_TEST_ENVIRONMENT'] == 'False':
    mongo_client = MongoClient(**configuration['MongoDB'])
else:
    logger.info('Creating a database in RAM.')
    datastore = ming.create_datastore('mim://')
    mongo_client = datastore.conn
