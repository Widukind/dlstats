from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dlstats import configuration
from dlstats.logger import logger
try:
    mongo_client = MongoClient(**configuration['MongoDB'])
except ConnectionFailure: # Be careful. MongoClient don't throw that exception in the latest versions of pymongo
    logger.error('Could not connect to MongoDB. Creating a database in RAM.')
