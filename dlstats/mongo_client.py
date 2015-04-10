import ming
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from . import configuration
from dlstats import lgr
try:
    mongo_client = MongoClient(**configuration['MongoDB'])
except ConnectionFailure: # Be careful. MongoClient don't throw that exception in the latest versions of pymongo
    lgr.errors('Could not connect to MongoDB. Creating a database in RAM.')
    datastore = ming.create_datastore('mim://')
    mongo_client = datastore.conn
