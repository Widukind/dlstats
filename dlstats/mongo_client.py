from pymongo import MongoClient
from . import configuration
mongo_client = MongoClient(**configuration['MongoDB'])
