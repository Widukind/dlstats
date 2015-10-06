# -*- coding: utf-8 -*-

import os

from dlstats.fetchers.bis import BIS

import unittest
from ..base import RESOURCES_DIR

def get_mongo_db():
    from pymongo import MongoClient
    uri = os.environ.get("MONGODB_URL", "mongodb://localhost/widukind_test")
    #TODO: tz_aware
    client = MongoClient(uri)
    return client.get_default_database()    

def get_es_db():
    from urllib.parse import urlparse
    from elasticsearch import Elasticsearch
    url = urlparse(os.environ.get("ES_URL", "http://localhost:9200"))
    es = Elasticsearch([{"host":url.hostname, "port":url.port}])
    return es

def clean_mongodb():
    db = get_mongo_db()
    #TODO: remove collections
    
def clean_es():
    es = get_es_db()    
    #TODO: remove index

class BisFetcherTestCase(unittest.TestCase):
    """
    1. open and verify mongodb and ES index
        - connection error
        - access right error
    2. download file or load file
        - local: file not found
        - remote: 404 error, connect error
    3. Search update file (filename or attribute file or text in file)    
        3.1. Verify updated data    
    4. parse file
        - parse error
    5. Create/Update mongodb:
        5.1 categories
        5.2 datasets
        5.3 series
    """
    
    def test_local_source(self):
        """Load csv file from local directory"""
        csv_file = os.path.abspath(os.path.join(RESOURCES_DIR, "full_BIS_LBS_DISS_csv.csv"))
        self.assertTrue(os.path.exists(csv_file))
        self.fail("NotImplemented")

    def test_remote_source(self):
        """Load csv file from remote site"""
        self.fail("NotImplemented")

    def test_csv_fields(self):
        """Verify fields in csv file"""
        self.fail("NotImplemented")



