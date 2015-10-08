# -*- coding: utf-8 -*-

from dlstats import constants

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

def clean_mongodb(db=None):
    db = db or get_mongo_db()
    for col in constants.COL_ALL:
        try:
            db.drop_collection(col)
        except:
            pass

def clean_es():
    es = get_es_db()    
    #TODO: remove index
