# -*- coding: utf-8 -*-

import os

from pymongo import ASCENDING, DESCENDING

from dlstats import constants

UPDATE_INDEXES = False

ES_INDEX_CREATED = False

def create_elasticsearch_index(es_client=None, index=None):
    """Create ElasticSearch Index
    """
    global ES_INDEX_CREATED
    
    if ES_INDEX_CREATED:
        return

    index = index or constants.ES_INDEX
    es_client = es_client or get_es_client()
    try:
        es_client.indices.create(index)
    except:
        pass
    
    ES_INDEX_CREATED = True

def create_or_update_indexes(db, force_mode=False):
    """Create or update MongoDB indexes"""
    
    global UPDATE_INDEXES
    
    if not force_mode and UPDATE_INDEXES:
        return

    db[constants.COL_PROVIDERS].create_index([
        ("name", ASCENDING)], 
        name="name_idx", unique=True)
    
    db[constants.COL_CATEGORIES].create_index([
        ("provider", ASCENDING), 
        ("categoryCode", ASCENDING)], 
        name="provider_categoryCode_idx", unique=True)
     
    #TODO: lastUpdate DESCENDING ?
    db[constants.COL_DATASETS].create_index([
            ("provider", ASCENDING), 
            ("datasetCode", ASCENDING)], 
            name="provider_datasetCode_idx", unique=True)
        
    db[constants.COL_DATASETS].create_index([
        ("name", ASCENDING)], 
        name="name_idx")
    
    db[constants.COL_DATASETS].create_index([
        ("lastUpdate", DESCENDING)], 
        name="lastUpdate_idx")

    db[constants.COL_SERIES].create_index([
        ("provider", ASCENDING), 
        ("datasetCode", ASCENDING), 
        ("key", ASCENDING)], 
        name="provider_datasetCode_key_idx", unique=True)

    db[constants.COL_SERIES].create_index([
        ("key", ASCENDING)], 
        name="key_idx")
        
    db[constants.COL_SERIES].create_index([
        ("name", ASCENDING)], 
        name="name_idx")
    
    db[constants.COL_SERIES].create_index([
        ("frequency", DESCENDING)], 
        name="frequency_idx")
    
    UPDATE_INDEXES = True

def get_mongo_url():
    return os.environ.get("WIDUKIND_MONGODB_URL", "mongodb://localhost/widukind")


def get_es_url():
    return os.environ.get("WIDUKIND_ES_URL", "http://localhost:9200")


def get_mongo_client(url=None):
    from pymongo import MongoClient
    # TODO: tz_aware
    url = url or get_mongo_url()
    client = MongoClient(url)
    return client


def get_mongo_db(url=None):
    from pymongo import MongoClient
    # TODO: tz_aware
    url = url or get_mongo_url()
    client = get_mongo_client(url)
    return client[constants.MONGODB_NAME]


def get_es_client(url=None):
    from elasticsearch import Elasticsearch
    from urllib.parse import urlparse
    url = url or get_es_url()
    url = urlparse(url)
    es = Elasticsearch([{"host": url.hostname, "port": url.port}])
    return es

def clean_mongodb(db=None):
    """Drop all collections used by dlstats
    """
    db = db or get_mongo_db()
    for col in constants.COL_ALL:
        try:
            db.drop_collection(col)
        except:
            pass

def clean_elasticsearch(es_client=None, index=None):
    """Delete and create ElasticSearch Index
    """

    index = index or constants.ES_INDEX
    es_client = es_client or get_es_client()
    try:
        es_client.indices.delete(index=index)
    except:
        pass

    try:
        es_client.indices.create(index)
    except:
        pass


def configure_logging(debug=False, stdout_enable=True, config_file=None,
                      level="INFO"):

    import sys
    import logging
    import logging.config
    
    if config_file:
        logging.config.fileConfig(config_file, disable_existing_loggers=True)
        return logging.getLogger('')


    #TODO: handler file ?    
    LOGGING = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'debug': {
                'format': 'line:%(lineno)d - %(asctime)s %(name)s: [%(levelname)s] - [%(process)d] - [%(module)s] - %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
            'simple': {
                'format': '%(asctime)s %(name)s: [%(levelname)s] - %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
        },    
        'handlers': {
            'null': {
                'level':level,
                'class':'logging.NullHandler',
            },
            'console':{
                'level':level,
                'class':'logging.StreamHandler',
                'formatter': 'simple'
            },      
        },
        'loggers': {
            '': {
                'handlers': [],
                'level': 'INFO',
                'propagate': False,
            },
        },
    }
    
    if stdout_enable:
        if not 'console' in LOGGING['loggers']['']['handlers']:
            LOGGING['loggers']['']['handlers'].append('console')

    '''if handlers is empty'''
    if not LOGGING['loggers']['']['handlers']:
        LOGGING['loggers']['']['handlers'] = ['console']
    
    if debug:
        LOGGING['loggers']['']['level'] = 'DEBUG'
        for handler in LOGGING['handlers'].keys():
            LOGGING['handlers'][handler]['formatter'] = 'debug'
            LOGGING['handlers'][handler]['level'] = 'DEBUG' 

    logging.config.dictConfig(LOGGING)
    return logging.getLogger()

