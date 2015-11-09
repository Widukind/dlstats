# -*- coding: utf-8 -*-

import os

from dlstats import constants

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
    return client.get_default_database()


def get_es_client(url=None):
    from elasticsearch import Elasticsearch
    from urllib.parse import urlparse
    url = url or get_es_url()
    url = urlparse(url)
    es = Elasticsearch([{"host": url.hostname, "port": url.port}])
    return es

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

