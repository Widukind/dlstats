# -*- coding: utf-8 -*-

import logging
import os
import sys
from pprint import pprint
import string

from pymongo import ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError

from dlstats import constants

logger = logging.getLogger(__name__)

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

    db[constants.COL_CATEGORIES].create_index([
        ("tags", ASCENDING)], 
        name="tags_idx")
         
    
    '''********* DATASETS *********'''
    
    #TODO: lastUpdate DESCENDING ?
    db[constants.COL_DATASETS].create_index([
            ("provider", ASCENDING), 
            ("datasetCode", ASCENDING)], 
            name="provider_datasetCode_idx", unique=True)
        
    db[constants.COL_DATASETS].create_index([
        ("name", ASCENDING)], 
        name="name_idx")

    db[constants.COL_DATASETS].create_index([
        ("tags", ASCENDING)], 
        name="tags_idx")
    
    db[constants.COL_DATASETS].create_index([
        ("lastUpdate", DESCENDING)], 
        name="lastUpdate_idx")

    '''********* SERIES *********'''

    db[constants.COL_SERIES].create_index([
        ("provider", ASCENDING), 
        ("datasetCode", ASCENDING), 
        ("key", ASCENDING)], 
        name="provider_datasetCode_key_idx", unique=True)

    db[constants.COL_SERIES].create_index([
        ("key", ASCENDING)], 
        name="key_idx")

    db[constants.COL_SERIES].create_index([
        ("provider", ASCENDING), 
        ("datasetCode", ASCENDING)], 
        name="provider_datasetCode_idx")

    db[constants.COL_SERIES].create_index([
        ("datasetCode", ASCENDING)], 
        name="datasetCode_idx")

    db[constants.COL_SERIES].create_index([
        ("provider", ASCENDING)], 
        name="provider_idx")    

    db[constants.COL_SERIES].create_index([
        ("tags", ASCENDING)], 
        name="tags_idx")
    
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
    from elasticsearch import Elasticsearch, RequestsHttpConnection
    from urllib.parse import urlparse
    url = url or get_es_url()
    url = urlparse(url)
    es = Elasticsearch([{"host": url.hostname, "port": url.port}],
                       connection_class=RequestsHttpConnection, 
                       timeout=30, 
                       max_retries=5, 
                       use_ssl=False,
                       verify_certs=False,
                       sniff_on_start=True)
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


def clean_es_with_filter(es_client=None, index=None, doc_type=None, 
                         filter=None,
                         #source=None, 
                         bulk_size=200,
                         ignore_errors=True):

    index = index or constants.ES_INDEX
    es_client = es_client or get_es_client()
    
    from elasticsearch import helpers
    
    results = []
    ignore = []
    
    try:
        if ignore_errors:
            ignore=[400, 404]
        
        res = helpers.scan(es_client, 
                     index=index,
                     query={'_source': 'false','filter': {'term': filter}}, 
                     #scroll, 
                     #raise_on_error, 
                     #preserve_order
                     )
        """    
        res = es_client.search(index=index, scroll='1m', search_type='scan', 
                               #size=bulk_size,
                               #doc_type=doc_type, 
                               body={'_source': 'false','filter': {'term': filter}},
                               ignore=ignore)
        """
        
        print("---------------------")
        print(res, type(res))
        print("---------------------")
        for r in res:
            print(r, type(r))
            """
DEBUG:elasticsearch:< {"_scroll_id":"c2NhbjswOzE7dG90YWxfaGl0czowOw==","took":5,"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"total":0,"max_score":0.0,
"hits":[]}}            
            """
        
        return
        
        sid = res['_scroll_id']
        scroll_size = res['hits']['total']
        
        while (scroll_size > 0):
            res = es_client.scroll(scroll_id=sid, scroll='1m')
            if len(res['hits']['hits']) == 0:
                break
            sid = res['_scroll_id']
            bulk = []
            for r in res['hits']['hits']:
                bulk.append({'delete': {'_index': str(r['_index']),
                                        '_type': str(r['_type']),
                                        '_id': str(r['_id'])}})
            
            results.append(es_client.bulk(body=bulk))
        
        return results
        """
        {'errors': False,
         'items': [{'delete': {'_id': 'BIS.CNFS',
                               '_index': 'widukind',
                               '_type': 'datasets',
                               '_version': 3,
                               'found': True,
                               'status': 200}}],
         'took': 48}
        """
            
    except Exception as err:
        logger.error(str(err))
        raise

def clean_es_dataset(es_client=None, index=None, 
                     provider_name=None, dataset_code=None, 
                     bulk_size=200):

    index = index or constants.ES_INDEX
    es_client = es_client or get_es_client()
        
    filter = {"provider": provider_name}
    if dataset_code:
        filter["datasetCode"] = dataset_code
    #filter = { "_id": provider_name + '.' + dataset_code}
    #doc_type = 'datasets'
    filter = {"_source": filter}

    return es_client.search(index=index,
                            doc_type='datasets,series', 
                            body=filter)
    
    return es_client.delete_by_query(index=index,
                       #doc_type='datasets,series', 
                       q=filter)
    
    return clean_es_with_filter(es_client=es_client, index=index, 
                                #doc_type=doc_type,
                                #source=source,
                                filter=filter, 
                                bulk_size=bulk_size)

    
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
                'formatter': 'simple',
                'stream': sys.stdout
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


EXCLUDE_WORDS = [
    "the",
    "to",
    "from",
    "of",
    "on",
    "in"
]

REPLACE_CHARS = []

#'!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
REPLACE_CHARS.extend([s for s in string.punctuation if not s in ["-", "_"]])

def generate_tags(db, doc, doc_type=None):
    """Generate array of tag for series and dataset
    """
    
    def clean(values_for_split, tags):

        for values in values_for_split:
            
            for value in values.split():
                v = value.strip().lower()
                
                for c in REPLACE_CHARS:
                    v = v.replace(c, "")                
                
                if v.isdigit():
                    continue
                
                if v in EXCLUDE_WORDS:
                    continue
                
                if not v or len(v) == 0:
                    continue
                
                #TODO: exclure les 1 char ?
                
                if v in ["-", "_"]:
                    continue
                
                if not v in tags:
                    tags.append(v)
        
    
    values_for_split = []
    tags = []
    
    def search_dataset_dimensionList(key, value, dataset_doc):
        dimensions = dataset_doc['dimensionList'][key]
        for d in dimensions:
            if value == d[0]:
                return d[1] 

    def search_dataset_attributeList(key, value, dataset_doc):
        attributes = dataset_doc['attributeList'][key]
        for a in attributes:
            if value == a[0]:
                return a[1] 
    
    if doc_type == constants.COL_DATASETS:

        #values_for_split.append(doc['datasetCode'])
        #values_for_split.append(doc['provider'])
        values_for_split.append(doc['name'])
        
        if 'notes' in doc and len(doc['notes'].strip()) > 0: 
            values_for_split.append(doc['notes'].strip())
        
        for key, values in doc['dimensionList'].items():            
            #values_for_split.append(key)        #dimension name:            
            for item in values:                 #value de dimension value               
                #TODO: clé de la dimension:
                #values_for_split.append(item[0])
                values_for_split.append(item[1])
        #clean(values_for_split, tags)

        for key, values in doc['attributeList'].items():            
            #values_for_split.append(key)        #attribute name:            
            for item in values:                 #value de dimension value            
                #TODO: clé de l'attribut:
                #values_for_split.append(item[0])
                values_for_split.append(item[1])
        
        clean(values_for_split, tags)

    elif doc_type == constants.COL_SERIES:

        query = {
            "provider": doc['provider'], 
            "datasetCode": doc['datasetCode']
        }
        dataset = db[constants.COL_DATASETS].find_one(query)

        #values_for_split.append(doc['datasetCode'])
        #values_for_split.append(doc['provider'])
        #values_for_split.append(doc['key'])
        values_for_split.append(doc['name'])
        
        if 'notes' in doc and len(doc['notes'].strip()) > 0: 
            values_for_split.append(doc['notes'].strip())

        for dimension_key, dimension_code in doc['dimensions'].items():            
            #values_for_split.append(dimension_key)
            dimension_value = search_dataset_dimensionList(dimension_key, 
                                                           dimension_code, 
                                                           dataset)
            if dimension_value:            
                values_for_split.append(dimension_value)

        for attribute_key, attribute_code in doc['attributes'].items():            
            #values_for_split.append(attribute_key)
            attribute_value = search_dataset_attributeList(attribute_key, 
                                                           attibute_code, 
                                                           dataset)
            if attribute_value:
                values_for_split.append(attribute_value)
        
        clean(values_for_split, tags)
        
    return sorted(tags)

def run_bulk(bulk=None):
    try:
        result = bulk.execute()
        #TODO: bulk.execute({'w': 3, 'wtimeout': 1})
        #pprint(result)
        return result
    except BulkWriteError as err:        
        pprint(err.details)
        raise
    
def update_tags(db, 
                provider_name=None, dataset_code=None, serie_key=None, 
                col_name=None, max_bulk=20):
    
    #TODO: cumul des results bulk
    bulk = db[col_name].initialize_unordered_bulk_op()
    count = 0
    query = {}

    if provider_name:
        query['provider'] = provider_name
    if dataset_code:
        query['datasetCode'] = dataset_code
    if col_name == constants.COL_SERIES and serie_key:
        query['key'] = serie_key

    for doc in db[col_name].find(query):
        tags = generate_tags(db, doc, doc_type=col_name)
        bulk.find({'_id': doc['_id']}).update_one({"$set": {'tags': sorted(tags)}})
        count += 1
        
        if count >= max_bulk:
            run_bulk(bulk)
            bulk = db[col_name].initialize_unordered_bulk_op()
            count = 0

    #bulk delta
    if count > 0:
        run_bulk(bulk)

def drop_gridfs(db):
    collections = db.collection_names()
    if 'fs.files' in collections:
        db.drop_collection('fs.files')
        db.drop_collection('fs.chunks')

FREQUENCIES_CONVERT = {
    "quarterly": "Q",
    "quarter": "Q",
    "monthly": "M",                       
    "month": "M",                       
    "annualy": "A",                       
    "annual": "A",                       
}

def search_series_tags(db, 
                       provider_name=None, dataset_code=None, frequency=None, 
                       search_tags=None, skip=0, limit=0):
    """Search in series by tags field
    
    >>> from dlstats import utils
    >>> db = utils.get_mongo_db()    

    # Search in all provider and dataset
    >>> docs = utils.search_series_tags(db, frequency="A", search_tags=["Belgium", "Euro", "Agriculture"])

    # Filter provider and/or dataset
    >>> docs = utils.search_series_tags(db, provider_name="Eurostat", dataset_code="nama_10_a10", search_tags=["Belgium", "Euro", "Agriculture"])
    
    #print(docs.count())    
    #for doc in docs: print(doc['provider'], doc['datasetCode'], doc['key'], doc['name'])
    """

    '''Convert search tag to lower case and strip tag'''    
    tags = [t.strip().lower() for t in search_tags]

    query = {"tags": {"$all": tags}}

    if provider_name:
        query['provider'] = provider_name
    if dataset_code:
        query['datasetCode'] = dataset_code
    if frequency:
        '''Convert frequencies words'''    
        search_frequencies = []
        if isinstance(frequency, str):
            #TODO: raise error if frequency not found ?
            search_frequencies.append(FREQUENCIES_CONVERT.get(frequency, 'Annual'))
        else:
            for f in frequency:
                search_frequencies.append(FREQUENCIES_CONVERT.get(f, 'Annual'))
        
        query['frequency'] = {"$in": list(set(search_frequencies))}
    
    #print(query, limit)
    return db[constants.COL_SERIES].find(query, skip=skip, limit=limit)

