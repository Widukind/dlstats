# -*- coding: utf-8 -*-

import logging
import os
import sys
from pprint import pprint
import string

import pandas

from pymongo import ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError
from bson.son import SON

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

    '''********* PROVIDERS ********'''

    db[constants.COL_PROVIDERS].create_index([
        ("name", ASCENDING)], 
        name="name_idx", unique=True)

    '''********* CATEGORIES *******'''
    
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

    db[constants.COL_SERIES].create_index([
        ("provider", ASCENDING), 
        ("tags", ASCENDING), 
        ("frequency", DESCENDING)], 
        name="provider_tags_frequency_idx")

    '''********* TAGS ***********'''

    db[constants.COL_TAGS_DATASETS].create_index([
        ("name", ASCENDING)], 
        name="name_idx", unique=True)

    db[constants.COL_TAGS_DATASETS].create_index([
        ("providers.name", ASCENDING)], 
        name="providers_name_idx")

    db[constants.COL_TAGS_SERIES].create_index([
        ("name", ASCENDING)], 
        name="name_idx", unique=True)

    db[constants.COL_TAGS_SERIES].create_index([
        ("providers.name", ASCENDING)], 
        name="providers_name_idx")
    
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


#'!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
TAGS_REPLACE_CHARS = []
TAGS_REPLACE_CHARS.extend([s for s in string.punctuation if not s in ["_"]]) #TODO: "-", 

TAGS_MIN_CHAR = 2

def tags_filter(value):

    if value in constants.TAGS_EXCLUDE_WORDS:
        return False
    
    if not value or len(value.strip()) < TAGS_MIN_CHAR:
        return False
    
    if value in ["-", "_", " "]:
        return False
    
    return True            

def tags_map(value):

    value = value.strip().lower()

    new_value = []
    for v in value:
        if not v in TAGS_REPLACE_CHARS:
            new_value.append(v)
        else:
            new_value.append(" ")
    
    return "".join(new_value).split()
        
def str_to_tags(value_str):
    """Split and filter word - return array of word (to lower) 

    >>> utils.str_to_tags("Bank's of France")
    ['bank', 'france']
    
    >>> utils.str_to_tags("Bank's of & France")
    ['bank', 'france']
    
    >>> utils.str_to_tags("France")
    ['france']
    
    >>> utils.str_to_tags("Bank's")
    ['bank']                
    """    
    tags = tags_map(value_str)
    return [a for a in filter(tags_filter, tags)]
    
def generate_tags(db, doc, doc_type=None, 
                  doc_provider=None, doc_dataset=None):
    """Split and filter datas for return array of tags
    
    Used in update_tags()

    :param pymongo.database.Database db: MongoDB Database instance
    :param doc dict: Document MongoDB        
    :param doc_type str: 
    :param bool is_indexes: Bypass create_or_update_indexes() if False 

    :raises ValueError: if provider_name is None
    """
        
    select_for_tags = []
    tags = []
    
    def search_dataset_dimensionList(key, value, dataset_doc):
        if key in dataset_doc['dimensionList']: 
            dimensions = dataset_doc['dimensionList'][key]
            for d in dimensions:
                if value == d[0]:
                    return d[1] 

    def search_dataset_attributeList(key, value, dataset_doc):
        if key in dataset_doc['attributeList']:
            attributes = dataset_doc['attributeList'][key]
            for a in attributes:
                if value == a[0]:
                    return a[1] 
    
    if doc_type == constants.COL_DATASETS:

        select_for_tags.append(doc['provider'])
        select_for_tags.append(doc['datasetCode'])
        select_for_tags.append(doc['name'])
        
        if 'notes' in doc and len(doc['notes'].strip()) > 0: 
            select_for_tags.append(doc['notes'].strip())
        
        for key, values in doc['dimensionList'].items():            
            #select_for_tags.append(key)        #dimension name:            
            for item in values:               
                #TODO: dimension key ?
                #select_for_tags.append(item[0])
                select_for_tags.append(item[1])

        for key, values in doc['attributeList'].items():            
            #select_for_tags.append(key)        #attribute name:            
            for item in values:            
                #TODO: attribute key ?
                #select_for_tags.append(item[0])
                select_for_tags.append(item[1])
        
    elif doc_type == constants.COL_SERIES:

        query = {
            "provider": doc['provider'], 
            "datasetCode": doc['datasetCode']
        }
        dataset = doc_dataset or db[constants.COL_DATASETS].find_one(query)
        
        if not dataset:
            raise Exception("dataset not found for provider[%(provider)s] - datasetCode[%(datasetCode)s]" % query)

        select_for_tags.append(doc['provider'])
        select_for_tags.append(doc['datasetCode'])
        select_for_tags.append(doc['key'])
        select_for_tags.append(doc['name'])
        
        if 'notes' in doc and len(doc['notes'].strip()) > 0: 
            select_for_tags.append(doc['notes'].strip())

        for dimension_key, dimension_code in doc['dimensions'].items():
            #select_for_tags.append(dimension_key)
            if dimension_key and dimension_code:
                dimension_value = search_dataset_dimensionList(dimension_key, 
                                                               dimension_code, 
                                                               dataset)
                if dimension_value:            
                    select_for_tags.append(dimension_value)

        for attribute_key, attribute_code in doc['attributes'].items():            
            #select_for_tags.append(attribute_key)
            if attribute_key and attribute_code:
                attribute_value = search_dataset_attributeList(attribute_key, 
                                                               attribute_code, 
                                                               dataset)
                if attribute_value:
                    select_for_tags.append(attribute_value)

    for value in select_for_tags:
        tags.extend(str_to_tags(value))
        
    return sorted(list(set(tags)))

def bulk_result_aggregate(bulk_result):
    """Aggregate array of bulk execute to unique dict
    
    >>> bulk_result[0]
    {'upserted': [], 'nUpserted': 10, 'nModified': 0, 'nMatched': 20, 'writeErrors': [], 'nRemoved': 0, 'writeConcernErrors': [], 'nInserted': 0}
    >>> bulk_result[1]
    {'upserted': [], 'nUpserted': 5, 'nModified': 0, 'nMatched': 4, 'writeErrors': [], 'nRemoved': 0, 'writeConcernErrors': [], 'nInserted': 0}
    >>> result = bulk_result_aggregate(bulk_result)
    >>> result
    {'writeErrors': [], 'nUpserted': 15, 'nMatched': 0, 'nModified': 0, 'upserted': [], 'nRemoved': 0, 'writeConcernErrors': [], 'nInserted': 0}    
    """
    
    bulk_dict = {
        "nUpserted": 0,
        "nModified": 0,
        "nMatched": 0,
        "nRemoved": 0,
        "nInserted": 0,
        #"upserted": [],
        "writeErrors": [],
        "writeConcernErrors": [],
    }
    
    for r in bulk_result:
        bulk_dict["nUpserted"] += r["nUpserted"]
        bulk_dict["nModified"] += r["nModified"]
        bulk_dict["nMatched"] += r["nMatched"]
        bulk_dict["nRemoved"] += r["nRemoved"]
        bulk_dict["nInserted"] += r["nInserted"]
        #bulk_dict["upserted"].extend(r["upserted"])
        bulk_dict["writeErrors"].extend(r["writeErrors"])
        bulk_dict["writeConcernErrors"].extend(r["writeConcernErrors"])
    
    return bulk_dict
    

def run_bulk(bulk=None):
    try:
        result = bulk.execute()
        #TODO: bulk.execute({'w': 3, 'wtimeout': 1})
        #pprint(result)
        return result
    except BulkWriteError as err:        
        #pprint(err.details)
        raise
    
def update_tags(db, 
                provider_name=None, dataset_code=None, serie_key=None, 
                col_name=None, max_bulk=20):
    
    #TODO: cumul des results bulk
    bulk = db[col_name].initialize_unordered_bulk_op()
    count = 0
    query = {}
    projection = None
    doc_provider = None
    doc_dataset = None

    if dataset_code:
        query['datasetCode'] = dataset_code

    if col_name == constants.COL_DATASETS:
        projection = {"docHref": False}
    
    if col_name == constants.COL_SERIES and serie_key:
        query['key'] = serie_key
        
    if col_name == constants.COL_SERIES:
        projection = {"releaseDates": False, "values": False}

    for doc in db[col_name].find(query):
        
        #TODO: load dataset doc if search series ?
        tags = generate_tags(db, doc, doc_type=col_name)
        
        #projection=projection
        bulk.find({'_id': doc['_id']}).update_one({"$set": {'tags': tags}})
        
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

def search_tags(db, 
               provider_name=None, 
               dataset_code=None, 
               frequency=None,
               projection=None, 
               search_tags=None,
               search_type=constants.COL_DATASETS,
               start_date=None,
               end_date=None,
               sort=None,
               sort_desc=False,                        
               skip=None, limit=None):
    """Search in series by tags field
    
    >>> from dlstats import utils
    >>> db = utils.get_mongo_db()
    
    >>> docs = utils.search_series_tags(db, search_tags=["Belgium", "Euro"])    

    # Search in all provider and dataset
    >>> docs = utils.search_series_tags(db, frequency="A", search_tags=["Belgium", "Euro", "Agriculture"])

    # Filter provider and/or dataset
    >>> docs = utils.search_series_tags(db, provider_name="Eurostat", dataset_code="nama_10_a10", search_tags=["Belgium", "Euro", "Agriculture"])
    
    #print(docs.count())    
    #for doc in docs: print(doc['provider'], doc['datasetCode'], doc['key'], doc['name'])
    """
    
    '''Convert search tag to lower case and strip tag'''
    tags = str_to_tags(search_tags)        
    #tags = [t.strip().lower() for t in search_tags]

    # TODO: OR, NOT ?
    query = {"tags": {"$all": tags}}

    if provider_name:
        if isinstance(provider_name, str):
            providers = [provider_name]
        else:
            providers = provider_name
        query['provider'] = {"$in": providers}
        
    if search_type == "series":

        COL_SEARCH = constants.COL_SERIES

        date_freq = constants.FREQ_ANNUALY

        if frequency:
            query['frequency'] = frequency
            date_freq = frequency
                        
        if dataset_code:
            query['datasetCode'] = dataset_code

        if start_date:
            ordinal_start_date = pandas.Period(start_date, freq=date_freq).ordinal
            query["startDate"] = {"$gte": ordinal_start_date}
        
        if end_date:
            query["endDate"] = {"$lte": pandas.Period(end_date, freq=date_freq).ordinal}

    else:
        COL_SEARCH = constants.COL_DATASETS
        
    print("---------- QUERY -------------------")        
    pprint(query)
    print("------------------------------------")        
    
    cursor = db[COL_SEARCH].find(query, projection=projection)

    if skip:
        cursor = cursor.skip(skip)
    
    if limit:
        cursor = cursor.limit(limit)
    
    if sort:
        sort_direction = ASCENDING
        if sort_desc:
            sort_direction = DESCENDING
        cursor = cursor.sort(sort, sort_direction)
    
    return cursor
           
def search_series_tags(db, **kwargs):
    return search_tags(db, search_type=constants.COL_SERIES, **kwargs)

def search_datasets_tags(db, **kwargs):
    return search_tags(db, search_type=constants.COL_DATASETS, **kwargs)

def _aggregate_tags(db, source_col, target_col, max_bulk=20):

    bulk = db[target_col].initialize_unordered_bulk_op()
    count = 0
    
    pipeline = [
      {"$match": {"tags.0": {"$exists": True}}},
      {'$project': { '_id': 0, 'tags': 1, 'provider': 1}},
      {"$unwind": "$tags"},
      {"$group": {"_id": {"tag": "$tags", "provider": "$provider"}, "count": {"$sum": 1}}},
      {'$project': { 'tag': "$_id.tag", 'count': 1, 'provider': {"name": "$_id.provider", "count": "$count"}}},
      {"$group": {"_id": "$tag", "count": {"$sum": "$count"}, "providers":{ "$addToSet": "$provider" } }},
      #{"$sort": SON([("count", -1), ("_id", -1)])}      
    ]
    
    bulk_result = []
    
    result = db[source_col].aggregate(pipeline, allowDiskUse=True)
    
    for doc in result:
        update = {
            '$addToSet': {'providers': {"$each": doc['providers']}},
            "$set": {"count": doc['count']}
        }
        bulk.find({'name': doc['_id']}).upsert().update_one(update)
        count += 1
        
        if count >= max_bulk:
            bulk_result.append(run_bulk(bulk))
            bulk = db[target_col].initialize_unordered_bulk_op()
            count = 0

    #bulk delta
    if count > 0:
        bulk_result.append(run_bulk(bulk))
    
    return bulk_result_aggregate(bulk_result)
    
def aggregate_tags_datasets(db, max_bulk=20):
    """
    >>> pp(list(db.tags.datasets.find().sort([("count", -1)]))[0])
    {'_id': ObjectId('565ade73426049c4cea21c0e'),
     'count': 10,
     'name': 'france',
     'providers': [{'count': 8, 'name': 'BIS'},
                   {'count': 1, 'name': 'OECD'},
                   {'count': 1, 'name': 'Eurostat'}]}
                   
    db.tags.datasets.distinct("name")
    
    TOP 10:
        >>> pp(list(db.tags.datasets.find({}).sort([("count", -1)])[:10]))
        [{'_id': ObjectId('565ade73426049c4cea21c0e'),
          'count': 10,
          'name': 'france',
          'providers': [{'count': 8, 'name': 'BIS'},
                        {'count': 1, 'name': 'OECD'},
                        {'count': 1, 'name': 'Eurostat'}]},
         {'_id': ObjectId('565ade73426049c4cea21c7d'),
          'count': 10,
          'name': 'norway',
          'providers': [{'count': 8, 'name': 'BIS'},
                        {'count': 1, 'name': 'OECD'},
                        {'count': 1, 'name': 'Eurostat'}]},                       
                               
    """
    return _aggregate_tags(db, 
                           constants.COL_DATASETS, 
                           constants.COL_TAGS_DATASETS, 
                           max_bulk=max_bulk)

def aggregate_tags_series(db, max_bulk=20):
    return _aggregate_tags(db, 
                           constants.COL_SERIES, 
                           constants.COL_TAGS_SERIES, 
                           max_bulk=max_bulk)
