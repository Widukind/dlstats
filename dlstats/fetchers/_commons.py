#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
.. module:: _commons
    :synopsis: Code imported by the different fetchers
"""
import os
import pymongo
from pymongo import IndexModel, ASCENDING, DESCENDING
from pymongo import ReturnDocument
from datetime import datetime
import logging
import pprint
from elasticsearch import Elasticsearch, helpers
from collections import defaultdict, OrderedDict
from copy import deepcopy

from dlstats import mongo_client
from dlstats import configuration
from dlstats import constants
from dlstats.fetchers import schemas
from dlstats import logger

UPDATE_INDEXES = False

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

class Fetcher(object):
    """Abstract base class for all fetchers"""
    
    def __init__(self, 
                 provider_name=None, 
                 db=None, 
                 es_client=None,
                 is_indexes=True):
        """
        :param str provider_name: Provider Name
        :param pymongo.database.Database db: MongoDB Database instance        
        :param elasticsearch.Elasticsearch es_client: Instance of Elasticsearch client
        :param bool is_indexes: Bypass create_or_update_indexes() if False 

        :raises ValueError: if provider_name is None
        """        
        if not provider_name:
            raise ValueError("provider_name is required")

        self.provider_name = provider_name
        self.db = db or mongo_client.widukind
        self.es_client = es_client or Elasticsearch()
        self.provider = None
        
        if is_indexes:
            create_or_update_indexes(self.db)

    def upsert_categories(self):
        """Upsert the categories in MongoDB
        """        
        raise NotImplementedError("This method from the Fetcher class must"
                                  "be implemented.")
    
    def upsert_series(self):
        """Upsert all the series in MongoDB
        
        .. versionchanged:: 0.3.0
           Remove function. (Not used in fetchers)                
        """        
        raise NotImplementedError("This method from the Fetcher class must"
                                  "be implemented.")

    #TODO: not used function ?    
    def upsert_a_series(self, id):
        """Upsert the series in MongoDB
        
        .. versionchanged:: 0.3.0
           Remove function. (Not used in fetchers)
        
        :param id: :class:`str` - ID of :class:`Series`        
        """        
        raise NotImplementedError("This method from the Fetcher class must"
                                  "be implemented.")
    
    def upsert_dataset(self, dataset_code, datas=None):
        """Upsert a dataset in MongoDB
        
        :param str dataset_code: ID of :class:`Datasets`
        """        
        raise NotImplementedError("This method from the Fetcher class must"
                                  "be implemented.")
    
    #TODO: not used function ?    
    def insert_provider(self):
        """Insert the provider in MongoDB
        """        
        self.provider.update_database()
        
    def update_metas(self, dataset_code):
        """Update Meta datas to ElasticSearch
        
        :param str dataset_code: ID of :class:`Datasets`
        """        
        es = ElasticIndex(db=self.db, es_client=self.es_client)
        es.make_index(self.provider_name, dataset_code)
        
class DlstatsCollection(object):
    """Abstract base class for objects that are stored and indexed by dlstats
    """
    
    def __init__(self, fetcher=None):
        """
        :param Fetcher fetcher: Fetcher instance 

        :raises ValueError: if fetcher is None
        :raises TypeError: if not instance of :class:`Fetcher`  
        """
        
        if not fetcher:
            raise ValueError("fetcher is required")

        if not isinstance(fetcher, Fetcher):
            raise TypeError("Bad type for fetcher")
        
        self.fetcher = fetcher
        
    def update_mongo_collection(self, collection, keys, bson, 
                                log_level=logging.INFO):
        """Update one document

        :param str collection: Collection name
        :param list keys: List of value for unique key
        :param dict bson: Document values
        :param int log_level: Default logging level
        
        :return: Instance of :class:`bson.objectid.ObjectId`  
        """
        lgr = logging.getLogger(__name__)
        key = {k: bson[k] for k in keys}
        try:
            result = self.fetcher.db[collection].find_one_and_replace(key, bson, upsert=True,
                                                                      return_document=ReturnDocument.AFTER)
            result = result['_id']
        except Exception as err:
            lgr.critical('%s.update_database() failed for %s - %s [%s]' % (collection, str(key), str(result), str(err)))
            return None
        else:
            lgr.log(log_level,collection + ' ' + str(key) + ' updated.')
            return result
        
class Providers(DlstatsCollection):
    """Providers class
    
    Inherit from :class:`DlstatsCollection`
    """

    def __init__(self,
                 name=None,
                 long_name=None,
                 region=None,
                 website=None,
                 fetcher=None):
        """        
        :param str name: Provider Short Name
        :param str long_name: Provider Long Name
        :param str region: Region
        :param str website: Provider Web Site
        :param Fetcher fetcher: Fetcher instance
        """        
        super().__init__(fetcher=fetcher)
        self.name = name
        self.long_name = long_name
        self.region = region
        self.website = website

        self.validate = schemas.provider_schema({
            'name': self.name,
            'longName': self.long_name,
            'region': self.region,
            'website': self.website
         })

    def __repr__(self):
        return pprint.pformat([(key, self.validate[key]) for key in sorted(self.validate.keys())])

    @property
    def bson(self):
        return {'name': self.name,
                'longName': self.long_name,
                'region': self.region,
                'website': self.website}

    def update_database(self):
        schemas.provider_schema(self.bson)
        return self.update_mongo_collection(constants.COL_PROVIDERS, 
                                            ['name'], 
                                            self.bson)
                
class Categories(DlstatsCollection):
    """Categories class
    
    Inherit from :class:`DlstatsCollection`
    """
    
    def __init__(self,
                 provider=None,
                 name=None,
                 docHref=None,
                 children=None,
                 categoryCode=None,
                 lastUpdate=None,
                 exposed=False,
                 fetcher=None):
        """
        :param str provider: Provider name
        :param str name: Category short name
        :param str docHref: (Optional) Category - web link
        :param bson.objectid.ObjectId children: Array of ObjectId or empty list        
        :param str categoryCode: Unique Category Code
        :param datetime.datetime lastUpdate: (Optional) Last updated date
        :param bool exposed: Exposed ?
        :param Fetcher fetcher: Fetcher instance
        """        
        super().__init__(fetcher=fetcher)
        self.provider = provider
        self.name = name
        self.docHref = docHref
        self.children = children
        self.categoryCode = categoryCode
        self.lastUpdate = lastUpdate
        self.exposed = exposed
        
        self.validate = schemas.category_schema({
            'provider': self.provider,
            'categoryCode': self.categoryCode,
            'name': self.name,
            'children': self.children,
            'docHref': self.docHref,
            'lastUpdate': self.lastUpdate,
            'exposed': self.exposed
        })

    def __repr__(self):
        return pprint.pformat([(key, self.validate[key])
                               for key in sorted(self.validate.keys())])

    @property
    def bson(self):
        return {'provider': self.provider,
                'name': self.name,
                'docHref': self.docHref,
                'children': self.children,
                'categoryCode': self.categoryCode,
                'lastUpdate': self.lastUpdate,
                'exposed': self.exposed}
        
    def update_database(self):
        # we will log to info when we switch to bulk update
        schemas.category_schema(self.bson)
        return self.update_mongo_collection(constants.COL_CATEGORIES, 
                                            ['provider', 'categoryCode'],
                                            self.bson, 
                                            log_level=logging.DEBUG)

class Datasets(DlstatsCollection):
    """Abstract base class for datasets
    
    Inherit from :class:`DlstatsCollection`
    """
    
    def __init__(self, 
                 provider_name=None,
                 dataset_code=None, 
                 name=None,
                 doc_href=None,
                 last_update=None,
                 bulk_size=1000,
                 fetcher=None, 
                 is_load_previous_version=True):
        """
        :param str provider_name: Provider name
        :param str dataset_code: Dataset code
        :param str name: Dataset name
        :param str doc_href: Dataset link
        :param int bulk_size: Batch size for mongo bulk
        :param datetime.datetime last_update: Dataset Last updated 
        :param Fetcher fetcher: Fetcher instance
        :param bool is_load_previous_version: Bypass load previous version if False        
        """        
        super().__init__(fetcher=fetcher)
        self.provider_name = provider_name
        self.dataset_code = dataset_code
        self.name = name
        self.doc_href = doc_href
        self.last_update = last_update
        self.bulk_size = bulk_size
        self.dimension_list = CodeDict()
        self.attribute_list = CodeDict()
        
        if is_load_previous_version:
            self.load_previous_version(provider_name, dataset_code)
            
        self.notes = ''
        
        self.series = Series(provider_name=self.provider_name, 
                             dataset_code=self.dataset_code, 
                             last_update=self.last_update, 
                             bulk_size=self.bulk_size, 
                             fetcher=self.fetcher)

    def __repr__(self):
        return pprint.pformat([('provider_name', self.provider_name),
                               ('dataset_code', self.dataset_code)])
    @property
    def bson(self):
        return {'provider': self.provider_name,
                'name': self.name,
                'datasetCode': self.dataset_code,
                'dimensionList': self.dimension_list.get_list(),
                'attributeList': self.attribute_list.get_list(),
                'docHref': self.doc_href,
                'lastUpdate': self.last_update,
                'notes': self.notes}

    def load_previous_version(self, provider_name, dataset_code):
        dataset = self.fetcher.db[constants.COL_DATASETS].find_one(
                                            {'provider': provider_name,
                                             'datasetCode': dataset_code})
        if dataset:
            # convert to dict of dict
            self.dimension_list.set_from_list(dataset['dimensionList'])
            self.attribute_list.set_from_list(dataset['attributeList'])
        
    def update_database(self):
        self.series.process_series_data()        
        schemas.dataset_schema(self.bson)
        return self.update_mongo_collection(constants.COL_DATASETS,
                                            ['provider', 'datasetCode'],
                                            self.bson)

class Series(DlstatsCollection):
    """Time Series class
    """
    
    def __init__(self, 
                 provider_name=None, 
                 dataset_code=None, 
                 last_update=None, 
                 bulk_size=1000, 
                 fetcher=None):
        """        
        :param str provider_name: Provider name
        :param str dataset_code: Dataset code
        :param datetime.datetime last_update: Last updated date
        :param int bulk_size: Batch size for mongo bulk
        :param Fetcher fetcher: Fetcher instance
        """        
        super().__init__(fetcher=fetcher)
        self.provider_name = provider_name
        self.dataset_code = dataset_code
        self.last_update = last_update
        self.bulk_size = bulk_size
        # temporary storage necessary to get old_bson in bulks
        self.series_list = []
    
    def __repr__(self):
        return pprint.pformat([('provider_name', self.provider_name),
                               ('datasetCode', self.dataset_code),
                               ('lastUpdate', self.last_update)])

    def process_series_data(self):
        count = 0
        while True:
            try:
                # append result from __next__ method in fetchers
                # one iteration by serie
                data = next(self.data_iterator)
                self.series_list.append(data)
            except StopIteration:
                break
            count += 1
            if count > self.bulk_size:
                self.update_series_list()
                count = 0
        if count > 0:
            self.update_series_list()

    def update_series_list(self):

        #TODO: gestion erreur bulk (BulkWriteError)

        keys = [s['key'] for s in self.series_list]

        old_series = self.fetcher.db[constants.COL_SERIES].find({
                                        'provider': self.provider_name,
                                        'datasetCode': self.dataset_code,
                                        'key': {'$in': keys}})

        old_series = {s['key']:s for s in old_series}
        
        bulk = self.fetcher.db[constants.COL_SERIES].initialize_ordered_bulk_op()

        for data in self.series_list:
            key = data['key']
            if not key in old_series:
                bson = self.update_series(data,is_bulk=True)                                
                bulk.insert(bson)
            else:
                old_bson = old_series[key]
                bson = self.update_series(data,
                                          old_bson=old_bson,
                                          is_bulk=True)                
                bulk.find({'_id': old_bson['_id']}).update({'$set': bson})
        
        try:
            result = bulk.execute()
        except pymongo.errors.BulkWriteError as err:
            logger.critical(str(err.details))
            pprint.pprint(err.details)
            raise
                 
        self.series_list = []
        return result
            
    def update_series(self, bson, old_bson=None, is_bulk=False):
        # gets either last_update passed to Datasets or the one provided
        # in the bson
        last_update = self.last_update
        if 'lastUpdate' in bson:
            last_update = bson.pop('lastUpdate')

        old_bson = old_bson or self.fetcher.db[constants.COL_SERIES].find_one({
            'provider': self.provider_name,
            'datasetCode': self.dataset_code,
            'key': bson['key']})
                
        col = self.fetcher.db[constants.COL_SERIES]

        if not old_bson:
            bson['releaseDates'] = [last_update for v in bson['values']]
            schemas.series_schema(bson)
            if is_bulk:
                return bson
            return col.insert(bson)
        else:
            revisions_is_present = False
            if 'revisions' in old_bson and len(old_bson['revisions']) > 0:
                bson['revisions'] = old_bson['revisions']
                revisions_is_present = True

            start_date = bson['startDate']
            old_start_date = old_bson['startDate']
            bson['releaseDates'] = deepcopy(old_bson['releaseDates'])
            
            iv1 = iv2 = 0
            if start_date < old_start_date:
                # index of old_bson values in bson values 
                iv2 = old_start_date - start_date
                # update all positions
                if revisions_is_present:
                    offset = old_start_date - start_date
                    ikeys = [int(k) for k in bson['revisions']]
                    for p in sorted(ikeys,reverse=True):
                        bson['revisions'][str(p+offset)] = bson['revisions'][str(p)]
                        bson['revisions'].pop(str(p))
                # add last_update in fron of releaseDates
                bson['releaseDates'] = [last_update for r in range(iv2)] + bson['releaseDates']
                        
            elif start_date > old_start_date:
                iv1 = start_date - old_start_date
                # previous, longer, series is kept
                # fill beginning with na
                for p in range(start_date-old_start_date):
                    # insert in front of the values, releaseDates and attributes
                    bson['values'].insert(0,'na')
                    bson['releaseDates'].insert(0,last_update)
                    for a in bson['attributes']:
                        bson['attributes'][a].insert(0,"") 
                
                bson['startDate'] = old_bson['startDate']
                
            for position,values in enumerate(zip(old_bson['values'][iv1:],bson['values'][iv2:])):
                if values[0] != values[1]:
                    bson['releaseDates'][position+iv2] = last_update
                    if not revisions_is_present:
                        bson['revisions'] = {}
                        revisions_is_present = True
                    rev = {'value':values[0],
                           'releaseDate':old_bson['releaseDates'][position+iv1]}
                    if str(position+iv2) in bson['revisions']:
                        bson['revisions'][str(position+iv2)].append(rev)
                    else:
                        bson['revisions'][str(position+iv2)] = [rev]
            if bson['endDate'] < old_bson['endDate']:
                for p in range(old_bson['endDate']-bson['endDate']):
                    bson['values'].append('na')
                    bson['releaseDates'].append(last_udpate)
                    for a in bson['attributes']:
                        bson['attributes'][a].append("")

            schemas.series_schema(bson)
            if is_bulk:
                return bson
            return col.find_one_and_update({'_id': old_bson['_id']}, {'$set': bson})

class CodeDict():
    """Class for handling code lists
    
    >>> code_list = {'Country': {'FR': 'France'}}
    >>> print(code_list)
    {'Country': {'FR': 'France'}}
    """    
    
    def __init__(self):
        # code_dict is a dict of OrderedDict
        self.code_dict = {}
        schemas.codedict_schema(self.code_dict)
        
    def update(self,arg):
        schemas.codedict_schema(arg.code_dict)
        self.code_dict.update(arg.code_dict)
        
    def update_entry(self,dim_name,dim_short_id,dim_long_id):
        if dim_name in self.code_dict:
            if not dim_long_id:
                dim_short_id = 'None'
                if 'None' not in self.code_dict[dim_name]:
                    self.code_dict[dim_name].update({'None': 'None'})
            elif not dim_short_id:
                # find the next (numerical) short id in self.code_dict[dim_name]
                try:
                    dim_short_id = next(key for key,value in self.code_dict[dim_name].items() if value == dim_long_id)
                except StopIteration:
                    dim_short_id = str(len(self.code_dict[dim_name]))
                    self.code_dict[dim_name].update({dim_short_id: dim_long_id})
            elif not dim_short_id in self.code_dict[dim_name]:
                self.code_dict[dim_name].update({dim_short_id: dim_long_id})
        else:
            if not dim_short_id:
                # numerical short id starts with 0
                dim_short_id = '0'
            self.code_dict[dim_name] = OrderedDict({dim_short_id: dim_long_id})
        return(dim_short_id)

    def get_dict(self):
        return(self.code_dict)

    def get_list(self):
        return({d1: list(d2.items()) for d1,d2 in self.code_dict.items()})

    def set_dict(self,arg):
        self.code_dict = arg
        
    def set_from_list(self,dimension_list):
        self.code_dict = {d1: OrderedDict(d2) for d1,d2 in dimension_list.items()}
    
class ElasticIndex():
    
    def __init__(self, db=None, es_client=None):
        """
        :param pymongo.database.Database db: MongoDB Database instance
        :param elasticsearch.Elasticsearch es_client: Instance of Elasticsearch client
        """        
        
        self.db = db or mongo_client.widukind
        self.elasticsearch_client = es_client or Elasticsearch()

    def make_index(self, provider_name, dataset_code):
        """
        :param str provider_name: Provider short name
        :param str dataset_code: Dataset ID
        """        
        
        mb_dataset = self.db[constants.COL_DATASETS].find_one({'provider': provider_name, 'datasetCode': dataset_code})
        mb_series = self.db[constants.COL_SERIES].find({'provider': provider_name, 'datasetCode': dataset_code},
                                        {'key': 1, 'dimensions': 1, 'name': 1, 'frequency': 1})

        try:    
            es_data = self.elasticsearch_client.search(index = constants.ES_INDEX, doc_type = 'datasets',
                                                       body= { "filter":
                                                               { "term":
                                                                 { "_id": provider_name + '.' + dataset_code}}})
        except Exception as err:
            logger.critical(err)
            raise
        
        if es_data['hits']['total'] == 0:
            es_dataset = {}
        else:
            es_dataset = es_data['hits']['hits'][0]['_source']

        es_dataset['name'] = mb_dataset['name']
        es_dataset['docHref'] = mb_dataset['docHref']
        es_dataset['lastUpdate'] = mb_dataset['lastUpdate']
        es_dataset['provider'] = mb_dataset['provider']
        es_dataset['datasetCode'] = mb_dataset['datasetCode']
        es_dataset['frequencies'] = mb_series.distinct('frequency')
        
        try:
            es_series = self.elasticsearch_client.search(index = constants.ES_INDEX, doc_type = 'series',
                                body= { "filter":
                                        { "term":
                                          { "provider": provider_name.lower(), "datasetCode": dataset_code.lower()}}})
        except Exception as err:
            logger.critical(err)
            raise
        
        es_series_dict = {e['_source']['key']: e['_source'] for e in es_series['hits']['hits']}

        mb_dimension_dict = {d1: {d2[0]: d2[1] for d2 in mb_dataset['dimensionList'][d1]} for d1 in mb_dataset['dimensionList']}
        # updating long names in ES index
        if 'codeList' in es_dataset:
            es_dimension_dict = {d1: {d2[0]: mb_dimension_dict[d1][d2[0]] for d2 in es_dataset['codeList'][d1]} for d1 in es_dataset['codeList']}
        else:
            es_dimension_dict = {}
            
        es_bulk = EsBulk(self.elasticsearch_client,mb_dimension_dict)
        for s in mb_series:
            mb_dim = s['dimensions']
            s['dimensions'] = {d: [mb_dim[d],mb_dimension_dict[d][mb_dim[d]]] for d in mb_dim}
        
            if s['key'] not in es_series_dict:
                es_bulk.add_to_index(provider_name,dataset_code,s)
            else:
                es_bulk.update_index(provider_name,dataset_code,s,es_series_dict[s['key']])
            dim = s['dimensions']
            for d in dim:
                if d not in es_dimension_dict:
                    es_dimension_dict[d] = {dim[d][0]:dim[d][1]}
                elif dim[d][0] not in es_dimension_dict[d]:
                    es_dimension_dict[d].update({dim[d][0]:dim[d][1]})
        es_bulk.update_database()
        es_dataset['codeList'] = {d1: [[d2[0], d2[1]] for d2 in es_dimension_dict[d1].items()] for d1 in es_dimension_dict}
        schemas.es_dataset_schema(es_dataset)
        self.elasticsearch_client.index(index = constants.ES_INDEX,
                                  doc_type='datasets',
                                  id = provider_name + '.' + dataset_code,
                                  body = es_dataset)

class EsBulk():
    def __init__(self,db,mb_dimension_dict):
        """
        :param pymongo.database.Database db: MongoDB Database instance
        :param dict mb_dimension_dict: Dimensions
        """
        self.db = db
        self.es_bulk = []
        self.mb_dimension_dict = mb_dimension_dict

    def flush_db(self):
        if len(self.es_bulk) > 200:
            self.update_database()
            self.es_bulk = []
        
    def add_to_index(self,provider_name,dataset_code,s):
        self.flush_db()
        bson = {"_op_type": 'index', 
                "_index": constants.ES_INDEX,
                "_type": 'series',
                "_id": provider_name + '.' + dataset_code + '.' + s['key'],
                'provider': provider_name,
                'key': s['key'],
                'name': s['name'],
                'datasetCode': dataset_code,
                'dimensions': s['dimensions'],
                'frequency': s['frequency']}
        schemas.es_series_schema(bson)
        self.es_bulk.append(bson)
                                     
    def update_index(self,provider_name,dataset_code,s,es_s):
        self.flush_db()
        update = False
        mb_dim = s['dimensions']
        new_bson = {"_op_type": 'update',
                "_index": constants.ES_INDEX,
                "_type": 'series',
                "_id": provider_name + '.' + dataset_code + '.' + s['key']}

        if es_s['name'] != s['name']:
            new_bson['name'] = s['name']
            update = True
        update1 = False
        for d1 in es_s['dimensions']:
            if es_s['dimensions'][d1] != mb_dim[d1]:
                es_s['dimensions'][d1] = mb_dim[d1]
                update1 = True
        if update1:
                new_bson['dimensions'] = es_s['dimensions']
                update = True
                
        if update:
            schemas.es_series_schema(bson)
            self.es_bulk.append(new_bson)
            
    def update_database(self):
        return helpers.bulk(self.db, self.es_bulk, index = constants.ES_INDEX)

