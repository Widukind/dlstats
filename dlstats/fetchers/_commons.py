#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
.. module:: _commons
    :synopsis: Code imported by the different fetchers
"""
import pymongo
import datetime
import pandas
from voluptuous import Required, All, Length, Range, Schema, Invalid, Object, Optional, Any, Extra
from datetime import datetime
import logging
import bson
import pprint
from elasticsearch import Elasticsearch, helpers
from collections import defaultdict, OrderedDict

from dlstats import mongo_client
from dlstats import configuration
from dlstats import constants

class Fetcher(object):
    """Abstract base class for all fetchers"""
    
    def __init__(self, 
                 provider_name=None, 
                 db=None, 
                 es_client=None):
        """
        :param provider_name: :class:`str` - Provider Name
        :param db: Instance of :class:`~pymongo.database.Database`        
        :param es_client: Instance of :class:`~elasticsearch.Elasticsearch` 

        Raises :class:`AttributeError` or :class:`TypeError`
        """        
        if not provider_name:
            raise ValueError("provider_name is required")

        self.provider_name = provider_name
        self.db = db or mongo_client.widukind
        self.es_client = es_client or Elasticsearch()
        self.provider = None

    def upsert_categories(self):
        """Upsert the categories in MongoDB
        """        
        raise NotImplementedError("This method from the Fetcher class must"
                                  "be implemented.")
    
    def upsert_series(self):
        """Upsert all the series in MongoDB
        """        
        raise NotImplementedError("This method from the Fetcher class must"
                                  "be implemented.")

    #TODO: not used function ?    
    def upsert_a_series(self, id):
        """Upsert the series in MongoDB
        
        :param id: :class:`str` - ID of :class:`Series`
        """        
        raise NotImplementedError("This method from the Fetcher class must"
                                  "be implemented.")
    
    def upsert_dataset(self, dataset_code):
        """Upsert a dataset in MongoDB
        
        :param dataset_code: :class:`str` - ID of :class:`Dataset`
        """        
        raise NotImplementedError("This method from the Fetcher class must"
                                  "be implemented.")
    
    #TODO: not used function ?    
    def insert_provider(self):
        """Insert the provider in MongoDB
        """        
        self.provider.update_database()
        
    def update_metas(self, dataset_code):
        """Update Meta datas - (TODO: store ElasticSearch or MongoDB)
        
        :param dataset_code: :class:`str` - ID of :class:`Dataset`
        """        
        es = ElasticIndex(db=self.db, es_client=self.es_client)
        es.make_index(self.provider_name, dataset_code)
        

def date_validator(value):
    """Custom validator (only a few types are natively implemented in voluptuous)
    """
    if isinstance(value, datetime):
        return value
    else:
        raise Invalid('Input date was not of type datetime')

def typecheck(type, msg=None):
    """Coerce a value to a type.

    If the type constructor throws a ValueError, the value will be marked as
    Invalid.
    """
    def validator(value):
        if not isinstance(value,type):
            raise Invalid(msg or ('expected %s' % type.__name__))
        else:
            return value
    return validator


#Schema definition in voluptuous
revision_schema = {str: [{Required('value'): str,
                          Required('releaseDates'): date_validator}]}

class DlstatsCollection(object):
    """Abstract base class for objects that are stored and indexed by dlstats
    """
    
    def __init__(self, fetcher=None):
        """
        :param fetcher: Instance of :class:`dlstats.fetchers._commons.Fetcher`
        """
        
        if not fetcher:
            raise ValueError("fetcher is required")

        if not isinstance(fetcher, Fetcher):
            raise TypeError("Bad type for fetcher")
        
        self.fetcher = fetcher
        self.testing_mode = False
        
    def update_mongo_collection(self, collection, key, bson, log_level=logging.INFO):
        lgr = logging.getLogger(__name__)
        """
        lgr.setLevel(log_level)
        fh = logging.FileHandler(configuration['General']['logging_directory']+'/dlstats.log')
        fh.setLevel(log_level)
        frmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(frmt)
        lgr.addHandler(fh)
        """
        if not self.testing_mode:
            try:
                result = self.fetcher.db[collection].replace_one({key: bson[key]}, bson, upsert=True)
            except:
                lgr.critical(collection + '.update_database() failed for '+ bson[key]+result)
                return None
            else:
                lgr.log(log_level,collection + ' ' + bson[key] + ' updated.')
                return result
        
class Provider(DlstatsCollection):
    """Abstract base class for providers
    
    Inherit from :class:`DlstatsCollection`
    
    >>> provider = Provider(name='Eurostat',website='http://ec.europa.eu/eurostat')
    >>> print(provider)
    [('name', 'Eurostat'), ('website', 'http://ec.europa.eu/eurostat')]
    """

    def __init__(self,
                 name=None,
                 website=None,
                 testing_mode=False,
                 fetcher=None):
        """
        :param name: :class:`str` - Provider Name
        :param website: :class:`str` - Provider Web Site
        :param testing_mode: :class:`bool` - Testing Mode
        :param fetcher: Instance of :class:`dlstats.fetchers._commons.Fetcher`
        """        
        super().__init__(fetcher=fetcher)
        self.name = name
        self.website = website
        
        self.schema = Schema({
            'name': All(str, Length(min=1)),
            'website': All(str, Length(min=9))
        },required=True)

        self.validate = self.schema({
            'name': self.name,
            'website': self.website
        })
        
    def __repr__(self):
        return pprint.pformat([(key, self.validate[key]) for key in sorted(self.validate.keys())])

    @property
    def bson(self):
        return {'name': self.name,
                'website': self.website}

    def update_database(self):
        self.schema(self.bson)
        return self.update_mongo_collection(constants.COL_PROVIDERS, 
                                            'name', 
                                            self.bson)
                
class Category(DlstatsCollection):
    """Abstract base class for categories
    
    >>> from datetime import datetime
    >>> f = Fetcher(provider_name='Test provider')
    >>> category = Category(provider='Test provider',name='GDP',
    ...                 categoryCode='nama_gdp',
    ...                 children=[bson.objectid.ObjectId.from_datetime(datetime(2014,12,3))],
    ...                 docHref='http://www.perdu.com',
    ...                 lastUpdate=datetime(2014,12,2),
    ...                 fetcher=f,
    ...                 exposed=True)
    >>> print(category)
    [('categoryCode', 'nama_gdp'),
     ('children', [ObjectId('547e52800000000000000000')]),
     ('docHref', 'http://www.perdu.com'),
     ('exposed', True),
     ('lastUpdate', datetime.datetime(2014, 12, 2, 0, 0)),
     ('name', 'GDP'),
     ('provider', 'Test provider')]
    """
    
    def __init__(self,
                 provider=None,
                 name=None,
                 docHref=None,
                 children=[None],
                 categoryCode=None,
                 lastUpdate=None,
                 exposed=False,
                 fetcher=None):
        """
        :param provider: :class:`str` - Provider name
        :param name: :class:`str` - Category short name
        :param docHref: (Optional) :class:`str` - Category - web link
        :param children: Array of :class:`bson.objectid.ObjectId` or empty list        
        :param categoryCode: :class:`str` - Unique Category Code
        :param lastUpdate: (Optional) :class:`datetime.datetime` - Last updated date
        :param exposed: :class:`bool` - Exposed ?
        :param fetcher: Instance of :class:`dlstats.fetchers._commons.Fetcher`
        """        
        super().__init__(fetcher=fetcher)
        self.provider = provider
        self.name = name
        self.docHref = docHref
        self.children = children
        self.categoryCode = categoryCode
        self.lastUpdate = lastUpdate
        self.exposed = exposed
        
        self.schema = Schema({
            'name': All(str, Length(min=1)),
            'provider': All(str, Length(min=1)),
            'children': Any([None,typecheck(bson.objectid.ObjectId)]), 
            Optional('docHref'): Any(None,str),
            Optional('lastUpdate'): Any(None,typecheck(datetime)),
            'categoryCode': All(str, Length(min=1)),
            'exposed': typecheck(bool)
          }, required=True
        )
        
        self.validate = self.schema({
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
        self.schema(self.bson)
        return self.update_mongo_collection(constants.COL_CATEGORIES, 
                                            'categoryCode', 
                                            self.bson, 
                                            log_level=logging.DEBUG)

class Dataset(DlstatsCollection):
    """Abstract base class for datasets
    
    >>> from datetime import datetime
    >>> dataset = Dataset('Test provider','nama_gdp_fr')
    >>> print(dataset)
    [('provider_name', 'Test provider'), ('dataset_code', 'nama_gdp_fr')]
    """
    
    def __init__(self, 
                 provider_name=None,
                 dataset_code=None, 
                 name=None,
                 doc_href=None,
                 last_update=None,
                 fetcher=None, 
                 is_load_previous_version=True):
        """
        :param provider: :class:`str` - Provider name
        :param dataset_code: :class:`str` - Dataset code
        :param fetcher: Instance of :class:`dlstats.fetchers._commons.Fetcher`
        """        
        super().__init__(fetcher=fetcher)
        #TODO: modifier attribut provider_name en provider pour cohérence schéma
        self.provider_name = provider_name
        #TODO: modifier attribut dataset_code en datasetCode pour cohérence schéma
        self.dataset_code = dataset_code
        self.name = name
        #TODO: modifier attribut doc_href en docHref pour cohérence schéma
        self.doc_href = doc_href
        #TODO: modifier attribut last_update en lastUpdate pour cohérence schéma
        self.last_update = last_update
        #TODO: modifier attribut dimension_list en dimensionList pour cohérence schéma
        self.dimension_list = CodeDict()
        #TODO: modifier attribut attribute_list en attributeList pour cohérence schéma
        self.attribute_list = CodeDict()
        
        if is_load_previous_version:
            self.load_previous_version(provider_name, dataset_code)
            
        self.notes = ''
        
        self.schema = Schema({
            'name': All(str, Length(min=1)),
            'provider': All(str, Length(min=1)),
            'datasetCode': All(str, Length(min=1)),
            'docHref': Any(None,str),
            'lastUpdate': typecheck(datetime),
            'dimensionList': {str: [All()]},
            'attributeList': Any(None, {str: [(str,str)]}),
            Optional('notes'): str
         },required=True)

        self.series = Series(self.provider_name,
                             self.dataset_code,
                             self.last_update,
                             fetcher=self.fetcher)

    def __repr__(self):
        return pprint.pformat([('provider_name', self.provider_name),
                               ('dataset_code', self.dataset_code)])
    @property
    def bson(self):
        #FIXME: mettre plutôt notes à None ou ''
        if self.notes:
            return {'provider': self.provider_name,
                    'name': self.name,
                    'datasetCode': self.dataset_code,
                    'dimensionList': self.dimension_list.get_list(),
                    'attributeList': self.attribute_list.get_list(),
                    'docHref': self.doc_href,
                    'lastUpdate': self.last_update,
                    'notes': self.notes}
        else:
            return {'provider': self.provider_name,
                    'name': self.name,
                    'datasetCode': self.dataset_code,
                    'dimensionList': self.dimension_list.get_list(),
                    'attributeList': self.attribute_list.get_list(),
                    'docHref': self.doc_href,
                    'lastUpdate': self.last_update}

    def load_previous_version(self,provider_name,dataset_code):
        dataset = self.fetcher.db.datasets.find_one({'provider': provider_name,
                                             'datasetCode': dataset_code})
        if dataset:
            # convert to dict of dict
            self.dimension_list.set_from_list(dataset['dimensionList'])
            self.attribute_list.set_from_list(dataset['attributeList'])
        
    def update_database(self):
        self.series.process_series()        
        self.schema(self.bson)
        return self.update_mongo_collection(constants.COL_DATASETS,
                                            'datasetCode', self.bson)

class SerieEntry(DlstatsCollection):
    """Abstract base class for one time serie
    """
    
    __slots__ = ("provider", "datasetCode", "key", "name", "startDate", 
                 "endDate", "values", "releaseDates", "attributes",
                 "revisions", "dimensions", "frequency", "notes",
                 "fetcher", "schema")
    
    def __init__(self, 
                 provider=None, 
                 datasetCode=None, 
                 #TODO: unused ? : #lastUpdate=None,
                 key=None,
                 name=None,
                 startDate=0,
                 endDate=0,
                 values=[],
                 releaseDates=[],
                 attributes={},
                 revisions={},
                 dimensions={},
                 frequency=None,
                 notes=None,   
                 fetcher=None):
        super().__init__(fetcher=fetcher)
        self.provider = provider
        self.key = key
        self.name = name
        self.datasetCode = datasetCode
        #TODO: unused ? : self.lastUpdate = lastUpdate
        self.startDate = startDate
        self.endDate = endDate
        self.values = values
        self.releaseDates = releaseDates
        self.attributes = attributes
        self.revisions = revisions
        self.dimensions = dimensions
        self.frequency = frequency
        self.notes = notes
        
        # schema for on serie
        self.schema = Schema({
            'name': All(str, Length(min=1)),
            'provider': All(str, Length(min=1)),
            'key': All(str, Length(min=1)),
            'datasetCode': All(str, Length(min=1)),
            'startDate': int,
            'endDate': int,
            'values': [Any(str)],
            'releaseDates': [date_validator],
            'attributes': Any({}, {str: [str]}),
            Optional('revisions'): Any(None, revision_schema),
            'dimensions': {str: str},
            'frequency': All(str, Length(min=1)),
            Optional('notes'): Any(None, str)
        },required=True)

    def populate(self, bson):
        """Populate current object with bson entry from MongoDB"""
        self.provider = bson.get('provider')
        self.key = bson.get('key')
        self.name = bson.get('name')
        self.datasetCode = bson.get('datasetCode')
        #TODO: unused ? : self.lastUpdate = bson.get('lastUpdate')
        self.startDate = bson.get('startDate', 0)
        self.endDate = bson.get('endDate', 0)
        self.values = bson.get('values', [])
        self.releaseDates = bson.get('releaseDates', [])
        self.attributes = bson.get('attributes', {})
        self.revisions = bson.get('revisions', {})
        self.dimensions = bson.get('dimensions', {})
        self.frequency = bson.get('frequency', {})
        self.notes = bson.get('notes')

    @property
    def bson(self):
        return {'provider': self.provider,
                'key': self.key,
                'name': self.name,
                'datasetCode': self.datasetCode,
                #'lastUpdate': self.lastUpdate,
                'startDate': self.startDate,
                'endDate': self.endDate,
                'values': self.values,
                'releaseDates': self.releaseDates,
                'attributes': self.attributes,
                'revisions': self.revisions,
                'dimensions': self.dimensions,
                'frequency': self.frequency,
                'notes': self.notes}

    def update_serie(self, is_bulk=False):
        
        old_bson = self.fetcher.db[constants.COL_SERIES].find_one({
                                        'provider': self.provider,
                                        'datasetCode': self.datasetCode,
                                        'key': self.key})
                
        col = self.fetcher.db[constants.COL_SERIES]
        
        bson = self.bson
        
        if not old_bson:
            self.schema(bson)
            if is_bulk:
                return bson
            return col.insert(bson)
        else:
            release_date = bson['releaseDates'][0]
            
            if 'revision' in old_bson:
                bson['revisions'] = old_bson['revisions']
            
            start_date = bson['startDate']
            old_start_date = old_bson['startDate']
            
            if start_date < old_start_date:
                # update all positions
                if 'revisions' in bson:
                    offset = old_start_date - start_date
                    ikeys = [int(k) for k in bson['revisions']]
                    for p in sorted(ikeys,reverse=True):
                        bson['revisions'][str(p+offset)] = bson['revisions'][str(p)]
                         
            elif start_date > old_start_date:
                # previous, longer, series is kept
                # fill beginning with na
                for p in range(start_date-old_start_date):
                    # insert in front of the values, releaseDates and attributes
                    bson['values'].insert(0,'na')
                    bson['releaseDates'].insert(0,release_date)
                    for a in bson['attributes']:
                        bson['attributes'][a].insert(0,"") 
                
                bson['startDate'] = old_bson['startDate']
                
            for position,values in enumerate(zip(old_bson['values'],bson['values'])):
                
                if values[0] != values[1]:
                    if 'revisions' not in bson:
                        bson['revisions'] = defaultdict(list)
                    bson['revisions'][str(position)].append(
                        {'value':values[0],
                         'releaseDates':old_bson['releaseDates'][position]})
                    
            if bson['endDate'] < old_bson['endDate']:
                for p in range(old_bson['endDate']-bson['endDate']):
                    bson['values'].append('na')
                    bson['releaseDates'].append(release_date)
                    for a in bson['attributes']:
                        bson['attributes'][a].append("")
                        
            self.schema(bson)
            if is_bulk:
                #FIXME: doit retourné un SerieEntry mis à jour ?
                return bson
            return col.find_one_and_update({'_id': old_bson['_id']}, {'$set': bson})

class Series(DlstatsCollection):
    """Abstract base class for time series"""
    
    def __init__(self, 
                 provider=None, 
                 datasetCode=None, 
                 lastUpdate=None, 
                 bulk_size=1000, 
                 fetcher=None):
        """        
        :param provider: :class:`str` - Provider name
        :param datasetCode: :class:`str` - Dataset code
        :param lastUpdate: :class:`datetime.datetime` - Last updated date
        :param bulk_size: :class:`int` - Batch size for mongo bulk update
        :param fetcher: Instance of :class:`dlstats.fetchers._commons.Fetcher`
        """        
        super().__init__(fetcher=fetcher)
        self.provider = provider
        self.datasetCode = datasetCode
        self.lastUpdate = lastUpdate
        self.bulk_size = bulk_size
        self.ser_list = []
    
    def __repr__(self):
        return pprint.pformat([('provider', self.provider),
                               ('datasetCode', self.datasetCode),
                               ('lastUpdate', self.lastUpdate)])

    def process_series(self):
        count = 0
        while True:
            try:
                # append result from __next__ method in fetchers
                # one iteration by serie
                bson = next(self.data_iterator)
                serie = SerieEntry(fetcher=self.fetcher)
                serie.populate(bson)
                self.ser_list.append(serie)
            except StopIteration:
                break
            count += 1
            if count > self.bulk_size:
                self.update_series_list()
                count = 0
        if count > 0:
            self.update_series_list()

    def update_series_list(self):

        keys = [s.key for s in self.ser_list]

        old_series = self.fetcher.db[constants.COL_SERIES].find({
                                        'provider': self.provider,
                                        'datasetCode': self.datasetCode,
                                        'key': {'$in': keys}})

        old_series = {s['key']:s for s in old_series}
        
        bulk = self.fetcher.db[constants.COL_SERIES].initialize_ordered_bulk_op()

        for serie in self.ser_list:                    
            if not serie.key in old_series:
                bson = serie.update_serie(is_bulk=True)                                
                bulk.insert(bson)
            else:
                old_bson = old_series[serie.key]
                bson = serie.update_serie(is_bulk=True)                
                bulk.find({'_id': old_bson['_id']}).update({'$set': bson})
        
        result = bulk.execute()         
        self.ser_list = []
        return result
            
class CodeDict():
    """Class for handling code lists
    
    >>> code_list = {'Country': {'FR': 'France'}}
    >>> print(code_list)
    {'Country': {'FR': 'France'}}
    """    
    
    def __init__(self):
        # code_dict is a dict of OrderedDict
        self.code_dict = {}
        self.schema = Schema({Extra: dict})
        self.schema(self.code_dict)
        
    def update(self,arg):
        self.schema(arg.code_dict)
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
        
        self.db = db or mongo_client.widukind
        self.elasticsearch_client = es_client or Elasticsearch()

    def make_index(self, provider_name, dataset_code):
        
        mb_dataset = self.db.datasets.find_one({'provider': provider_name, 'datasetCode': dataset_code})
        mb_series = self.db.series.find({'provider': provider_name, 'datasetCode': dataset_code},
                                        {'key': 1, 'dimensions': 1, 'name': 1, 'frequency': 1})
    
        es_data = self.elasticsearch_client.search(index = 'widukind', doc_type = 'datasets',
                                                   body= { "filter":
                                                           { "term":
                                                             { "_id": provider_name + '.' + dataset_code}}})
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
        
        es_series = self.elasticsearch_client.search(index = 'widukind', doc_type = 'series',
                            body= { "filter":
                                    { "term":
                                      { "provider": provider_name.lower(), "datasetCode": dataset_code.lower()}}})
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
        self.elasticsearch_client.index(index = 'widukind',
                                  doc_type='datasets',
                                  id = provider_name + '.' + dataset_code,
                                  body = es_dataset)

class EsBulk():
    def __init__(self,db,mb_dimension_dict):
        self.db = db
        self.es_bulk = []
        self.mb_dimension_dict = mb_dimension_dict
        
    def add_to_index(self,provider_name,dataset_code,s):
        bson = {"_op_type": 'index', 
                "_index": 'widukind',
                "_type": 'series',
                "_id": provider_name + '.' + dataset_code + '.' + s['key'],
                'provider': provider_name,
                'key': s['key'],
                'name': s['name'],
                'datasetCode': dataset_code,
                'dimensions': s['dimensions'],
                'frequency': s['frequency']}
        self.es_bulk.append(bson)
                                     
    def update_index(self,provider_name,dataset_code,s,es_s):
        update = False
        mb_dim = s['dimensions']
        new_bson = {"_op_type": 'update',
                "_index": 'widukind',
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
            self.es_bulk.append(new_bson)
            
    def update_database(self):
        res_es = helpers.bulk(self.db, self.es_bulk, index = 'widukind')

if __name__ == "__main__":
    import doctest
    doctest.testmod()
