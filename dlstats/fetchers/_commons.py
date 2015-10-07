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

from .. import mongo_client
from .. import configuration

class Fetcher(object):
    """Abstract base class for fetchers"""
    def __init__(self, provider_name=None, db=None):
        self.configuration = configuration
        self.provider_name = provider_name
        self.db = db or mongo_client.widukind

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
    def upsert_a_series(self,id):
        """Upsert the series in MongoDB
        """
        raise NotImplementedError("This method from the Fetcher class must"
                                  "be implemented.")
    def upsert_dataset(self,id):
        """Upsert a dataset in MongoDB
        """
        raise NotImplementedError("This method from the Fetcher class must"
                                  "be implemented.")
    def insert_provider(self):
        """Insert the provider in MongoDB
        """
        self.provider.update_database()

#Validation and ODM
#Custom validator (only a few types are natively implemented in voluptuous)
def date_validator(value):
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
dimensions = {str: str}
attributes = {str: [str]}
attribute_list_schema = {str: [(str,str)]}
dimension_list_schema = {str: [All()]}

class DlstatsCollection(object):
    """Abstract base class for objects that are stored and indexed by dlstats
    """
    def __init__(self, db=None):
        self.db = db or mongo_client.widukind
        self.testing_mode = False
        
    def update_mongo_collection(self,collection,key,bson,log_level=logging.INFO):
        lg = logging.getLogger(__name__)
        if not self.testing_mode:
            try:
                result = self.db[collection].replace_one({key: bson[key]},bson,upsert=True)
            except:
                lg.critical(collection + '.update_database() failed for '+ bson[key]+result)
                return None
            else:
                lg.log(log_level,collection + ' ' + bson[key] + ' updated.')
                return result
        
class Provider(DlstatsCollection):
    """Abstract base class for providers
    >>> provider = Provider(name='Eurostat',website='http://ec.europa.eu/eurostat')
    >>> print(provider)
    [('name', 'Eurostat'), ('website', 'http://ec.europa.eu/eurostat')]
    >>>
    """

    def __init__(self,
                 name=None,
                 website=None,
                 testing_mode=False,
                 db=None):
        super().__init__(db=db)
        self.configuration=configuration
        self.name=name
        self.website=website
        
        self.schema = Schema({'name':
                              All(str, Length(min=1)),
                              'website':
                              All(str, Length(min=9))
                             },required=True)

        self.validate = self.schema({'name': self.name,
                                     'website': self.website
                                 })
        
    def __repr__(self):
        return pprint.pformat([(key, self.validate[key]) for key in sorted(self.validate.keys())])

    @property
    def bson(self):
        return {'name': self.name,
                'website': self.website}

    def update_database(self,mongo_id=None,key=None):
        return self.update_mongo_collection('providers','name',self.bson)
                
class Category(DlstatsCollection):
    """Abstract base class for categories
    >>> from datetime import datetime
    >>> category = Category(provider='Test provider',name='GDP',
    ...                 categoryCode='nama_gdp',
    ...                 children=[bson.objectid.ObjectId.from_datetime(datetime(2014,12,3))],
    ...                 docHref='http://www.perdu.com',
    ...                 lastUpdate=datetime(2014,12,2),
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
                 db=None):
        super().__init__(db=db)
        self.configuration = configuration
        self.provider=provider
        self.name=name
        self.docHref=docHref
        self.children=children
        self.categoryCode=categoryCode
        self.lastUpdate=lastUpdate
        self.exposed=exposed
        self.configuration=configuration
        self.schema = Schema({'name':
                              All(str, Length(min=1)),
                              'provider':
                              All(str, Length(min=1)),
                              'children':
                              Any([None,typecheck(bson.objectid.ObjectId)]),
                              Optional('docHref'):
                              Any(None,str),
                              Optional('lastUpdate'):
                              Any(None,typecheck(datetime)),
                              'categoryCode':
                              All(str, Length(min=1)),
                              'exposed':
                              typecheck(bool)
                          }, required=True
)
        self.validate = self.schema({'provider': self.provider,
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
        return self.update_mongo_collection('categories','categoryCode',self.bson,log_level=logging.DEBUG)

class Dataset(DlstatsCollection):
    """Abstract base class for datasets
    >>> from datetime import datetime
    >>> dataset = Dataset('Test provider','nama_gdp_fr')
    >>> print(dataset)
    [('provider_name', 'Test provider'), ('dataset_code', 'nama_gdp_fr')]
    """
    def __init__(self, provider_name, dataset_code, db=None):
        super().__init__(db=db)
        self.provider_name = provider_name
        self.dataset_code = dataset_code
        self.name = None
        self.doc_href = None
        self.last_update = None
        self.dimension_list = CodeDict()
        self.attribute_list = CodeDict()
        self.load_previous_version(provider_name,dataset_code)
        self.notes = ''
        self.schema = Schema({'name':
                              All(str, Length(min=1)),
                              'provider':
                              All(str, Length(min=1)),
                              'datasetCode':
                              All(str, Length(min=1)),
                              'docHref':
                              Any(None,str),
                              'lastUpdate':
                              typecheck(datetime),
                              'dimensionList':
                              dimension_list_schema,
                              'attributeList':
                              Any(None,attribute_list_schema),
                              Optional('notes'):
                              str
                             },required=True)
        self.series = Series(self.provider_name,
                             self.dataset_code,
                             self.last_update,
                             db=self.db)

    def __repr__(self):
        return pprint.pformat([('provider_name', self.provider_name),
                               ('dataset_code', self.dataset_code)])
    @property
    def bson(self):
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
        dataset = self.db.datasets.find_one({'provider': provider_name,
                                             'datasetCode': dataset_code})
        if dataset:
            # convert to dict of dict
            self.dimension_list.set_from_list(dataset['dimensionList'])
            self.attribute_list.set_from_list(dataset['attributeList'])
        
    def update_database(self):
        self.series.process_series()
        self.schema(self.bson)
        return self.update_mongo_collection('datasets','datasetCode',self.bson)

class Series(DlstatsCollection):
    """Abstract base class for time series
    >>> dataset = Dataset(provider_name='Test provider',
    ...                   dataset_code='nama_gdp_fr')
    >>> dataset.last_update = datetime(2015,8,15)
    >>> series = Series(dataset.provider_name,
    ...                 dataset.dataset_code,
    ...                 dataset.last_update)
    >>> print(series)
    [('provider_name', 'Test provider'),
     ('dataset_code', 'nama_gdp_fr'),
     ('last_update', datetime.datetime(2015, 8, 15, 0, 0))]
    """

    def __init__(self, provider_name, dataset_code, last_update,bulk_size=1000, db=None):
        super().__init__(db=db)
        self.provider_name = provider_name
        self.dataset_code = dataset_code
        self.last_update = last_update
        self.bulk_size = bulk_size
        self.ser_list = []
        self.schema = Schema({'name':
                              All(str, Length(min=1)),
                              'provider':
                              All(str, Length(min=1)),
                              'key':
                              All(str, Length(min=1)),
                              'datasetCode':
                              All(str, Length(min=1)),
                              'startDate':
                              int,
                              'endDate':
                              int,
                              'values':
                              [Any(str)],
                              'releaseDates':
                              [date_validator],
                              'attributes':
                              Any({},attributes),
                              Optional('revisions'):
                              Any(None,revision_schema),
                              'dimensions':
                              dimensions,
                              'frequency': 
                              All(str, Length(min=1)),
                              Optional('notes'):
                              str
                             },required=True)

    def __repr__(self):
        return pprint.pformat([('provider_name', self.provider_name),
                               ('dataset_code', self.dataset_code),
                               ('last_update', self.last_update)])
        
    def set_data_iterator(self,data_iterator):
        self.data_iterator = data_iterator

    def process_series(self):
        count = 0
        while True:
            try:
                self.ser_list.append( next(self.data_iterator))
            except StopIteration:
                break
            count += 1
            if count > self.bulk_size:
                self.update_series_list()
                count = 0
        if count > 0:
            self.update_series_list()
        
    def update_series_list(self):
        if self.testing_mode == False:
            keys = [s['key'] for s in self.ser_list]
            old_series = self.db.series.find({'provider': self.provider_name,
                                              'datasetCode': self.dataset_code,
                                              'key': {'$in': keys}})
            old_series = {s['key']:s for s in old_series}
            bulk = self.db.series.initialize_ordered_bulk_op()
            for bson in self.ser_list:
                if bson['key'] not in old_series:
                    self.schema(bson)
                    bulk.insert(bson)
                else:
                    release_date = bson['releaseDates'][0]
                    old_bson = old_series[bson['key']]
                    if 'revision'  in old_bson:
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
                    bulk.find({'_id': old_bson['_id']}).update({'$set': bson})
            result = bulk.execute()
            self.ser_list = []
            
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

    def make_index(self,provider_name,dataset_code):
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
