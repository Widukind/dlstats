#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
.. module:: _skeleton
    :synopsis: Module containing an abstract base class for all the fetchers
"""
import pymongo
import datetime
import pandas
from voluptuous import Required, All, Length, Range, Schema, Invalid, Object, Optional, Any
from dlstats import configuration
#from dlstats.misc_func import dictionary_union
from ..misc_func import dictionary_union
from datetime import datetime
import logging
import bson
import pprint
from collections import defaultdict
import elasticsearch
from .. import mongo_client

class Skeleton(object):
    """Abstract base class for fetchers"""
    def __init__(self, provider_name=None):
        self.configuration = configuration
        self.provider_name = provider_name
        self.db = mongo_client.widukind
        self.elasticsearch = elasticsearch.Elasticsearch(
            host = self.configuration['ElasticSearch']['host'])
    def upsert_categories(self,id):
        """Upsert the categories in MongoDB
        """
        raise NotImplementedError("This method from the Skeleton class must"
                                  "be implemented.")
    def upsert_series(self):
        """Upsert all the series in MongoDB
        """
        raise NotImplementedError("This method from the Skeleton class must"
                                  "be implemented.")
    def upsert_a_series(self,id):
        """Upsert the series in MongoDB
        """
        raise NotImplementedError("This method from the Skeleton class must"
                                  "be implemented.")
    def upsert_dataset(self,id):
        """Upsert a dataset in MongoDB
        """
        raise NotImplementedError("This method from the Skeleton class must"
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
revision_schema = [{Required('value'): str, Required('position'): int,
             Required('releaseDates'): [date_validator]}]
dimensions = {str: str}
attributes = {str: [str]}
attribute_list_schema = {str: [(str,str)]}
dimension_list_schema = {str: [All()]}

class Provider(object):
    """Abstract base class for providers
    >>> provider = Provider(name='Eurostat',website='http://ec.europa.eu/eurostat')
    >>> print(provider)
    [('name', 'Eurostat'), ('website', 'http://ec.europa.eu/eurostat')]
    >>>
    """

    def __init__(self,
                 name=None,
                 website=None):
        self.configuration=configuration
        self.db = mongo_client.widukind
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
        return self.db.providers.update({'name':self.bson['name']},self.bson,upsert=True)

class Series(object):
    """Abstract base class for time series
    >>> from datetime import datetime
    >>> series = Series(provider='Test provider',name='GDP in France',
    ...                 key='GDP_FR',datasetCode='nama_gdp_fr',
    ...                 values = ['2700', '2720', '2740', '2760'],
    ...                 releaseDates = [datetime(2013,11,28),datetime(2014,12,28),datetime(2015,1,28),datetime(2015,2,28)],
    ...                 period_index = pandas.period_range('1/1999', periods=72, freq='Q'),
    ...                 attributes = {'OBS_VALUE': ['p']},
    ...                 revisions = [{'value': '2710', 'position':2,
    ...                 'releaseDates' : [datetime(2014,11,28)]}],
    ...                 dimensions = [{'Seasonal adjustment':'wda'}])
    >>> print(series)
    [('attributes', {'OBS_VALUE': ['p']}),
     ('datasetCode', 'nama_gdp_fr'),
     ('dimensions', {'Seasonal adjustment': 'wda'}),
     ('key', 'GDP_FR'),
     ('name', 'GDP in France'),
     ('period_index',
      <class 'pandas.tseries.period.PeriodIndex'>
    [1999Q1, ..., 2016Q4]
    Length: 72, Freq: Q-DEC),
     ('provider', 'Test provider'),
     ('releaseDates',
      [datetime.datetime(2013, 11, 28, 0, 0),
       datetime.datetime(2014, 12, 28, 0, 0),
       datetime.datetime(2015, 1, 28, 0, 0),
       datetime.datetime(2015, 2, 28, 0, 0)]),
     ('revisions',
      [{'position': 2,
        'releaseDates': [datetime.datetime(2014, 11, 28, 0, 0)],
        'value': '2710'}]),
     ('values', ['2700', '2720', '2740', '2760'])]
    >>>
    """

    def __init__(self,
                 provider=None,
                 name=None,
                 key=None,
                 datasetCode=None,
                 period_index=None, 
                 releaseDates=None, 
                 values=None,
                 attributes=None,
                 revisions=None,
                 frequency=None,
                 dimensions=None):
        self.configuration=configuration
        self.db = mongo_client.widukind
        self.collection = self.db.series
        self.provider = provider.upper()
        self.name=name
        self.key=key.upper()
        #SDMX equivalent concept: flowRef
        self.datasetCode=datasetCode
        self.period_index=period_index
        self.frequency=frequency.upper()
        self.values=values
        self.releaseDates=releaseDates
        self.attributes=attributes
        self.revisions=revisions
        self.dimensions=dimensions

        self.schema = Schema({'name':
                              All(str, Length(min=1)),
                              'provider':
                              All(str, Length(min=1)),
                              'key':
                              All(str, Length(min=1)),
                              'datasetCode':
                              All(str, Length(min=1)),
                              'period_index':
                              typecheck(pandas.tseries.period.PeriodIndex),
                              'values':
                              [str],
                              'releaseDates':
                              [date_validator],
                              'attributes':
                              Any({},attributes),
                              'revisions':
                              Any(None,revision_schema),
                              'dimensions':
                              dimensions
                               },required=True)

        self.validate = self.schema({'provider': self.provider,
                                     'name': self.name,
                                     'key': self.key,
                                     'datasetCode': self.datasetCode,
                                     'period_index': self.period_index,
                                     'values': self.values,
                                     'attributes': self.attributes,
                                     'dimensions': self.dimensions,
                                     'revisions': self.revisions,
                                     'releaseDates': self.releaseDates
                                 })
        
#        _to_be_validated = {'provider': self.provider,
#                            'name': self.name,
#                            'key': self.key,
#                            'datasetCode': self.datasetCode,
#                            'period_index': self.period_index,
#                            'values': self.values,
#                            'attributes': self.attributes,
#                            'dimensions': self.dimensions,
#                            'revisions': self.revisions,
#                            'releaseDates': self.releaseDates
#                           }

#        for optional_key in ['attributes','revisions']:
#            if _to_be_validated[optional_key] is None:
#                _to_be_validated.pop(optional_key)
#        self.validate = self.schema(_to_be_validated)

    @classmethod
    def from_index(cls,mongo_id):
        return cls.from_bson(self.collection.find(mongo_id))

    @classmethod
    def from_bson(cls,bson):
        period_index = pandas.period_range(bson['startDate'],
                                           periods=bson['numberOfPeriods'],
                                           freq=bson['frequency'])
        return cls(provider=bson['provider'],
                   name=bson['name'],
                   key=bson['key'],
                   dataset_code=bson['datasetCode'],
                   values=bson['values'],
                   releaseDates=bson['releaseDates'],
                   attributes=bson['attributes'],
                   revisions=bson['revisions'],
                   dimensions=bson['dimensions'],
                   period_index=period_index,
                  )

    def __repr__(self):
        return pprint.pformat([(key, self.validate[key])
                               for key in sorted(self.validate.keys())])

    @property
    def bson(self):
        return {'provider': self.provider,
                'name': self.name,
                'key': self.key,
                'datasetCode': self.datasetCode,
                'startDate': self.period_index[0].to_timestamp(),
                'endDate': self.period_index[-1].to_timestamp(),
                'values': self.values,
                'releaseDates': self.releaseDates,
                'attributes': self.attributes,
                'dimensions': self.dimensions,
                'numberOfPeriods': len(self.period_index),
                'revisions': self.revisions,
                'frequency': self.frequency}

    def update_database(self,mongo_id=None,key=None):
        old_bson = self.db.series.find_one({'key': self.bson['key']})

        if old_bson == None:
            return self.collection.insert(self.bson)
        else:
            position = 0
            self.revisions = old_bson['revisions']
            old_start_period = pandas.Period(
                old_bson['startDate'],freq=old_bson['frequency'])
            start_period = pandas.Period(
                self.bson['startDate'],freq=self.bson['frequency'])
            if start_period > old_start_period:
            # previous, longer, series is kept
                offset = start_period - old_start_period
                self.bson['numberOfPeriods'] += offset
                self.bson['startDate'] = old_bson['startDate']
                for values in zip(old_bson['values'][offset:],self.values):
                    if values[0] != values[1]:
                        self.revisions.append(
                            {'value':values[0],
                             'position': offset+position,
                             'releaseDates':
                             old_bson['releaseDates'][offset+position]})
                    position += 1
            else:
            # zero or more data are added at the beginning of the series
                offset = old_start_period - start_period
                for values in zip(old_bson['values'],self.values[offset:]):
                    if values[0] != values[1]:
                        self.revisions.append(
                            {'value':values[0],
                             'position': offset+position,
                             'releaseDates':
                             old_bson['releaseDates'][position]})
                    position += 1
                                              
            self.bson['revisions'] = self.revisions
            self.collection.find({'_id': old_bson['_id']}).upsert().update(
                {'$set': self.bson})
        return old_bson['_id']

class ESSeriesIndex(object):
    def __init__(self,series,codeDict):
        self.key = series.key
        self.provider = series.provider
        self.name = series.name
        self.datasetCode = series.datasetCode
        self.dimensions = {}

        for key, value in series.dimensions.items():
            if len(codeDict):
                self.dimensions[key] = [value, codeDict[key][value]]
            else:
                self.dimensions[key] = [value]

    @property
    def bson(self):
        return({'provider': self.provider,
                'key': self.key,
                'name': self.name,
                'datasetCode': self.datasetCode,
                'dimensions': self.dimensions
                })

class BulkSeries(object):
    def __init__(self,datasetCode,dimensionList={},attributeList={},data=[]):
        self.db = mongo_client.widukind
        self.data = data
        self.datasetCode = datasetCode.upper()
        self.dimensionList = dimensionList
        dimensionList.update(attributeList)
        # check whether there is a label for the dimension codes
        if len(dimensionList):
            if len(list(dimensionList.items())[0][1][0]) == 2:
                self.codeDict =  {d: {v[0]: v[1]
                                      for v in dimensionList[d]}
                                  for d in dimensionList}
            else:
                self.codeDict =  {d: {v: None
                                      for v in dimensionList[d]}
                                  for d in dimensionList}
        else:
            self.codeDict = {}

    def __iter__(self):
        return iter(self.data)
    
    def append(self,series):
        self.data.append(series)

    class EffectiveDimensionList(object):
        def __init__(self,codeDict):
            self.codeDict = codeDict
            # mode == 1: a single code; mode == 2: a short and a long code
            if len(self.codeDict):
                self.mode = 2
            else:
                self.mode = 1
            self.effective_dimension_dict = {}

        def update(self,dimensions):
            for d in dimensions:
                if d in self.effective_dimension_dict:
                    if not dimensions[d] in self.effective_dimension_dict[d]:
                        if self.mode == 2:
                            self.effective_dimension_dict[d].update({dimensions[d]: self.codeDict[d][dimensions[d]]})
                        else:
                            self.effective_dimension_dict[d].update([dimensions[d]])
                            
                else:
                    if self.mode == 2:
                        self.effective_dimension_dict[d] = {dimensions[d]: self.codeDict[d][dimensions[d]]}
                    else:
                        self.effective_dimension_dict[d] = set([dimensions[d]])
                        
        def get(self):
            if self.mode == 2:
                return({d: list(self.effective_dimension_dict[d].items()) for d in self.effective_dimension_dict})
            else:
                return({d: list(self.effective_dimension_dict[d]) for d in self.effective_dimension_dict})

            
    def bulk_update_database(self):
        mdb_bulk = self.db.series.initialize_ordered_bulk_op()
        es_bulk = []

        es = elasticsearch.Elasticsearch(host = "localhost")
        body = {
                'created': datetime.today()
        }
        es.index(index="widukind", doc_type='series', id=1, body=body)
        es_data = es.search(index = 'widukind', doc_type = 'series',
                            body={"query" : { "filtered" :
                                             { "filter":
                                              {"term":
                                               {"_id": self.datasetCode}}}}})
        old_es_index = {e['_source']['key']: e for e in es_data['hits']['hits']}
        effective_dimension_list = self.EffectiveDimensionList(self.codeDict)
        
        for s in self.data:
            s.collection = mdb_bulk
            s.update_database()
            es_index = ESSeriesIndex(s,self.codeDict)
            if s.key in old_es_index:
                if es_index != old_es_index[s.key]:
                    op_dict = {
                        "update": {
                            "_index": 'widukind',
                            "_type": 'series',
                            "_id": s.key
                        }
                    }
                    es_bulk.append(op_dict)
            else:
                op_dict = {
                    "index": {
                        "_index": 'widukind',
                        "_type": 'series',
                        "_id": s.key
                    }
                }
                es_bulk.append(op_dict)
            es_bulk.append(es_index.bson)
            effective_dimension_list.update(s.dimensions)
                                            
        res_mdb = mdb_bulk.execute();
        res_es = es.bulk(index = 'widukind', body = es_bulk, refresh = True)
        return(effective_dimension_list)
    
class Dataset(object):
    """Abstract base class for datasets
    >>> from datetime import datetime
    >>> dataset = Dataset(provider='Test provider',name='GDP in France',
    ...                 datasetCode='nama_gdp_fr',
    ...                 dimensionList=[{'name':'COUNTRY','values':[('FR','France'),('DE','Germany')]}],
    ...                 attributeList=[{'OBS_VALUE': [('p', 'preliminary'), ('f', 'forecast')]}],
    ...                 docHref='nauriset',
    ...                 lastUpdate=datetime(2014,12,2))
    >>> print(dataset)
    [('attributeList',
      [{'OBS_VALUE'
        : [('p', 'preliminary'), ('f', 'forecast')]}]),
     ('datasetCode', 'nama_gdp_fr'),
     ('dimensionList',
      [{'name': 'COUNTRY', 'values': [('FR', 'France'), ('DE', 'Germany')]}]),
     ('docHref', 'nauriset'),
     ('lastUpdate', datetime.datetime(2014, 12, 2, 0, 0)),
     ('name', 'GDP in France'),
     ('provider', 'Test provider')]
    """
    def __init__(self,
                 provider=None,
                 datasetCode=None,
                 name=None,
                 dimensionList=None,
                 attributeList=None,
                 docHref=None,
                 lastUpdate=None
                ):
        self.configuration=configuration
        self.db = mongo_client.widukind
        self.provider=provider.upper()
        self.datasetCode=datasetCode.upper()
        self.name=name
        self.attributeList=attributeList
        self.dimensionList=dimensionList
        self.docHref=docHref
        self.lastUpdate=lastUpdate
        self.configuration = configuration
        self.schema = Schema({'name':
                              All(str, Length(min=1)),
                              'provider':
                              All(str, Length(min=1)),
                              'datasetCode':
                              All(str, Length(min=1)),
                              Optional('docHref'):
                              Any(None,str),
                              'lastUpdate':
                              typecheck(datetime),
                              'dimensionList':
                              dimension_list_schema,
                              'attributeList':
                              Any(None,attribute_list_schema)
                               },required=True)

        if docHref is None:
            self.validate = self.schema({'provider': self.provider,
                                         'datasetCode': self.datasetCode,
                                         'name': self.name,
                                         'dimensionList': self.dimensionList,
                                         'attributeList': self.attributeList,
                                         'lastUpdate': self.lastUpdate
                                         })
        else:
            self.validate = self.schema({'provider': self.provider,
                                         'datasetCode': self.datasetCode,
                                         'name': self.name,
                                         'dimensionList': self.dimensionList,
                                         'attributeList': self.attributeList,
                                         'docHref': self.docHref,
                                         'lastUpdate': self.lastUpdate
                                        })

    def __repr__(self):
        return pprint.pformat([(key, self.validate[key])
                               for key in sorted(self.validate.keys())])

    @property
    def bson(self):
        return {'provider': self.provider,
                'name': self.name,
                'datasetCode': self.datasetCode,
                'dimensionList': self.dimensionList,
                'attributeList': self.attributeList,
                'docHref': self.docHref,
                'lastUpdate': self.lastUpdate}

    def es_bson(self,effectiveDimensionList):
        return {'provider': self.provider,
                'name': self.name,
                'datasetCode': self.datasetCode,
                'codeList': effectiveDimensionList.get(),
                'docHref': self.docHref,
                'lastUpdate': self.lastUpdate}

    def update_database(self):
        self.db.datasets.update({'datasetCode': self.bson['datasetCode']},
                                self.bson,upsert=True)

    def update_es_database(self,effectiveDimensionList):
        es = elasticsearch.Elasticsearch(host = "localhost")
        es.index(index = 'widukind', doc_type = 'datasets',
                 id = self.provider+'.'+self.datasetCode,
                 body = self.es_bson(effectiveDimensionList))
                 
class Category(object):
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
                 children=None,
                 categoryCode=None,
                 lastUpdate=None,
                 exposed=False
                ):
        self.configuration = configuration
        self.db = mongo_client.widukind
        self.provider=provider.upper()
        self.name=name
        self.docHref=docHref
        self.children=children
        self.categoryCode=categoryCode.upper()
        self.lastUpdate=lastUpdate
        self.exposed=exposed
        self.configuration=configuration
        self.schema = Schema({'name':
                              All(str, Length(min=1)),
                              'provider':
                              All(str, Length(min=1)),
                              'children':
                              Any(None,[typecheck(bson.objectid.ObjectId)]),
                              'docHref':
                              Any(None,str),
                              'lastUpdate':
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
        in_base_category = self.db.categories.find_one(
            {'categoryCode': self.bson['categoryCode']})
        if in_base_category is None:
  	     	_id_ = self.db.categories.insert(self.bson)
        else:
            self.db.categories.update(
                {'_id': in_base_category['_id']},self.bson)
            _id_ = in_base_category['_id']
        return _id_

if __name__ == "__main__":
    import doctest
    doctest.testmod()
