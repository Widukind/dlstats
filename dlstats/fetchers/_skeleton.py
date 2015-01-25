#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
.. module:: _skeleton
    :synopsis: Module containing an abstract base class for all the fetchers
"""
import pymongo
import pandas
from voluptuous import Required, All, Length, Range, Schema, Invalid, Object, Optional, Any
from dlstats import configuration
from datetime import datetime
import logging
import bson
import pprint
from collections import defaultdict
import elasticsearch

class Skeleton(object):
    """Abstract base class for fetchers"""
    def __init__(self, provider_name=None):
        self.configuration = configuration
        self.provider_name = provider_name
        self.client = pymongo.MongoClient(**self.configuration['MongoDB'])
        self.db = self.client.widukind
        self.elasticsearch = elasticsearch.Elasticsearch(host = self.configuration['ElasticSearch']['host'])
    def upsert_categories(self,id):
        """Upsert the categories in MongoDB
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
    def create_index_elasticsearch(self):
        def get_dimensions(dimensions,dimension_list):
            dd = defaultdict(dict)
            dl = {s['name']: {s1[0]: s1[1] for s1 in s['values']} for s in dimension_list}
            for d in dimensions:
                for di in d['dimensions'].items():
                    if di[0] in dl.keys():
                        dd[di[0]][di[1]] = dl[di[0]][di[1]]
            return dd

        ES_HOST = {
            "host" : self.configuration['ElasticSearch']['host'],
            "port" : self.configuration['ElasticSearch']['port']
        }

        INDEX_NAME = 'widukind'
        TYPE_NAME = 'datasets'

        ID_FIELD = 'datasetCode'

        cat = self.db.categories.find_one({'provider': self.provider_name, 'name': 'root'})

        coll = { d['_id']: d for d in self.db.categories.find({'provider': self.provider_name},{'provider': 0})}

        def levels(d,node,coll,level,order):
            level += [coll[node]['name']]
            children = coll[node]['children']
            if type(children) is list:
                if type(children[0]) is list:
                    children = children[0]
                for child in children:
                    (d, order) = levels(d,child,coll,level,order)
                level.pop()
            else:
                d[coll[node]['categoryCode']] = [order]+level[1:]
                order += 1
                level.pop()
            return (d, order)

        res = levels({},cat['_id'],coll,[],0)
        d_levels = res[0]

        print(d_levels)

        datasets = self.db.datasets.find({'provider': self.provider_name})

        bulk_data = []

        for d in datasets:
            if 'dimensionList' in d.keys():
                dimensions = self.db.series.find({'datasetCode': d['datasetCode']},{'dimensions': 1})
                data_dict = {}
                data_dict['name'] = d['name']
                data_dict['dimensionList'] = get_dimensions(dimensions,d['dimensionList'])
                data_dict['provider'] = d['provider']
                data_dict['datasetCode'] = d['datasetCode']
                data_dict['order'] = d_levels[d['datasetCode']][0]
                k = 1
                for e in d_levels[d['datasetCode']][1:]:
                    data_dict['level'+str(k)] = e
                    k += 1
                    
                op_dict = {
                    "index": {
                        "_index": INDEX_NAME,
                        "_type": TYPE_NAME,
                        "_id": data_dict[ID_FIELD]
                    }
                }
                bulk_data.append(op_dict)
                bulk_data.append(data_dict)


        if self.elasticsearch.indices.exists(INDEX_NAME):
            res = self.elasticsearch.indices.delete(index = INDEX_NAME)

        request_body = {
            "settings" : {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        }
        res = self.elasticsearch.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)

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
             Required('releaseDate'): date_validator}]
dimensions = {str: str}
attributes = {str: [str]}
dimension_list_schema = [{Required('name'): str, Required('values'): [Any(str,(str, str))]}]

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
    ...                 'releaseDate' : datetime(2014,11,28)}],
    ...                 dimensions = {'Seasonal adjustment':'wda'})
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
        'releaseDate': datetime.datetime(2014, 11, 28, 0, 0),
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
        self.provider=provider
        self.name=name
        self.key=key
        #SDMX equivalent concept: flowRef
        self.datasetCode=datasetCode
        self.period_index=period_index
        self.frequency=frequency
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
        self.client = pymongo.MongoClient(**self.configuration['MongoDB'])
        self.db = self.client.widukind
        return cls.from_bson(self.db.series.find(mongo_id))

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
        return pprint.pformat([(key, self.validate[key]) for key in sorted(self.validate.keys())])

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
        self.client = pymongo.MongoClient(**self.configuration['MongoDB'])
        self.db = self.client.widukind
        old_bson = self.db.series.find_one({'key': self.bson['key']})

        if old_bson == None:
            return self.db.series.insert(self.bson)
        else:
            position = 0
            self.revisions = old_bson['revisions']
            old_start_period = pandas.Period(old_bson['startDate'],freq=old_bson['frequency'])
            start_period = pandas.Period(self.bson['startDate'],freq=self.bson['frequency'])
            if start_period > old_start_period:
            # previous, longer, series is kept
                offset = start_period - old_start_period
                self.bson['numberOfPeriods'] += offset
                self.bson['startDate'] = old_bson['startDate']
                for values in zip(old_bson['values'][offset:],self.values):
                    if values[0] != values[1]:
                        self.revisions.append({'value':values[0],
                                               'position': offset+position,
                                               'releaseDates':
                                               old_bson['releaseDates'][offset+position]})
                    position += 1
            else:
            # zero or more data are added at the beginning of the series
                offset = old_start_period - start_period
                for values in zip(old_bson['values'],self.values[offset:]):
                    if values[0] != values[1]:
                        self.revisions.append({'value':values[0],
                                               'position': offset+position,
                                               'releaseDates':
                                               old_bson['releaseDates'][position]})
                    position += 1
                                              
            self.bson['revisions'] = self.revisions
            self.db.series.update({'_id': old_bson['_id']},self.bson,
                                  upsert=True)
        return old_bson['_id']

class Dataset(object):
    """Abstract base class for datasets
    >>> from datetime import datetime
    >>> dataset = Dataset(provider='Test provider',name='GDP in France',
    ...                 datasetCode='nama_gdp_fr',
    ...                 dimensionList=[{'name':'COUNTRY','values':[('FR','France'),('DE','Germany')]}],
    ...                 attributeList=[{'name': 'OBS_VALUE', 'values': [('p', 'preliminary'), ('f', 'forecast')]}],
    ...                 docHref='nauriset',
    ...                 lastUpdate=datetime(2014,12,2))
    >>> print(dataset)
    [('attributeList',
      [{'name': 'OBS_VALUE',
        'values': [('p', 'preliminary'), ('f', 'forecast')]}]),
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
        self.provider=provider
        self.datasetCode=datasetCode
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
                              'docHref':
                              Any(None,str),
                              'lastUpdate':
                              typecheck(datetime),
                              'dimensionList':
                              dimension_list_schema,
                              'attributeList':
                              dimension_list_schema
                               },required=True)

    def __repr__(self):
        return pprint.pformat([(key, self.validate[key]) for key in sorted(self.validate.keys())])

    @property
    def bson(self):
        return {'provider': self.provider,
                'name': self.name,
                'datasetCode': self.datasetCode,
                'dimensionList': self.dimensionList,
                'attributeList': self.attributeList,
                'docHref': self.docHref,
                'lastUpdate': self.lastUpdate}
    def update_database(self):
        self.client = pymongo.MongoClient(**self.configuration['MongoDB'])
        self.db = self.client.widukind
        self.db.datasets.update({'datasetCode': self.bson['datasetCode']},
                                self.bson,upsert=True)

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
        return pprint.pformat([(key, self.validate[key]) for key in sorted(self.validate.keys())])

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
        self.client = pymongo.MongoClient(**self.configuration['MongoDB'])
        self.db = self.client.widukind
        in_base_category = self.db.categories.find_one({'categoryCode': self.bson['categoryCode']})
        if in_base_category is None:
  	     	_id_ = self.db.categories.insert(self.bson)
        else:
            self.db.categories.update({'_id': in_base_category['_id']}, self.bson)
            _id_ = in_base_category['_id']
        return _id_

if __name__ == "__main__":
    import doctest
    doctest.testmod()
