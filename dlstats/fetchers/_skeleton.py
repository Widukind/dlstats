#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#TODO: Review category_code. Review Category(). Put in skeletons methods for updating things, properly implemented
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

class Skeleton(object):
    """Abstract base class for fetchers"""
    def __init__(self):
        self.configuration = configuration
        self.client = pymongo.MongoClient(**self.configuration['MongoDB'])
        self.db = self.client.widukind
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
revision_schema = [{'value':Required(All(int)), 'position':Required(All(int)),
             'releaseDate':Required(All(date_validator))}]
dimensions = {str: str}
attributes = {str: [str]}
dimension_list_schema = [{Required('name'): All(str), Required('values'): [(All(str),All(str))]}]

class Series(object):
    """Abstract base class for time series
    >>> from datetime import datetime
    >>> series = Series(provider='Test provider',name='GDP in France',
    ...                 key='GDP_FR',datasetCode='nama_gdp_fr',
    ...                 values = [2700, 2720, 2740, 2760],
    ...                 releaseDates = [datetime(2013,11,28),datetime(2014,12,28),datetime(2015,1,28),datetime(2015,2,28)],
    ...                 period_index = pandas.period_range('1/1999', periods=72, freq='Q'),
    ...                 attributes = {'name':'OBS_VALUE','value':'p'},
    ...                 revisions = [{'value':2710, 'position':2,
    ...                 'releaseDate' : datetime(2014,11,28)}],
    ...                 dimensions = [{'name':'Seasonal adjustment', 'value':'wda'}])
    >>> print(series)
    [('attributes', {'name': 'OBS_VALUE', 'value': 'p'}),
     ('datasetCode', 'nama_gdp_fr'),
     ('dimensions', [{'name': 'Seasonal adjustment', 'value': 'wda'}]),
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
        'value': 2710}]),
     ('values', [2700, 2720, 2740, 2760])]
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
                              All(typecheck(pandas.tseries.period.PeriodIndex)),
                              'values':
                              All([str]),
                              'releaseDates':
                              All([date_validator]),
                              'attributes':
                              Any({},attributes),
                              'revisions':
                              Any(None,revision_schema),
                              'dimensions':
                              All(dimensions)
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
            old_start_period = pandas.Period(old_bson['startDate'],old_bson['dimensions']['FREQ'])
            start_period = pandas.Period(self.bson['startDate'],self.bson['dimensions']['FREQ'])
            if start_period > old_start_period:
            # previous, longer, series is kept
                offset = start_period - old_start_period
                self.bson['numberOfPeriods'] += offset
                self.bson['startDate'] = old_bson['startDate']
                for values in zip(old_bson['values'][offset:],self.values):
                    if values[0] != values[1]:
                        self.revisions.append({'value':values[0],
                                               'position': offset+position,
                                               'releaseDate':
                                               old_bson['releaseDate'][offset+position]})
                    position += 1
            else:
            # zero or more data are added at the beginning of the series
                offset = old_start_period - start_period
                for values in zip(old_bson['values'],self.values[offset:]):
                    if values[0] != values[1]:
                        self.revisions.append({'value':values[0],
                                               'position': offset+position,
                                               'releaseDate':
                                               old_bson['releaseDate'][position]})
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
        self.schema = Schema({Required('name'):
                                     All(str, Length(min=1)),
                                     Required('provider'):
                                     All(str, Length(min=1)),
                                     Required('datasetCode'):
                                     All(str, Length(min=1)),
                                     Optional('docHref'):
                                     All(str, Length(min=1)),
                                     Required('lastUpdate'):
                                     All(typecheck(datetime)),
                                     Required('dimensionList'):
                                     All(dimension_list_schema),
                                     Required('attributeList'):
                                     All(dimension_list_schema)
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
        return pprint.pformat([(key, self.validate[key]) for key in sorted(self.validate.keys())])

    @property
    def bson(self):
        return {'provider': self.provider,
                'name': self.name,
                'datasetCode': self.datasetCode,
                'dimensionList': self.dimensionList,
                'attributeList': self.dimensionList,
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
        self.schema = Schema({Required('name'):
                                     All(str, Length(min=1)),
                                     Required('provider'):
                                     All(str, Length(min=1)),
                                     Required('children'):
                                     Any(None,[typecheck(bson.objectid.ObjectId)]),
                                     Required('docHref'):
                                     All(str, Length(min=1)),
                                     Required('lastUpdate'):
                                     All(typecheck(datetime)),
                                     Required('categoryCode'):
                                     All(str, Length(min=1)),
                                     Required('exposed'):
                                     All(typecheck(bool)),
                               })
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
        self.db.categories.update({'categoryCode': self.bson['categoryCode']},
                                  self.bson,upsert=True)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
