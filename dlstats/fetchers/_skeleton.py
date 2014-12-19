#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#TODO: Review category_code. Review Category(). Put in skeletons methods for updating things, properly implemented
"""
.. module:: _skeleton
    :synopsis: Module containing an abstract base class for all the fetchers
"""
import pymongo
import pandas
from voluptuous import Required, All, Length, Range, Schema, Invalid, Object, Optional
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
revision = [{'value':Required(All(int)), 'position':Required(All(int)),
             'release_date':Required(All(date_validator))}]
dimension = {Required('name'): All(str), Required('value'): All(str)}
dimension_list_schema = [{Required('name'): All(str), Required('values'): [(All(str),All(str))]}]

class Series(object):
    """Abstract base class for time series
    >>> from datetime import datetime
    >>> series = Series(provider='Test provider',name='GDP in France',
    ...                 key='GDP_FR',dataset_code='nama_gdp_fr',
    ...                 values = [2700, 2720, 2740, 2760],
    ...                 period_index = pandas.period_range('1/1999', periods=72, freq='Q'),
    ...                 attributes = {'name':'OBS_VALUE','value':'p'},
    ...                 revisions = [{'value':2710, 'position':2,
    ...                 'release_date' : datetime(2014,11,28)}],
    ...                 dimensions = [{'name':'Seasonal adjustment', 'value':'wda'}])
    >>> print(series)
    [('attributes', {'name': 'OBS_VALUE', 'value': 'p'}),
     ('dataset_code', 'nama_gdp_fr'),
     ('dimensions', [{'name': 'Seasonal adjustment', 'value': 'wda'}]),
     ('key', 'GDP_FR'),
     ('name', 'GDP in France'),
     ('period_index',
      <class 'pandas.tseries.period.PeriodIndex'>
    [1999Q1, ..., 2016Q4]
    Length: 72, Freq: Q-DEC),
     ('provider', 'Test provider'),
     ('revisions',
      [{'position': 2,
        'release_date': datetime.datetime(2014, 11, 28, 0, 0),
        'value': 2710}]),
     ('values', [2700, 2720, 2740, 2760])]
    None
    """

    def __init__(self,
                 provider=None,
                 name=None,
                 key=None,
                 dataset_code=None,
                 period_index=None, 
                 release_dates=None, 
                 values=None,
                 attributes=None,
                 revisions=defaultdict(dict),
                 frequency=None,
                 dimensions=None):
        self.configuration=configuration
        self.provider=provider
        self.name=name
        self.key=key
        #SDMX equivalent concept: flowRef
        self.dataset_code=dataset_code
        self.period_index=period_index
        self.values=values
        self.release_dates=release_dates
        self.attributes=attributes
        self.revisions=revisions
        self.dimensions=dimensions
        self.schema = Schema({Required('name'):
                              All(str, Length(min=1)),
                              Required('provider'):
                              All(str, Length(min=1)),
                              Required('key'):
                              All(str, Length(min=1)),
                              Required('dataset_code'):
                              All(str, Length(min=1)),
                              Required('period_index'):
                              All(typecheck(pandas.tseries.period.PeriodIndex)),
                              Required('values'):
                              All([int]),
                              Optional('release_dates'):
                              All([date_validator]),
                              Optional('attributes'):
                              All(dimension),
                              Required('revisions'):
                              All(revision),
                              Required('dimensions'):
                              All([dimension])
                               })
        if attributes is None:
            self.validate = self.schema({'provider': self.provider,
                                         'name': self.name,
                                         'key': self.key,
                                         'dataset_code': self.dataset_code,
                                         'period_index': self.period_index,
                                         'values': self.values,
                                         'dimensions': self.dimensions,
                                         'revisions': self.revisions
                                        })
        else:
            self.validate = self.schema({'provider': self.provider,
                                         'name': self.name,
                                         'key': self.key,
                                         'dataset_code': self.dataset_code,
                                         'period_index': self.period_index,
                                         'values': self.values,
                                         'attributes': self.attributes,
                                         'dimensions': self.dimensions,
                                         'revisions': self.revisions
                                        })

    @classmethod
    def from_index(cls,mongo_id):
        self.client = pymongo.MongoClient(**self.configuration['MongoDB'])
        self.db = self.client.widukind
        return cls.from_bson(self.db.series.find(mongo_id))

    @classmethod
    def from_bson(cls,bson):
        period_index = pandas.period_range(bson['start_period'],
                                           periods=bson['number_of_periods'],
                                           freq=bson['frequency'])
        return cls(provider=bson['provider'],
                   name=bson['name'],
                   key=bson['key'],
                   dataset_code=bson['dataset_code'],
                   values=bson['values'],
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
                'dataset_code': self.dataset_code,
                'start_date': self.start_date,
                'endDate': self.end_date,
                'values': self.values,
                'attributes': self.attributes,
                'dimensions': self.dimensions,
                'start_date': self.period_index[0],
                'number_of_periods': len(self.period_index),
                'revisions': self.revisions,
                'frequency': self.frequency}

    def update_database(self,mongo_id=None,key=None):
        self.client = pymongo.MongoClient(**self.configuration['MongoDB'])
        self.db = self.client.widukind
        old_bson = self.db.series.find_one({'key': self.bson['key']})

        if old_bson == None:
            return self.db.series.insert(self.bson)
        else:
            self.revisions = self.bson['revisions']
            position = 0
            for values in zip(self.old_bson['values'],self.values):
                if values[0] != values[1]:
                    self.revisions.append({'value':values[0],
                                           'position': position,
                                           'release_date':
                                           old_bson['release_date']})
            self.bson['revisions'] = self.revisions
            self.db.series.update({'_id': old_bson['_id']},self.bson,
                                  upsert=True)
        return old_bson['_id']

class Dataset(object):
    """Abstract base class for datasets
    >>> from datetime import datetime
    >>> dataset = Dataset(provider='Test provider',name='GDP in France',
    ...                 dataset_code='nama_gdp_fr',
    ...                 dimension_list=[{'name':'COUNTRY','values':[('FR','France'),('DE','Germany')]}],
    ...                 doc_href='nauriset',
    ...                 last_update=datetime(2014,12,2))
    >>> print(dataset)
    [('dataset_code', 'nama_gdp_fr'),
     ('dimension_list',
      [{'name': 'COUNTRY', 'values': [('FR', 'France'), ('DE', 'Germany')]}]),
     ('doc_href', 'nauriset'),
     ('last_update', datetime.datetime(2014, 12, 2, 0, 0)),
     ('name', 'GDP in France'),
     ('provider', 'Test provider')]
    None
    """
    def __init__(self,
                 provider=None,
                 dataset_code=None,
                 name=None,
                 dimension_list=None,
                 doc_href=None,
                 last_update=None
                ):
        self.provider=provider
        self.dataset_code=dataset_code
        self.name=name
        self.dimension_list=dimension_list
        self.doc_href=doc_href
        self.last_update=last_update
        self.configuration = configuration
        self.schema = Schema({Required('name'):
                                     All(str, Length(min=1)),
                                     Required('provider'):
                                     All(str, Length(min=1)),
                                     Required('dataset_code'):
                                     All(str, Length(min=1)),
                                     Optional('doc_href'):
                                     All(str, Length(min=1)),
                                     Required('last_update'):
                                     All(typecheck(datetime)),
                                     Required('dimension_list'):
                                     All(dimension_list_schema)
                               },required=True)
        if doc_href is None:
            self.validate = self.schema({'provider': self.provider,
                        'dataset_code': self.dataset_code,
                        'name': self.name,
                        'dimension_list': self.dimension_list,
                        'last_update': self.last_update
                        })
        else:
            self.validate = self.schema({'provider': self.provider,
                        'dataset_code': self.dataset_code,
                        'name': self.name,
                        'dimension_list': self.dimension_list,
                        'doc_href': self.doc_href,
                        'last_update': self.last_update
                        })

    def __repr__(self):
        return pprint.pformat([(key, self.validate[key]) for key in sorted(self.validate.keys())])

    @property
    def bson(self):
        return {'provider': self.provider,
                'name': self.name,
                'dataset_code': self.dataset_code,
                'dimension_list': self.dimension_list,
                'docHref': self.doc_href,
                'lastUpdate': self.last_update}
    def update_database(self):
        self.client = pymongo.MongoClient(**self.configuration['MongoDB'])
        self.db = self.client.widukind
        self.db.datasets.update({'dataset_code': self.bson['dataset_code']},
                                self.bson,upsert=True)

class Category(object):
    """Abstract base class for categories
    >>> from datetime import datetime
    >>> category = Category(provider='Test provider',name='GDP',
    ...                 category_code='nama_gdp',
    ...                 children=[bson.objectid.ObjectId.from_datetime(datetime(2014,12,3))],
    ...                 doc_href='http://www.perdu.com',
    ...                 last_update=datetime(2014,12,2),
    ...                 exposed=True)
    >>> print(category)
    [('category_code', 'GDP'),
     ('children', [ObjectId('547e52800000000000000000')]),
     ('doc_href', 'http://www.perdu.com'),
     ('exposed', True),
     ('last_update', datetime.datetime(2014, 12, 2, 0, 0)),
     ('name', 'GDP'),
     ('provider', 'Test provider')]
    None
    """
    def __init__(self,
                 provider=None,
                 name=None,
                 doc_href=None,
                 children=None,
                 category_code=None,
                 last_update=None,
                 exposed=False
                ):
        self.provider=provider
        self.name=name
        self.doc_href=doc_href
        self.children=children
        self.category_code=category_code
        self.last_update=last_update
        self.exposed=exposed
        self.configuration=configuration
        self.schema = Schema({Required('name'):
                                     All(str, Length(min=1)),
                                     Required('provider'):
                                     All(str, Length(min=1)),
                                     Required('children'):
                                     All([typecheck(bson.objectid.ObjectId)]),
                                     Required('doc_href'):
                                     All(str, Length(min=1)),
                                     Required('last_update'):
                                     All(typecheck(datetime)),
                                     Required('category_code'):
                                     All(str, Length(min=1)),
                                     Required('exposed'):
                                     All(typecheck(bool)),
                               })
        self.validate = self.schema({'provider': self.provider,
                    'category_code': self.name,
                    'name': self.name,
                    'children': self.children,
                    'doc_href': self.doc_href,
                    'last_update': self.last_update,
                    'exposed': self.exposed
                    })

    def __repr__(self):
        return pprint.pformat([(key, self.validate[key]) for key in sorted(self.validate.keys())])

    @property
    def bson(self):
        return {'provider': self.provider,
                'name': self.name,
                'doc_href': self.doc_href,
                'children': self.children,
                'category_code': self.category_code,
                'last_update': self.last_update,
                'exposed': self.exposed}
    def update_database(self):
        self.client = pymongo.MongoClient(**self.configuration['MongoDB'])
        self.db = self.client.widukind
        self.db.categories.update({'category': self.bson['category_code']},
                                  self.bson,upsert=True)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
