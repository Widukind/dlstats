#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
.. module:: _skeleton
    :synopsis: Module containing an abstract base class for all the fetchers
"""
import pymongo
from voluptuous import Required, All, Length, Range, Schema, Invalid
from dlstats import configuration
from datetime import datetime
import logging
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
    def _bson_update(self,coll,bson,key):
        old_bson = coll.find_one({key: bson[key]})
        if old_bson == None:
            _id = coll.insert(bson)
            return _id
        else:
            identical = True
            for k in bson.keys():
                if not (k == 'version_date'):
                    if (old_bson[k] != bson[k]):
                        logging.warning(coll.database.name+'.'+coll.name+': '+k+" has changed value. Old value: {}, new value: {}".format(old_bson[k],bson[k]))
                        identical = False
            if not identical:
                coll.update({'_id': old_bson['_id']},bson)
            return old_bson['_id']

    def _series_update(self,coll,bson,key):
        old_bson = coll.find_one({key: bson[key]})
        if old_bson == None:
            _id = coll.insert(bson)
            return _id
        else:
            identical = True
            for k in bson.keys():
                if (k != 'versionDate'):
                    if (old_bson[k] != bson[k]):
                        logging.warning(coll.database.name+'.'+coll.name+': '+k+" has changed value. Old value: {}, new value: {}".format(old_bson[k],bson[k]))
                        identical = False
            if not identical:
                values = bson['values']
                old_values = old_bson['values']
                releaseDates = bson['release_dates']
                old_releaseDates = old_bson['release_dates']
                revisions = old_bson['revisions']
                for i in range(len(old_values)):
                    if old_values[i] == values[i]:
                        releaseDates[i] = old_releaseDates[i]
                    else:
                        revisions[i][releaseDates[i]] = old_values[i]
                bson['revisions'] = revisions
                bson['releaseDates'] = releaseDates
                coll.update({'_id': old_bson['_id']},bson,upsert=True)
            return old_bson['_id']


    #Validation and ODM
    #Custom validator (only a few types are natively implemented in voluptuous)
    def date_validator(value):
        if isinstance(value, datetime):
            return value
        else:
            raise Invalid('Input date was not of type datetime.datetime')

    #Schema definition in voluptuous
    revision = [{'value':Required(All(int)), 'position':Required(All(int)), 'release_date':Required(All(date_validator))}]
    dimension = {Required('name'): All(str), Required('value'): All(str)}
    schema_series = Schema({Required('name'): All(str, Length(min=1)),
                            Required('key'): All(str, Length(min=1)),
                            Required('dataset_code'): All(str, Length(min=1)),
                            Required('start_ordinal_date'): All(int),
                            Required('end_ordinal_date'): All(int),
                            Required('values'): All(str),
                            Required('attributes'): All(str),
                            Required('revisions'): All([revision]),
                            Required('frequency'): All(str, Length(max=1)),
                            Required('dimensions'): All([dimension])
                           })
    dimension_list = [{Required('name'): All(str), Required('value'): [All(str)]}]
    schema_dataset = Schema({Required('dataset_code'): All(str, Length(min=1)),
                             Required('name'): All(str, Length(min=1)),
                             Required('dimension_list'): All(dimension_list, Length(min=1)),
                             Required('doc_href'): All(str, Length(min=1)),
                             Required('attribute_list'): All(dimension_list),
                             Required('last_update'): All(date_validator),
                             Required('version_date'): All(date_validator)
                            })
    schema_category = Schema({Required('name'): All(str, Length(min=1)),
                              Required('children'): All(str, Length(min=1)),
                              Required('category_code'): All(str, Length(min=1)),
                              Required('exposed'): All(bool),
                             })
    
    class _Series(object):
        """Abstract base class for time series
        >>> import datetime
        >>> series = Series(provider='Test provider',name='GDP in France',
        ...                 key='GDP_FR',dataset_code='nama_gdp_fr',
        ...                 start_ordinal_date=8052,end_ordinal_date=8056,
        ...                 values = [2700, 2720, 2740, 2760],
        ...                 attributes = {'name':'OBS_VALUE','value':'p'},
        ...                 release_dates = [datetime.datetime(2014,8,28),
        ...                                  datetime.datetime(2014,11,28)],
        ...                 revisions = [{'value':2710, 'position':2,
        ...                 'release_date':datetime.datetime(2014,11,28)}],
        ...                 frequency = 'Q',
        ...                 dimensions = {'name':'Seasonal adjustment', value:'wda'})
        """

        def __init__(self,
                     provider=None,
                     name=None,
                     key=None,
                     dataset_code=None,
                     start_ordinal_date=None,
                     end_ordinal_date=None, 
                     values=None,
                     attributes=None,
                     revisions=defaultdict(dict),
                     frequency=None,
                     dimensions=None):
            self.provider=provider
            self.name=name
            self.key=key
            #SDMX equivalent concept: flowRef
            self.dataset_code=dataset_code
            self.start_date=start_date
            self.end_date=end_date
            self.values=values
            self.attributes=attributes
            self.revisions=revisions
            self.frequency=frequency
            self.dimensions=dimensions
        @property
        def bson(self):
            self.validate()
            return {'provider': self.provider,
                    'name': self.name,
                    'key': self.key,
                    'dataset_code': self.dataset_code,
                    'startDate': self.start_date,
                    'endDate': self.end_date,
                    'values': self.values,
                    'attributes': self.attributes,
                    'dimensions': self.dimensions,
                    'releaseDates': self.release_dates,
                    'revisions': self.revisions,
                    'frequency': self.frequency}
        def validate(self):
            schema_series(self)
        def store(self,db):
            return Skeleton._series_update(self,db,self.bson,'key')

    class _Dataset(object):
        def __init__(self,
                     provider=None,
                     dataset_code=None,
                     name=None,
                     dimensions_list=None,
                     doc_href=None,
                     last_update=None,
                     version_date=None
                    ):
            self.provider=provider
            self.dataset_code=dataset_code
            self.name=name
            self.dimensions_list=dimensions_list
            self.doc_href=doc_href
            self.last_update=last_update
            self.version_date=version_date

        @property
        def bson(self):
            self.validate()
            return {'provider': self.provider,
                    'name': self.name,
                    'dataset_code': self.dataset_code,
                    'dimensions_list': self.dimensions_list,
                    'docHref': self.doc_href,
                    'lastUpdate': self.last_update}
        def validate(self):
            schema_dataset(self)
        def store(self,db):
            return Skeleton._bson_update(self,db,self.bson,'dataset_code')

    class _Category(object):
        def __init__(self,
                     provider=None,
                     name=None,
                     doc_href=None,
                     children=None,
                     category_code=None,
                     last_update=None,
                     exposed=False,
                    ):
            self.provider=provider
            self.name=name
            self.doc_href=doc_href
            self.children=children
            self.category_code=category_code
            self.last_update=last_update
            self.exposed=exposed

        @property
        def bson(self):
            self.validate()
            return {'provider': self.provider,
                    'name': self.name,
                    'docHref': self.doc_href,
                    'children': self.children,
                    'categoryCode': self.category_code,
                    'lastUpdate': self.last_update,
                    'exposed': self.exposed}
        def validate(self):
            print(self.bson)
            Skeleton.schema_category(self.bson)
        def store(self,db):
            return Skeleton._bson_update(self,db,self.bson,'categoryCode')
