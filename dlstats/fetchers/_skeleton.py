#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import pymongo
from voluptuous import Required, All, Length, Range, Schema
from dlstats import configuration
from datetime import datetime
import logging
from collections import defaultdict

class Skeleton(object):
    """Basic structure for statistical providers implementations."""
    def __init__(self):
        self.configuration = configuration
        self.client = pymongo.MongoClient(**self.configuration['MongoDB'])
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
                if not (k == 'versionDate'):
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
                releaseDates = bson['releaseDates']
                old_releaseDates = old_bson['releaseDates']
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
    def date_validator(v,fmt='%Y-%m-%d'):
        return datetime.strptime(v, fmt)
    #Schema definition in voluptuous
    str_date = (Required(All(str, Length(min=1))), Required(All(int, Range(min=1,max=20))))
    revision = (Required(All(int)), Required(All(int)),Required(All(str)))
    dimension = {Required('name'): All(str), Required('value'): All(str)}
    schema_series = Schema({Required('name'): All(str, Length(min=1)),
                            Required('key'): All(str, Length(min=1)),
                            Required('dataset_code'): All(str, Length(min=1)),
                            Required('start_date'): All(str_date),
                            Required('end_date'): All(str_date),
                            Required('values'): All(str),
                            Required('attributes'): All(str),
#                            Required('release_dates'): All([date_validator()]),
                            Required('revisions'): All([revision]),
                            Required('frequency'): All(str, Length(max=1)),
                            Required('dimensions'): All([dimension]),
                           })
#    dimension_list = {Required('name'): All(str), [list]}
    schema_dataset = Schema({Required('dataset_code'): All(str, Length(min=1)),
                             Required('name'): All(str, Length(min=1)),
#                             Required('dimension_list'): All(dimension_list, Length(min=1))),
                             Required('doc_href'): All(str, Length(min=1)),
#                             Required('attribute_list'): All(dimension_list),
#                             Required('last_update'): All(date_validator()),
#                             Required('version_date'): All(date_validator())
                            })
    schema_category = Schema({Required('name'): All(str, Length(min=1)),
#                              Required('children'): All(str, Length(min=1)),
                              Required('categoryCode'): All(str, Length(min=1)),
                              Required('exposed'): All(bool),
                             })
    
    class _Series(object):
        def __init__(self,
                     name=None,
                     key=None,
                     dataset_code=None,
                     start_date=None,
                     end_date=None, 
                     values=None,
                     attributes=None,
                     release_dates=None,
                     revisions=defaultdict(dict),
                     frequency=None,
                     dimensions=None):
            self.name=name
            self.key=key
            self.dataset_code=dataset_code
            self.start_date=start_date
            self.end_date=end_date
            self.values=values
            self.attributes=attributes
            self.release_dates=release_dates
            self.revisions=revisions
            self.frequency=frequency
            self.dimensions=dimensions
        @property
        def bson(self):
 #           self.validate()
            return {'name': self.name,
                    'key': self.key,
                    'datasetCode': self.dataset_code,
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
                     dataset_code=None,
                     name=None,
                     codes_list=None,
                     doc_href=None,
                     last_update=None,
                     version_date=None
                    ):
            self.dataset_code=dataset_code
            self.name=name
            self.codes_list=codes_list
            self.doc_href=doc_href
            self.last_update=last_update
            self.version_date=version_date

        @property
        def bson(self):
 #           self.validate()
            return {'name': self.name,
                    'datasetCode': self.dataset_code,
                    'codesList': self.codes_list,
                    'docHref': self.doc_href,
                    'lastUpdate': self.last_update}
        def validate(self):
            schema_dataset(self)
        def store(self,db):
            return Skeleton._bson_update(self,db,self.bson,'datasetCode')

    class _Category(object):
        def __init__(self,
                     name=None,
                     doc_href=None,
                     children=None,
                     category_code=None,
                     last_update=None,
                     exposed=False,
                    ):
            self.name=name
            self.doc_href=doc_href
            self.children=children
            self.category_code=category_code
            self.last_update=last_update
            self.exposed=exposed

        @property
        def bson(self):
#            self.validate()
            return {'name': self.name,
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
