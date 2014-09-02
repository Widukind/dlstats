#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import pymongo
from dlstats import configuration

class Skeleton(object):
    """Basic structure for statistical providers implementations."""
    def __init__(self):
        self.configuration = configuration
        self.client = pymongo.MongoClient(**self.configuration['MongoDB'])
    def create_categories_db(self):
        """Create the categories in MongoDB
        """
        raise NotImplementedError("This method from the Skeleton class must"
                                  "be implemented.")
    def update_categories_db(self):
        """Update the categories in MongoDB
        """
        raise NotImplementedError("This method from the Skeleton class must"
                                  "be implemented.")
    def create_series_db(self):
        """Create the series in MongoDB
        """
        raise NotImplementedError("This method from the Skeleton class must"
                                  "be implemented.")
    def update_series_db(self):
        """Update the series in MongoDB
        """
        raise NotImplementedError("This method from the Skeleton class must"
                                  "be implemented.")
    def bson_update(self,coll,bson,key):
        old_bson = coll.find_one({key: bson[key]})
        if old_bson == None:
            _id = coll.insert(bson)
            return _id
        else:
            identical = True
            for k in bson.keys():
                if not (k == 'versionDate'):
                    if (old_bson[k] != bson[k]):
                        self.log_warning(coll.database.name+'.'+coll.name+': '+k+" has changed value. Old value: {}, new value: {}".format(old_bson[k],bson[k]))
                        identical = False
            if not identical:
                coll.update({'_id': old_bson['_id']},bson)
            return old_bson['_id']

    def series_update(self,coll,bson,key):
        old_bson = coll.find_one({key: bson[key]})
        if old_bson == None:
            _id = coll.insert(bson)
            return _id
        else:
            identical = True
            for k in bson.keys():
                if (k != 'versionDate'):
                    if (old_bson[k] != bson[k]):
                        self.log_warning(coll.database.name+'.'+coll.name+': '+k+" has changed value. Old value: {}, new value: {}".format(old_bson[k],bson[k]))
                        identical = False
            if not identical:
                values = bson['values']
                old_values = old_bson['values']
                releaseDates = bson['releaseDates']
                old_releaseDates = old_bson['releaseDates']
                old_revisions = old_bson['revisions']
                for i in range(len(old_values)):
                    if old_values[i] == values[i]:
                        releaseDates[i] = old_releaseDates[i]
                    else:
                        revisions[i] = old_revisions[i]
                        revisions[i][releaseDates[i]] = old_values[i]
                bson['releaseDates'] = releaseDates
                bson['revisions'] = revisions
                coll.update({'_id': old_bson['_id']},bson,upsert=True)
            return old_bson['_id']

    def log_warning(self,msg):
        """Send message to the database operator"""

        print(msg)
