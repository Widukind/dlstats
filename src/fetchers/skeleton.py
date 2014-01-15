#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import pymongo

class Skeleton(object):
    def __init__(self):
        self.client = pymongo.MongoClient()
    def update_categories_db(self):
        """Update the categories in MongoDB
        """
        raise NotImplementedError("This method from the Skeleton class must"
                                  "be implemented.")
    def update_series_db(self):
        """Update the series in MongoDB
        """
        raise NotImplementedError("This method from the Skeleton class must"
                                  "be implemented.")
