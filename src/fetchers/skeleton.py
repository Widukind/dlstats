#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import pymongo

class Skeleton(object):
    """Basic structure for statistical providers implementations."""
    def __init__(self):
        self.client = pymongo.MongoClient()
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
