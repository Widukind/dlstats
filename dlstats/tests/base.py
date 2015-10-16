# -*- coding: utf-8 -*-

import os
import unittest

from dlstats import constants
from dlstats.fetchers._commons import create_or_update_indexes

from dlstats.tests import resources
from dlstats.tests import utils

RESOURCES_DIR = os.path.abspath(os.path.dirname(resources.__file__))

class BaseTest(unittest.TestCase):
    
    def setUp(self):
        unittest.TestCase.setUp(self)

class BaseDBTest(BaseTest):
    """Tests with MongoDB or ElasticSearch
    """

    def _collections_is_empty(self):
        self.assertEqual(self.db[constants.COL_CATEGORIES].count(), 0)
        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 0)
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 0)
        self.assertEqual(self.db[constants.COL_SERIES].count(), 0)
    
    def setUp(self):
        unittest.TestCase.setUp(self)

        self.db = utils.get_mongo_db()
        self.es = utils.get_es_db()
        
        utils.clean_mongodb(self.db)
        utils.clean_es()
                
        create_or_update_indexes(self.db, force_mode=True)
        

#class BaseFetcherDBTest(BaseTest):