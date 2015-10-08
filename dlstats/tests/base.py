# -*- coding: utf-8 -*-

import os
import unittest

from dlstats import constants

from . import resources
from . import utils

RESOURCES_DIR = os.path.abspath(os.path.dirname(resources.__file__))

class BaseFetcherTest(unittest.TestCase):
    pass

class BaseFetcherDBTest(BaseFetcherTest):

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

