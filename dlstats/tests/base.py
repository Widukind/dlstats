# -*- coding: utf-8 -*-

import os
import unittest

from dlstats import constants
from dlstats.tests import resources

RESOURCES_DIR = os.path.abspath(os.path.dirname(resources.__file__))

class BaseTestCase(unittest.TestCase):
    pass

class BaseDBTestCase(unittest.TestCase):
    """Tests with MongoDB
    """

    def setUp(self):
        BaseTestCase.setUp(self)

        from widukind_common.utils import get_mongo_db, create_or_update_indexes
        from widukind_common import tests_tools as utils
        
        db = get_mongo_db()
        self.db = db.client["widukind_test"] 
        
        self.assertEqual(self.db.name, "widukind_test")

        utils.clean_mongodb(self.db)
                
        create_or_update_indexes(self.db, force_mode=True, background=False)

        self._collections_is_empty()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        try:
            self.db.client.close()
        except:
            pass

    def _collections_is_empty(self):
        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 0)
        self.assertEqual(self.db[constants.COL_CATEGORIES].count(), 0)
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 0)
        self.assertEqual(self.db[constants.COL_SERIES].count(), 0)
        #self.assertEqual(self.db[constants.COL_TAGS].count(), 0)
        
