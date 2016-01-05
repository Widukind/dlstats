# -*- coding: utf-8 -*-

import os
import unittest
from unittest import mock
import imp

from widukind_common.utils import get_mongo_db, create_or_update_indexes
from widukind_common import tests_tools as utils
from widukind_common import constants as common_constants

from dlstats import constants

from dlstats.tests import resources

RESOURCES_DIR = os.path.abspath(os.path.dirname(resources.__file__))

VERIFIED_RELEASES = False

class BaseTestCase(unittest.TestCase):
    
    def setUp(self):
        unittest.TestCase.setUp(self)

class BaseDBTestCase(BaseTestCase):
    """Tests with MongoDB or ElasticSearch
    """
    MONGODB_URL = "mongodb://localhost/widukind_test"

    @mock.patch.dict(os.environ)
    def setUp(self):
        BaseTestCase.setUp(self)

        os.environ.update(
            WIDUKIND_MONGODB_URL=self.MONGODB_URL,
        )

        imp.reload(common_constants)
        imp.reload(constants)
        
        self.db = get_mongo_db()
        
        self.assertEqual(self.db.name, "widukind_test")

        #self._releases_verify_ok()
        
        utils.clean_mongodb(self.db)
                
        create_or_update_indexes(self.db, force_mode=True)

    def tearDown(self):
        BaseTestCase.tearDown(self)
        #TODO: clean DB and es
    
    def _release_mongodb(self):
        """Verify MongoDB and pymongo release
        
        - http://docs.mongodb.org/ecosystem/drivers/driver-compatibility-reference
        
        - pymongo >= 2.8 = mongodb 2.4, 2.6, 3.0
        - pymongo 2.7 = mongodb 2.4, 2.6
        - pymongo 2.6 = mongodb 2.4        
        """

        import pymongo

        # (3, 0, 3)
        if pymongo.version_tuple[0] < 3:
            self.fail("Not supported Pymongo [%s] release. Required Pymongo 3.0.x" % pymongo.version)

        server_info = self.db.client.server_info() 
        server_release = server_info['versionArray'] #'versionArray': [2, 4, 14, 0]}
        
        #TODO: server_info['maxBsonObjectSize'] # 16777216
        #TODO: 'bits': 64,
                
        if server_release[0] != 2 or (server_release[0] == 2 and server_release[1] != 4):
            self.fail("Not supported MongoDB [%s] release. Required MongoDB 2.4.x" % server_info['version'])

    def _releases_verify_ok(self):

        global VERIFIED_RELEASES
        
        if VERIFIED_RELEASES:
            return True

        if os.environ.get("SKIP_RELEASE_CONTROL", "0") != "1":
            self._release_mongodb()
        
        VERIFIED_RELEASES = True

    def _collections_is_empty(self):
        #TODO: add tags
        self.assertEqual(self.db[constants.COL_CATEGORIES].count(), 0)
        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 0)
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 0)
        self.assertEqual(self.db[constants.COL_SERIES].count(), 0)
        self.assertEqual(self.db[constants.COL_TAGS_DATASETS].count(), 0)
        self.assertEqual(self.db[constants.COL_TAGS_SERIES].count(), 0)
        
