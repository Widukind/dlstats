# -*- coding: utf-8 -*-

import os
import unittest
from unittest import mock
import imp

from dlstats import constants
from dlstats import utils

from dlstats.tests import resources

RESOURCES_DIR = os.path.abspath(os.path.dirname(resources.__file__))

VERIFIED_RELEASES = False

class BaseTestCase(unittest.TestCase):
    
    def setUp(self):
        unittest.TestCase.setUp(self)

class BaseDBTestCase(BaseTestCase):
    """Tests with MongoDB or ElasticSearch
    """
    
    ES_INDEX = "widukind_test"
    MONGODB_NAME = "widukind_test"

    @mock.patch.dict(os.environ)
    def setUp(self):
        BaseTestCase.setUp(self)

        os.environ.update(
            WIDUKIND_MONGODB_NAME=self.MONGODB_NAME,
            WIDUKIND_ES_INDEX=self.ES_INDEX,
        )

        imp.reload(constants)
        
        self.assertEqual(constants.ES_INDEX, self.ES_INDEX)
        
        self.db = utils.get_mongo_db()
        self.es = utils.get_es_client()

        self._releases_verify_ok()
        
        utils.clean_mongodb(self.db)
        utils.clean_elasticsearch(es_client=self.es, index=self.ES_INDEX)
                
        utils.create_or_update_indexes(self.db, force_mode=True)

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

    def _release_elasticsearch(self):
        """Verify Elasticsearch and elasticsearch-py release
        
        # Elasticsearch 2.x
        elasticsearch>=2.0.0,<3.0.0
        
        # Elasticsearch 1.x
        elasticsearch>=1.0.0,<2.0.0
        
        # Elasticsearch 0.90.x
        elasticsearch<1.0.0        
        """
        
        server_info = self.es.info() 
        server_release = server_info['version']['number']
        
        if server_release != "1.6.2":
            self.fail("Not supported ElasticSearch Server [%s] release. Required ElasticSearch 1.6.2" % server_release) 
        
        #TODO: version elasticsearch py 
        #from elasticsearch import VERSION #(1, 6, 0)
            
    def _releases_verify_ok(self):

        global VERIFIED_RELEASES
        
        if VERIFIED_RELEASES:
            return True

        if os.environ.get("SKIP_RELEASE_CONTROL", "0") != "1":
            self._release_mongodb()
            self._release_elasticsearch()
        
        VERIFIED_RELEASES = True

    def _collections_is_empty(self):
        self.assertEqual(self.db[constants.COL_CATEGORIES].count(), 0)
        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 0)
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 0)
        self.assertEqual(self.db[constants.COL_SERIES].count(), 0)
        
