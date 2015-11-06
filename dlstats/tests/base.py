# -*- coding: utf-8 -*-

import os
import unittest

from dlstats import constants
from dlstats.fetchers._commons import create_or_update_indexes

from dlstats.tests import resources
from dlstats.tests import utils

RESOURCES_DIR = os.path.abspath(os.path.dirname(resources.__file__))

VERIFIED_RELEASES = False

class BaseTestCase(unittest.TestCase):
    
    def setUp(self):
        unittest.TestCase.setUp(self)

class BaseDBTestCase(BaseTestCase):
    """Tests with MongoDB or ElasticSearch
    """
    
    ES_INDEX = "widukind_test"
    
    def setUp(self):
        unittest.TestCase.setUp(self)
        
        self.BACKUP_ES_INDEX = constants.ES_INDEX
        constants.ES_INDEX = self.ES_INDEX
        
        self.db = utils.get_mongo_db()
        self.es = utils.get_es_db()

        self._releases_verify_ok()
        
        utils.clean_mongodb(self.db)
        #utils.clean_es()
        self._clean_elasticsearch()
                
        create_or_update_indexes(self.db, force_mode=True)

    def tearDown(self):
        BaseTest.tearDown(self)
        constants.ES_INDEX = self.BACKUP_ES_INDEX

    
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

        {'cluster_name': 'elasticsearch',
         'name': 'Fateball',
         'status': 200,
         'tagline': 'You Know, for Search',
         'version': {'build_hash': 'b88f43fc40b0bcd7f173a1f9ee2e97816de80b19',
                     'build_snapshot': False,
                     'build_timestamp': '2015-07-29T09:54:16Z',
                     'lucene_version': '4.10.4',
                     'number': '1.7.1'}}
        """
        server_info = self.es.info() 
        server_release = server_info['version']['number']
        server_release_tuple = server_release.split('.')

        if server_release_tuple[0] != "1" or server_release_tuple[1] not in ["6", "7"]:
            self.fail("Not supported ElasticSearch Server [%s] release. Required ElasticSearch 1.6.x or 1.7.x" % server_release)
        
        from elasticsearch import VERSION #(1, 6, 0)
        #TODO: required
        
            
    def _releases_verify_ok(self):

        global VERIFIED_RELEASES
        
        if VERIFIED_RELEASES:
            return True

        self._release_mongodb()
        #TODO: self._release_elasticsearch()
        
        VERIFIED_RELEASES = True

    def _collections_is_empty(self):
        self.assertEqual(self.db[constants.COL_CATEGORIES].count(), 0)
        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 0)
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 0)
        self.assertEqual(self.db[constants.COL_SERIES].count(), 0)
        
    def _clean_elasticsearch(self):
        
        # Création de l'index - FIXME: utiliser un index de test
        try:
            self.es.indices.delete(index=constants.ES_INDEX)
            #self.es.indices.delete_template(name='*', ignore=404)
        except:
            pass

        try:
            self.es.indices.create(constants.ES_INDEX)
        except:
            pass
    
