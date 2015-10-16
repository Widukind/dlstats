# -*- coding: utf-8 -*-

import os
import urllib.request

from dlstats.fetchers.bis import BIS, LBS_DISS_Data
from dlstats import constants

import unittest

from dlstats.tests.base import RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseDBFetcherTestCase

class Bis_Lbs_Diss_FetcherTestCase(BaseDBFetcherTestCase):
    
    def test_from_csv_local(self):
        """Fetch from csv file in tests/resources directory"""
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:Bis_Lbs_Diss_FetcherTestCase.test_from_csv_local

        self._collections_is_empty()

        csv_file = os.path.abspath(os.path.join(RESOURCES_DIR, "full_BIS_LBS_DISS_csv.zip"))
        self.assertTrue(os.path.exists(csv_file))

        w = BIS(db=self.db, es_client=self.es)
                
        @property
        def _url(self):
            return "file:" + urllib.request.pathname2url(csv_file)        
        setattr(LBS_DISS_Data, 'url', _url)

        w.provider.update_database()
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": w.provider_name})
        self.assertIsNotNone(provider)
        
        w.upsert_categories()
        category = self.db[constants.COL_CATEGORIES].find_one({"provider": w.provider_name, 
                                                               "categoryCode": "LBS-DISS"})
        self.assertIsNotNone(category)
        
        w.upsert_dataset('LBS-DISS')
        dataset = self.db[constants.COL_DATASETS].find_one({"provider": w.provider_name, 
                                                            "datasetCode": "LBS-DISS"})
        self.assertIsNotNone(dataset)
        self.assertEqual(len(dataset["dimensionList"]), 12)

        series = self.db[constants.COL_SERIES].find({"provider": w.provider_name, 
                                                     "datasetCode": "LBS-DISS"})
        self.assertEqual(series.count(), 25)
        
        '''Search one serie by key'''
        serie = self.db[constants.COL_SERIES].find_one({"provider": w.provider_name, 
                                                        "datasetCode": "LBS-DISS",
                                                        "key": "Q:F:C:A:CHF:A:5J:A:5A:A:1C:N"})
        self.assertIsNotNone(serie)
        
        d = serie['dimensions']
        self.assertEqual(d["Frequency"], 'Q')
        self.assertEqual(d["Measure"], 'F')
        self.assertEqual(d["Balance sheet position"], 'C')
        
        serie = self.db[constants.COL_SERIES].find_one({"provider": w.provider_name, 
                                                        "datasetCode": "LBS-DISS",
                                                        "key": "Q:F:C:A:JPY:F:5J:A:5A:A:5J:N"})
        self.assertIsNotNone(serie)

        #TODO: meta_datas tests  
        
    @unittest.skipUnless('FULL_REMOTE_TEST' in os.environ, "Skip - not full remote test")
    def test_from_csv_remote(self):
        """Fetch from csv file in remote site"""

        self._collections_is_empty()

        w = BIS(db=self.db,
                es_client=None, #TODO: 
                #settings={"BIS_LBS_DISS_URL": "file:" + urllib.request.pathname2url(csv_file)}
                )
        
        w.provider.update_database()
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": w.provider_name})
        self.assertIsNotNone(provider)
        
        w.upsert_categories()
        category = self.db[constants.COL_CATEGORIES].find_one({"categoryCode": "LBS-DISS"})
        self.assertIsNotNone(category)
        
        w.upsert_dataset('LBS-DISS')
        dataset = self.db[constants.COL_DATASETS].find_one({"provider": w.provider_name, "datasetCode": "LBS-DISS"})
        self.assertIsNotNone(dataset)
        self.assertTrue(len(dataset["dimensionList"]), 12)

        series = self.db[constants.COL_SERIES].find({"provider": w.provider_name, "datasetCode": "LBS-DISS"})
        self.assertTrue(series.count() > 1)
        
    
