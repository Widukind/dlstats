# -*- coding: utf-8 -*-

import os
import urllib.request

from dlstats.fetchers.bis import BIS, load_zip_file

import unittest
from ..base import BaseFetcherDBTest, RESOURCES_DIR


class Bis_Lbs_Diss_FetcherTestCase(BaseFetcherDBTest):
    """
    TODO: 
    
    1. open and verify mongodb and ES index
        - connection error
        - access right error
    2. download file or load file
        - local: file not found
        - remote: 404 error, connect error
    3. Search update file (filename or attribute file or text in file)    
        3.1. Verify updated data    
    4. parse file
        - parse error
    5. Create/Update mongodb:
        5.1 categories
        5.2 datasets
        5.3 series
    """
    
    
    def test_from_csv_local(self):
        """Fetch from csv file in tests/resources directory"""

        self._collections_is_empty()

        csv_file = os.path.abspath(os.path.join(RESOURCES_DIR, "full_BIS_LBS_DISS_csv.zip"))
        self.assertTrue(os.path.exists(csv_file))

        w = BIS(db=self.db,
                es_client=None, #TODO: 
                settings={"BIS_LBS_DISS_URL": "file:" + urllib.request.pathname2url(csv_file)})

        w.provider.update_database()
        provider = self.db.providers.find_one({"name": w.provider_name})
        self.assertIsNotNone(provider)
        
        w.upsert_categories()
        category = self.db.categories.find_one({"categoryCode": "LBS-DISS"})
        self.assertIsNotNone(category)
        
        w.upsert_dataset('LBS-DISS')
        dataset = self.db.datasets.find_one({"provider": w.provider_name, "datasetCode": "LBS-DISS"})
        self.assertIsNotNone(dataset)
        self.assertEqual(len(dataset["dimensionList"]), 13)

        series = self.db.series.find({"provider": w.provider_name, "datasetCode": "LBS-DISS"})
        self.assertEqual(series.count(), 25)

        #TODO
        """        
        es_data = self.es.search(index='widukind', doc_type='datasets',
                                    body= { "filter":
                                           { "term":
                                            { "_id": "BIS" + '.' + "LBS-DISS"}}})
        """
        
        
    def test_from_csv_remote(self):
        """Fetch from csv file in remote site"""

        self._collections_is_empty()

        w = BIS(db=self.db,
                es_client=None, #TODO: 
                #settings={"BIS_LBS_DISS_URL": "file:" + urllib.request.pathname2url(csv_file)}
                )

        w.provider.update_database()
        provider = self.db.providers.find_one({"name": w.provider_name})
        self.assertIsNotNone(provider)
        
        w.upsert_categories()
        category = self.db.categories.find_one({"categoryCode": "LBS-DISS"})
        self.assertIsNotNone(category)
        
        w.upsert_dataset('LBS-DISS')
        dataset = self.db.datasets.find_one({"provider": w.provider_name, "datasetCode": "LBS-DISS"})
        self.assertIsNotNone(dataset)

        series = self.db.series.find({"provider": w.provider_name, "datasetCode": "LBS-DISS"})
        self.assertTrue(series.count() > 1)
        
    
