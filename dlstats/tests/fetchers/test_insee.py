# -*- coding: utf-8 -*-

import os
import urllib.request

import sdmx
from dlstats.fetchers.insee2 import INSEE, INSEE_Data
from dlstats import constants

import unittest

from dlstats.tests.base import RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseDBFetcherTestCase

class INSEE_FetcherTestCase(BaseDBFetcherTestCase):
    
    def test_from_sdmx_local(self):
        """Fetch from sdmx files in tests/resources directory"""
        
        # nosetests -s -v dlstats.tests.fetchers.test_insee:INSEE_FetcherTestCase.test_from_sdmx_local

        self._collections_is_empty()

        categoryscheme_xml = os.path.abspath(os.path.join(RESOURCES_DIR,'insee', "insee_categoryscheme.xml"))
        self.assertTrue(os.path.exists(categoryscheme_xml))
        dataflow_xml = os.path.abspath(os.path.join(RESOURCES_DIR,'insee', "dataflow"))
        self.assertTrue(os.path.exists(dataflow_xml))

        w = INSEE(db=self.db, es_client=self.es)

        @property
        def _categorisation(self):
            categorisation_xml = os.path.abspath(os.path.join(RESOURCES_DIR,'insee'))
            sdmx.insee = sdmx.Repository("file:" + urllib.request.pathname2url(categorisation_xml),'2_1','INSEE')
            return sdmx.insee.categorisation
        setattr(INSEE,'categorisation',_categorisation)

        @property
        def _dataflows(self):
            dataflow_xml = os.path.abspath(os.path.join(RESOURCES_DIR,'insee'))
            sdmx.insee = sdmx.Repository('file:' + urllib.request.pathname2url(dataflow_xml),'2_1','INSEE')
            sdmx.insee.dataflow_url = 'file:' + urllib.request.pathname2url(dataflow_xml+'/dataflow')
            return sdmx.insee.dataflows()
        setattr(INSEE,'dataflows',_dataflows)

        @property
        def _categories(self):
            categoryscheme_xml = os.path.abspath(os.path.join(RESOURCES_DIR,'insee', "insee_categoryscheme.xml"))
            sdmx.insee = sdmx.Repository('file:' + urllib.request.pathname2url(categoryscheme_xml),'2_1','INSEE')
            sdmx.insee.category_scheme_url = 'file:' + urllib.request.pathname2url(categoryscheme_xml)
            return sdmx.insee.categories
        setattr(INSEE,'categories',_categories)

        w.provider.update_database()
        res = self.db[constants.COL_PROVIDERS].find()
        for r in res:
            print(r)
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": w.provider_name})
        self.assertIsNotNone(provider)
        
        w.upsert_categories()
        count = self.db[constants.COL_CATEGORIES].count()
        self.assertEqual(count,7)
        category = self.db[constants.COL_CATEGORIES].find_one({"provider": w.provider_name, 
                                                               "categoryCode": w.provider_name + '_root'})
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
        
if __name__ == '__main__':
    i = INSEE_FetcherTestCase()
    i.test_from_sdmx_local()
