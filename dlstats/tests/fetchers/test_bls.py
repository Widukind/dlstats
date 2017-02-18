# -*- coding: utf-8 -*-

from datetime import datetime
import os

from dlstats.fetchers.bls import Bls as Fetcher

import httpretty
import unittest

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase


RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "bls"))

def get_filepath(name):
    return os.path.abspath(os.path.join(RESOURCES_DIR, name))

BLS_HTML_PAGES = [
    ("https://www.bls.gov/data/", "Databases, Tables & Calculators by Subject.html")
]

DATA_CU = {
    "filepath": get_filepath("cu/cu.html"),
    "dirname": get_filepath("cu"),
    "code_list_files": ['area', 'base', 'footnote', 'item', 'period', 'periodicity'],
    "series_file" : "series",
    "DSD": {
        "filepath": None,
        "dataset_code": "cu",
        "dsd_id": "cu",
        "is_completed": True,
        "categories_key": "cu",
        "categories_parents": ["BLS", "Inflation"],
        "categories_root": ['BLS'],
        "concept_keys": ['area', 'item', 'seasonal', 'periodicity', 'base', 'base_period', 'period', 'footnote'],
        "codelist_keys": ['area', 'item', 'seasonal', 'periodicity', 'base', 'base_period', 'period', 'footnote'],
        "codelist_count": {
            'area': None,
            'item':None,
            'seasonal': None,
            'periodicity': None,
            'base': None,
            'base_period': None,
        },                
        "dimension_keys": ['area', 'item', 'seasonal', 'periodicity', 'base', 'base_period','period', 'footnote'],
        "dimension_count": {
            'area': None,
            'item':None,
            'seasonal': None,
            'periodicity': None,
            'base': None,
            'base_period': None,
            'period': None,
            'footnote': None,
        },
        "attribute_keys": ['footnotes'],
        "attribute_count": {
            'footnotes': None,
        },
    },
    "series_accept": 22,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 462,
    "series_key_first": "0",
    "series_key_last": "21",
    "series_sample": {
        'provider_name': 'BLS',
        'dataset_code': 'cu',
        'key': '0',
        'name': 'Consumption of Households',
        'frequency': 'A',
        'last_update': None,
        'first_value': {
            'value': '105.7',
            'period': '1994',
            'attributes': None,
        },
        'last_value': {
            'value': '95.2',
            'period': '2014',
            'attributes': None
        },
        'dimensions': {
            "concept" : "0"
        },
        'attributes': None
    }
}


class FetcherTestCase(BaseFetcherTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test_bls:FetcherTestCase
    
    FETCHER_KLASS = Fetcher    
    DATASETS = {
        'cu': DATA_CU,
    }    
    DATASET_FIRST = "cu"
    DATASET_LAST = "cu"
    #DEBUG_MODE = False
    
    def _load_files(self, dataset_code=None):
        
        for url, filename in BLS_HTML_PAGES:
            filepath = get_filepath(filename)
            self.assertTrue(os.path.exists(filepath))
            self.register_url(url, filepath, content_type='text/html')
            
    def _load_files_dataset_cu(self):
        url = "https://download.bls.gov/pub/time.series/cu/"
        self.register_url(url, self.DATASETS["cu"]["filepath"],
                          content_type='text/html')
        url = "https://download.bls.gov/pub/time.series/cu/cu.series"
        self.register_url(url, self.DATASETS["cu"]["dirname"]+"/cu.series",
                          content_type='text')
        print(self.DATASETS["cu"]["dirname"]+"/cu.data.0.Current")
        url = "https://download.bls.gov/pub/time.series/cu/cu.data.0.Current"
        self.register_url(url, self.DATASETS["cu"]["dirname"]+"/cu.data.0.Current",
                          content_type='text')
        print(self.DATASETS["cu"]["dirname"]+"/cu.data.1.AllItems")
        url = "https://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems"
        self.register_url(url, self.DATASETS["cu"]["dirname"]+"/cu.data.1.AllItems",
                          content_type='text')
        for name in self.DATASETS["cu"]["code_list_files"]:
            url = "https://download.bls.gov/pub/time.series/cu/cu." + name
            self.register_url(url, self.DATASETS["cu"]["dirname"]+'/cu.'+name,
                              content_type='text')
            
        
    @httpretty.activate     
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    def test_load_datasets_first(self):

        dataset_code = "cu"
        self._load_files(dataset_code)
        self._load_files_dataset_cu()
        self.assertLoadDatasetsFirst([dataset_code])

    @httpretty.activate     
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    def test_load_datasets_update(self):

        dataset_code = "cu"
        self._load_files(dataset_code)
        self._load_files_dataset_cu()
        self.assertLoadDatasetsUpdate([dataset_code])

    @httpretty.activate
    @unittest.skipUnless(False, 'not yet implemented')
    def test_build_data_tree(self):

        dataset_code = "cu"
        self._load_files(dataset_code)
        self.assertDataTree(dataset_code)
        
        
    @httpretty.activate     
    def test_upsert_dataset_cu(self):

        # nosetests -s -v dlstats.tests.fetchers.test_bls:FetcherTestCase.test_upsert_dataset_cu
        
        dataset_code = "cu"
        self._load_files(dataset_code)
        self._load_files_dataset_cu()
    
        self.assertProvider()
        dataset = self.assertDataset(dataset_code)        
        series_list = self.assertSeries(dataset_code)
        
        dataset["last_update"] = datetime(2017, 2 , 13)

        for series in series_list:
            self.assertEquals(series["last_update_ds"], dataset["last_update"])

