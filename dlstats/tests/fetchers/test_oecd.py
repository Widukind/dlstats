# -*- coding: utf-8 -*-

from copy import deepcopy
import os

import unittest
from unittest import mock

from dlstats.fetchers.oecd import OECD as Fetcher

import httpretty

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase
from dlstats.tests.resources import xml_samples

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "oecd"))

def get_dimensions_from_dsd_MEI(self, xml_dsd=None, provider_name=None, dataset_code=None, dsd_id=None):
    dimension_keys = ['LOCATION', 'SUBJECT', 'MEASURE', 'FREQUENCY']
    dimensions = {
        "LOCATION": {},
        "SUBJECT": {},
        "MEASURE": {},
        "FREQUENCY": {'A': 'A'},
    }
    return dimension_keys, dimensions

def get_dimensions_from_dsd_EO(self, xml_dsd=None, provider_name=None, dataset_code=None, dsd_id=None):
    dimension_keys = ['LOCATION', 'VARIABLE', 'FREQUENCY']
    dimensions = {
        "LOCATION": {},
        "VARIABLE": {},
        "FREQUENCY": {'A': 'A'},
    }
    return dimension_keys, dimensions

LOCAL_DATASETS_UPDATE = {
    "MEI": {
        "dataset_code": "MEI",
        "dsd_id": "MEI",
        "is_completed": True,
        "categories_key": 'MEI',
        "categories_parents": None,
        "categories_root": ['EO', 'MEI'],    
        "concept_keys": ['frequency', 'location', 'measure', 'obs-status', 'powercode', 'referenceperiod', 'subject', 'time-format', 'unit'],
        "codelist_keys": ['frequency', 'location', 'measure', 'obs-status', 'powercode', 'referenceperiod', 'subject', 'time-format', 'unit'],
        "codelist_count": {
            "frequency": 3,
            "location": 64,
            "measure": 24,
            "obs-status": 14,
            "powercode": 17,
            "referenceperiod": 68,
            "subject": 1099,
            "time-format": 5,
            "unit": 296,
        },
        "dimension_keys": ['location', 'subject', 'measure', 'frequency'],
        "dimension_count": {
            "location": 64,
            "subject": 1099,
            "measure": 24,
            "frequency": 3,
        },
        "attribute_keys": ['obs-status', 'time-format', 'unit', 'referenceperiod', 'powercode'],
        "attribute_count": {
            "obs-status": 14,
            "time-format": 5,
            "unit": 296,
            "referenceperiod": 68,
            "powercode": 17,
        }, 
    },
    "EO": {
        "dataset_code": "EO",
        "dsd_id": "EO",
        "is_completed": True,
        "categories_key": 'EO',
        "categories_parents": None,
        "categories_root": ['EO', 'MEI'],    
        "concept_keys": ['frequency', 'location', 'obs-status', 'powercode', 'referenceperiod', 'time-format', 'unit', 'variable'],
        "codelist_keys": ['frequency', 'location', 'obs-status', 'powercode', 'referenceperiod', 'time-format', 'unit', 'variable'],
        "codelist_count": {
            "frequency": 2,
            "location": 59,
            "obs-status": 14,
            "powercode": 17,
            "referenceperiod": 68,
            "time-format": 5,
            "unit": 296,
            "variable": 297,
        },
        "dimension_keys": ['location', 'variable', 'frequency'],
        "dimension_count": {
            "location": 59,
            "variable": 297,
            "frequency": 2,
        },
        "attribute_keys": ['obs-status', 'time-format', 'unit', 'referenceperiod', 'powercode'],
        "attribute_count": {
            "obs-status": 14,
            "time-format": 5,
            "unit": 296,
            "referenceperiod": 68,
            "powercode": 17,
        }, 
            
    },
}

class FetcherTestCase(BaseFetcherTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_oecd:FetcherTestCase
    
    FETCHER_KLASS = Fetcher
    DATASETS = {
        'MEI': deepcopy(xml_samples.DATA_OECD_MEI),
        'EO': deepcopy(xml_samples.DATA_OECD_EO),
    }
    DATASET_FIRST = "EO"
    DATASET_LAST = "MEI"
    DEBUG_MODE = False
    
    def _load_files(self, dataset_code, data_key=None):

        filepaths = self.DATASETS[dataset_code]["DSD"]["filepaths"]

        url = "http://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/%s" % dataset_code
        self.register_url(url, filepaths["datastructure"])
        
        url = "http://stats.oecd.org/restsdmx/sdmx.ashx/GetData/%s/%s" % (dataset_code, data_key)
        self.register_url(url, self.DATASETS[dataset_code]['filepath'])

    @httpretty.activate     
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    def test_load_datasets_first(self):

        dataset_code = 'MEI'
        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])
        self._load_files(dataset_code)
        self.assertLoadDatasetsFirst([dataset_code])

    @httpretty.activate     
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    def test_load_datasets_update(self):

        dataset_code = 'MEI'
        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])
        self._load_files(dataset_code)
        self.assertLoadDatasetsUpdate([dataset_code])

    @httpretty.activate     
    def test_build_data_tree(self):

        dataset_code = 'MEI'
        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])
        #self._load_files(dataset_code)
        self.assertDataTree(dataset_code)
        
    @httpretty.activate     
    @mock.patch('dlstats.fetchers.oecd.OECD_Data._get_dimensions_from_dsd', get_dimensions_from_dsd_MEI)
    def test_upsert_dataset_mei(self):

        # nosetests -s -v dlstats.tests.fetchers.test_oecd:FetcherTestCase.test_upsert_dataset_mei

        dataset_code = 'MEI'
        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])
        self._load_files(dataset_code, data_key="...A")
        
        self.assertProvider()
        self.assertDataset(dataset_code)
        self.assertSeries(dataset_code)

    @httpretty.activate     
    @mock.patch('dlstats.fetchers.oecd.OECD_Data._get_dimensions_from_dsd', get_dimensions_from_dsd_EO)
    def test_upsert_dataset_eo(self):

        # nosetests -s -v dlstats.tests.fetchers.test_oecd:FetcherTestCase.test_upsert_dataset_eo

        dataset_code = 'EO'
        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])
        self._load_files(dataset_code, data_key="..A")
        
        self.assertProvider()
        self.assertDataset(dataset_code)
        self.assertSeries(dataset_code)
        
