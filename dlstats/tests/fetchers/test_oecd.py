# -*- coding: utf-8 -*-

from copy import deepcopy
import os

import unittest

from dlstats.fetchers.oecd import OECD as Fetcher

import httpretty

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase
from dlstats.tests.resources import xml_samples

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "oecd"))

LOCAL_DATASETS_UPDATE = {
    "MEI": {
        "dataset_code": "MEI",
        "dsd_id": "MEI",
        "is_completed": True,
        "categories_key": 'MEI',
        "categories_parents": None,
        "categories_root": ['EO', 'MEI'],    
        "concept_keys": ['FREQUENCY', 'LOCATION', 'MEASURE', 'OBS_STATUS', 'POWERCODE', 'REFERENCEPERIOD', 'SUBJECT', 'TIME_FORMAT', 'UNIT'],
        "codelist_keys": ['FREQUENCY', 'LOCATION', 'MEASURE', 'OBS_STATUS', 'POWERCODE', 'REFERENCEPERIOD', 'SUBJECT', 'TIME_FORMAT', 'UNIT'],
        "codelist_count": {
            "FREQUENCY": 2,
            "LOCATION": 2,
            "MEASURE": 1,
            "OBS_STATUS": 1,
            "POWERCODE": 1,
            "REFERENCEPERIOD": 1,
            "SUBJECT": 1,
            "TIME_FORMAT": 2,
            "UNIT": 1,
        },
        "dimension_keys": ['LOCATION', 'SUBJECT', 'MEASURE', 'FREQUENCY'],
        "dimension_count": {
            "LOCATION": 2,
            "SUBJECT": 1,
            "MEASURE": 1,
            "FREQUENCY": 2,
        },
        "attribute_keys": ['OBS_STATUS', 'TIME_FORMAT', 'UNIT', 'REFERENCEPERIOD', 'POWERCODE'],
        "attribute_count": {
            "OBS_STATUS": 1,
            "TIME_FORMAT": 2,
            "UNIT": 1,
            "REFERENCEPERIOD": 1,
            "POWERCODE": 1,
        }, 
    },
    "EO": {
        "dataset_code": "EO",
        "dsd_id": "EO",
        "is_completed": True,
        "categories_key": 'EO',
        "categories_parents": None,
        "categories_root": ['EO', 'MEI'],    
        "concept_keys": ['FREQUENCY', 'LOCATION', 'OBS_STATUS', 'POWERCODE', 'REFERENCEPERIOD', 'TIME_FORMAT', 'UNIT', 'VARIABLE'],
        "codelist_keys": ['FREQUENCY', 'LOCATION', 'OBS_STATUS', 'POWERCODE', 'REFERENCEPERIOD', 'TIME_FORMAT', 'UNIT', 'VARIABLE'],
        "codelist_count": {
            "FREQUENCY": 2,
            "LOCATION": 2,
            "OBS_STATUS": 0,
            "POWERCODE": 1,
            "REFERENCEPERIOD": 0,
            "TIME_FORMAT": 2,
            "UNIT": 1,
            "VARIABLE": 1,
        },
        "dimension_keys": ['LOCATION', 'VARIABLE', 'FREQUENCY'],
        "dimension_count": {
            "LOCATION": 2,
            "VARIABLE": 1,
            "FREQUENCY": 2,
        },
        "attribute_keys": ['OBS_STATUS', 'TIME_FORMAT', 'UNIT', 'REFERENCEPERIOD', 'POWERCODE'],
        "attribute_count": {
            "OBS_STATUS": 0,
            "TIME_FORMAT": 2,
            "UNIT": 1,
            "REFERENCEPERIOD": 0,
            "POWERCODE": 1,
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
    
    def _load_files(self, dataset_code):

        filepaths = self.DATASETS[dataset_code]["DSD"]["filepaths"]

        url = "http://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/%s" % dataset_code
        self.register_url(url, filepaths["datastructure"])
        
        url = "http://stats.oecd.org/restsdmx/sdmx.ashx/GetData/%s" % dataset_code
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
    def test_upsert_dataset_mei(self):

        # nosetests -s -v dlstats.tests.fetchers.test_oecd:FetcherTestCase.test_upsert_dataset_mei

        dataset_code = 'MEI'
        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])
        self._load_files(dataset_code)
        
        self.assertProvider()
        self.assertDataset(dataset_code)
        self.assertSeries(dataset_code)

    @httpretty.activate     
    def test_upsert_dataset_eo(self):

        # nosetests -s -v dlstats.tests.fetchers.test_oecd:FetcherTestCase.test_upsert_dataset_eo

        dataset_code = 'EO'
        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])
        self._load_files(dataset_code)
        
        self.assertProvider()
        self.assertDataset(dataset_code)
        self.assertSeries(dataset_code)
        
    #@httpretty.activate
    @unittest.skipIf(True, "TODO")
    def test__parse_agenda(self):
        pass
        
    #@httpretty.activate
    @unittest.skipIf(True, "TODO")
    def test_get_calendar(self):
        pass

