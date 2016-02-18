# -*- coding: utf-8 -*-

from copy import deepcopy
import os
from dlstats.fetchers.fed import FED as Fetcher
from dlstats.fetchers.fed import DATASETS as FETCHER_DATASETS

import unittest
import httpretty

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase
from dlstats.tests.resources import xml_samples

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "fed"))

LOCAL_DATASETS_UPDATE = {
    "G19-TERMS": {
        #'categories_root': ['BAL', 'BF', 'ERID', 'FA', 'HF', 'IA', 'IR', 'MSRB', 'PEI'],
        "concept_keys": ['CURRENCY', 'FREQ', 'ISSUE', 'OBS_STATUS', 'SERIES_NAME', 'TERMS', 'UNIT', 'UNIT_MULT'],
        "codelist_keys": ['CURRENCY', 'FREQ', 'ISSUE', 'OBS_STATUS', 'SERIES_NAME', 'TERMS', 'UNIT', 'UNIT_MULT'],
        "codelist_count": {
            "CURRENCY": 1,
            "FREQ": 1,
            "ISSUE": 2,
            "OBS_STATUS": 1,
            "SERIES_NAME": 0,
            "TERMS": 8,
            "UNIT": 3,
            "UNIT_MULT": 1,
        },
        "dimension_keys": ['ISSUE', 'TERMS', 'FREQ'],
        "dimension_count": {
            "ISSUE": 2,
            "TERMS": 8,
            "FREQ": 1,
        },
        "attribute_keys": ['OBS_STATUS', 'CURRENCY', 'UNIT', 'UNIT_MULT', 'SERIES_NAME'],
        "attribute_count": {
            "OBS_STATUS": 1,
            "CURRENCY": 1,
            "UNIT": 3,
            "UNIT_MULT": 1,
            "SERIES_NAME": 0,
        }
    }
}

class FetcherTestCase(BaseFetcherTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_fed:FetcherTestCase
    
    FETCHER_KLASS = Fetcher
    DATASETS = {
        "G19-TERMS": deepcopy(xml_samples.DATA_FED_TERMS)
    }
    DATASET_FIRST = "CHGDEL-CHGDEL"
    DATASET_LAST = "Z.1-Z1"
    DEBUG_MODE = False
    
    def _load_files(self, dataset_code):
        
        if dataset_code in ["G19-TERMS", "G19-CCOUT"]:
            dataset_zip_filepath = os.path.abspath(os.path.join(RESOURCES_DIR, "FRB_G19.zip"))
            self.DATASETS[dataset_code]["filepath"] = dataset_zip_filepath

        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])
        
        url = FETCHER_DATASETS[dataset_code]["url"]
        self.register_url(url, 
                          self.DATASETS[dataset_code]["filepath"],
                          match_querystring=False)

    @httpretty.activate
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    def test_load_datasets_first(self):

        dataset_code = "G19-TERMS"
        self._load_files(dataset_code)
        self.assertLoadDatasetsFirst(datasets_filter=[dataset_code])

    @httpretty.activate     
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    def test_load_datasets_update(self):

        dataset_code = "G19-TERMS"
        self._load_files(dataset_code)
        self.assertLoadDatasetsUpdate(datasets_filter=[dataset_code])

    @httpretty.activate     
    def test_build_data_tree(self):

        dataset_code = "G19-TERMS"
        self.assertDataTree(dataset_code)

    @httpretty.activate     
    def test_upsert_dataset_g19_terms(self):

        # nosetests -s -v dlstats.tests.fetchers.test_fed:FetcherTestCase.test_upsert_dataset_g19_terms
        
        dataset_code = "G19-TERMS"
        self._load_files(dataset_code)
        self.assertProvider()
        self.assertDataset(dataset_code)
        self.assertSeries(dataset_code)

