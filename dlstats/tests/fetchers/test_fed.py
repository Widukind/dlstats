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
        "concept_keys": ['currency', 'freq', 'issue', 'obs-status', 'series-name', 'terms', 'unit', 'unit-mult'],
        "codelist_keys": ['currency', 'freq', 'issue', 'obs-status', 'series-name', 'terms', 'unit', 'unit-mult'],
        "codelist_count": {
            "currency": 200,
            "freq": 50,
            "issue": 2,
            "obs-status": 4,
            "series-name": 0,
            "terms": 8,
            "unit": 413,
            "unit-mult": 8,
        },
        "dimension_keys": ['issue', 'terms', 'freq'],
        "dimension_count": {
            "issue": 2,
            "terms": 8,
            "freq": 50,
        },
        "attribute_keys": ['obs-status', 'currency', 'unit', 'unit-mult', 'series-name'],
        "attribute_count": {
            "obs-status": 4,
            "currency": 200,
            "unit": 413,
            "unit-mult": 8,
            "series-name": 0,
        }
    }
}

class FetcherTestCase(BaseFetcherTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_fed:FetcherTestCase
    
    FETCHER_KLASS = Fetcher
    DATASETS = {
        "G19-TERMS": deepcopy(xml_samples.DATA_FED_TERMS)
    }
    DATASET_FIRST = "CHGDEL"
    DATASET_LAST = "Z1"
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

    @unittest.skipIf(True, "FIXME")
    def test_build_data_tree(self):

        #FIXME: two categories for G19-TERMS
        # nosetests -s -v dlstats.tests.fetchers.test_fed:FetcherTestCase.test_build_data_tree

        dataset_code = "G19-TERMS"
        self.assertDataTree(dataset_code)

    @httpretty.activate     
    def test_upsert_dataset_g19_terms(self):

        # nosetests -s -v dlstats.tests.fetchers.test_fed:FetcherTestCase.test_upsert_dataset_g19_terms
        
        dataset_code = "G19-TERMS"
        self._load_files(dataset_code)
        self.DATASETS[dataset_code]["series_sample"]["attributes"].pop("SERIES_NAME", None)
        self.assertProvider()
        dataset = self.assertDataset(dataset_code)
        series_list = self.assertSeries(dataset_code)
        
        for series in series_list:
            self.assertEquals(series["last_update_ds"], dataset["last_update"])
        

