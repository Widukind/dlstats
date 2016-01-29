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
    "G19": {
        'categories_root': ['PEI', 'BAL', 'BF', 'ERID', 'FA', 'HF', 'IA', 'IR', 'MSRB'],
        "codelist_keys": ['FREQ', 'UNIT', 'CURRENCY', 'DATAREP', 'SERIES_NAME', 'TERMS', 'OBS_STATUS', 'CREDTYP', 'SA', 'ISSUE', 'UNIT_MULT', 'HOLDER'],
        "codelist_count": {
            "FREQ": 50,
            "UNIT": 413,
            "CURRENCY": 200,
            "DATAREP": 3,
            "SERIES_NAME": 0,
            "TERMS": 8,
            "OBS_STATUS": 4,
            "CREDTYP": 5,
            "SA": 2,
            "ISSUE": 2,
            "UNIT_MULT": 9,
            "HOLDER": 10,
        },
                
    }
}

class FetcherTestCase(BaseFetcherTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_fed:FetcherTestCase
    
    FETCHER_KLASS = Fetcher
    DATASETS = {
        "G19": deepcopy(xml_samples.DATA_FED)
    }
    DATASET_FIRST = "CHGDEL"
    DATASET_LAST = "Z1"
    DEBUG_MODE = False

    def _common(self, dataset_code):
        url = FETCHER_DATASETS[dataset_code]["url"]
        self.register_url(url, 
                          self.DATASETS[dataset_code]["filepath"])

    @httpretty.activate     
    def test_upsert_dataset_g19(self):
        
        dataset_code = "G19"
        
        self.DATASETS[dataset_code]["DSD"]["categories_root"] = LOCAL_DATASETS_UPDATE[dataset_code]["categories_root"]
        self.DATASETS[dataset_code]["DSD"]["codelist_keys"] = LOCAL_DATASETS_UPDATE[dataset_code]["codelist_keys"]
        self.DATASETS[dataset_code]["DSD"]["codelist_count"] = LOCAL_DATASETS_UPDATE[dataset_code]["codelist_count"]

        dataset_zip_filepath = os.path.abspath(os.path.join(RESOURCES_DIR, "FRB_G19.zip"))
        self.DATASETS[dataset_code]["filepath"] = dataset_zip_filepath
        
        self._common(dataset_code)
     
        self.assertProvider()
        self.assertDataTree(dataset_code)
        self.assertDataset(dataset_code)        
        self.assertSeries(dataset_code)

