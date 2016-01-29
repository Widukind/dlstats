# -*- coding: utf-8 -*-

from datetime import datetime
import os

from dlstats.fetchers.esri import Esri as Fetcher

import httpretty

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase

import unittest
from unittest import mock

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "esri"))

def get_filepath(name):
    return os.path.abspath(os.path.join(RESOURCES_DIR, name))

ESRI_HTML_PAGES = [
    ("http://www.esri.cao.go.jp/index-e.html", "index-e.html"), 
    ("http://www.esri.cao.go.jp/en/sna/sokuhou/sokuhou_top.html", "sokuhou_top.html"),
    ("http://www.esri.cao.go.jp/en/sna/data/sokuhou/files/toukei_top.html", "toukei_top.html"),
    ("http://www.esri.cao.go.jp/en/sna/data/sokuhou/files/2015/toukei_2015.html", "toukei_2015.html"),
    ("http://www.esri.cao.go.jp/en/sna/data/sokuhou/files/2015/qe153_2/gdemenuea.html", "gdemenuea.html"),
    ("http://www.esri.cao.go.jp/en/stat/di/di-e.html", "di-e.html"),
    ("http://www.esri.cao.go.jp/en/stat/juchu/juchu-e.html", "juchu-e.html"), 
    ("http://www.esri.cao.go.jp/en/stat/shouhi/shouhi-e.html", "shouhi-e.html"),
    ("http://www.esri.cao.go.jp/en/stat/hojin/hojin-e.html", "hojin-e.html"),
    ("http://www.esri.cao.go.jp/en/stat/ank/ank-e.html", "ank-e.html"),
]

DATA_KDED_CY = {
    "filepath": get_filepath("kdef-cy1532.csv"),
    "DSD": {
        "filepath": None,
        "dataset_code": "kdef-cy",
        "dsd_id": "kdef-cy",
        "is_completed": True,
        "categories_key": "Amount",
        "categories_parents": ["SNA", "QuarterlyGDP", "FD"],
        "categories_root": ['SNA'],
        "concept_keys": ['concept'],
        "codelist_keys": ['concept'],
        "codelist_count": {
            "concept": 22,
        },                
        "dimension_keys": ['concept'],
        "dimension_count": {
            "concept": 22,
        },
        "attribute_keys": [],
        "attribute_count": {},
    },
    "series_accept": 22,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 462,
    "series_key_first": "0",
    "series_key_last": "21",
    "series_sample": {
        'provider_name': 'ESRI',
        'dataset_code': 'kdef-cy',
        'key': '0',
        'name': 'Consumption of Households',
        'frequency': 'A',
        'last_update': None,
        'first_value': {
            'value': '105.7',
            'ordinal': 24,
            'period': '1994',
            'period_o': '1994',
            'attributes': None,
        },
        'last_value': {
            'value': '95.2',
            'ordinal': 44,
            'period': '2014',
            'period_o': '2014',
            'attributes': None
        },
        'dimensions': {
            "concept" : "0"
        },
        'attributes': None
    }
}

DATA_KRITU_JG = {
    "filepath": get_filepath("kritu-jg1532.csv"),
    "DSD": {
        "filepath": None,
        "dataset_code": "kritu-jg",
        "dsd_id": "kritu-jg",
        "is_completed": True,
        "categories_key": "Changes",
        "categories_parents": ["SNA", "QuarterlyGDP"],
        "categories_root": ['SNA'],
        "concept_keys": ['concept'],
        "codelist_keys": ['concept'],
        "codelist_count": {
            "concept": 22,
        },                
        "dimension_keys": ['concept'],
        "dimension_count": {
            "concept": 22,
        },
        "attribute_keys": [],
        "attribute_count": {},
    },
    "series_accept": 22,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 1914,
    "series_key_first": "0",
    "series_key_last": "21",
    "series_sample": {
        'provider_name': 'ESRI',
        'dataset_code': 'kritu-jg',
        'key': '0',
        'name': 'Consumption of Households',
        'frequency': 'Q',
        'last_update': None,
        'first_value': {
            'value': 'nan',
            'ordinal': 96,
            'period': '1994Q1',
            'period_o': '1994Q1',
            'attributes': None,
        },
        'last_value': {
            'value': '0.3',
            'ordinal': 182,
            'period': '2015Q3',
            'period_o': '2015Q3',
            'attributes': None
        },
        'dimensions': {
            "concept" : "0"
        },
        'attributes': None
    }
}


class FetcherTestCase(BaseFetcherTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test_esri:FetcherTestCase
    
    FETCHER_KLASS = Fetcher    
    DATASETS = {
        'kdef-cy': DATA_KDED_CY,
        'kritu-jg': DATA_KRITU_JG
    }    
    DATASET_FIRST = "kdef-cy"
    DATASET_LAST = "kritu-mk"
    DEBUG_MODE = False
    
    def _common(self):
        
        for url, filename in ESRI_HTML_PAGES:
            filepath = get_filepath(filename)
            self.assertTrue(os.path.exists(filepath))
            self.register_url(url, filepath, content_type='text/html')
        
    @httpretty.activate     
    def test_upsert_dataset_kdef_cy(self):

        # nosetests -s -v dlstats.tests.fetchers.test_esri:FetcherTestCase.test_upsert_dataset_kdef_cy
        
        dataset_code = "kdef-cy"
        
        self._common()

        url = "http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/kdef-cy1532.csv"
        self.register_url(url, self.DATASETS[dataset_code]["filepath"],
                          content_type='text/html')
    
        self.assertProvider()
        self.assertDataTree(dataset_code)    
        self.assertDataset(dataset_code)        
        self.assertSeries(dataset_code)

    @httpretty.activate     
    def test_upsert_dataset_kritu_jg(self):

        # nosetests -s -v dlstats.tests.fetchers.test_esri:FetcherTestCase.test_upsert_dataset_kritu_jg
        
        dataset_code = "kritu-jg"
        
        self._common()

        url = "http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/kritu-jg1532.csv"
        self.register_url(url, self.DATASETS[dataset_code]["filepath"],
                          content_type='text/html')
    
        self.assertProvider()
        self.assertDataTree(dataset_code)    
        self.assertDataset(dataset_code)        
        self.assertSeries(dataset_code)

