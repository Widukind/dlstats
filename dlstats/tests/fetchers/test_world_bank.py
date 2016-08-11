# -*- coding: utf-8 -*-

from datetime import datetime
import os
import re

import httpretty

from dlstats.fetchers.world_bank import WorldBankAPI as Fetcher

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase

import unittest
from unittest import mock

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "world_bank"))

def filepath(filename):
    return os.path.abspath(os.path.join(RESOURCES_DIR, filename))

def available_countries(self):
    return {
        "FRA": {
            'adminregion': {'id': '', 'iso2code': '', 'value': ''},
            'capitalCity': 'Paris',
             'id': 'FRA',
             'incomeLevel': {'id': 'OEC', 'iso2code': 'XS', 'value': 'High income: OECD'},
             'iso2Code': 'FR',
             'latitude': '48.8566',
             'lendingType': {'id': 'LNX', 'iso2code': 'XX', 'value': 'Not classified'},
             'longitude': '2.35097',
             'name': 'France',
             'region': {'id': 'ECS',
                        'iso2code': 'Z7',
                        'value': 'Europe & Central Asia (all income levels)'}
        } 
    }    

DATA_WB_GEP = {
    "DSD": {
        "provider": "WORLDBANK",
        "filepath": None,
        "dataset_code": "GEP",
        "dsd_id": "GEP",
        "is_completed": True,
        "categories_key": "GEP",
        "categories_parents": None,
        "categories_root": ['ADI', 'DBS', 'ED1', 'EDS', 'FDX', 'G2F', 'GDS', 'GEM', 'GEP', 'GFD', 'GMC', 'GPE', 'HNP', 'HNQ', 'HPP', 'IDA', 'IDS', 'JOB', 'MDG', 'POV', 'PSD', 'QDG', 'QDS', 'SE4', 'SNM', 'SNP', 'SNT', 'WAT', 'WDI', 'WGI'],
        "frequencies": ['A'],#, 'M', 'Q'],
        #'last_update': datetime(2016, 4, 5, 0, 0),
        "concept_keys": ['country', 'indicator', 'frequency', 'obs-status'],
        "codelist_keys": ['country', 'indicator', 'frequency', 'obs-status'],
        "codelist_count": {
            "country": 1,
            "obs-status": 2,
            "indicator": 1,
            "frequency": 1,
        },        
        "dimension_keys": ['indicator', 'country', 'frequency'],
        "dimension_count": {
            "indicator": 1,
            "country": 1,
            "frequency": 1,
        },
        "attribute_keys": ['obs-status'],
        "attribute_count": {
            'obs-status': 2,
        }
    },
    "series_accept": 1,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 20,
    "series_key_first": "NYGDPMKTPKDZ.FRA.A",
    "series_key_last": "NYGDPMKTPKDZ.FRA.A",
    "series_sample": {
        'provider_name': 'WORLDBANK',
        'dataset_code': 'GEP',
        'key': 'NYGDPMKTPKDZ.FRA.A',
        'name': 'Annual percentage growth rate of GDP at market prices based on constant 2010 US Dollars. - France - Annually',
        'frequency': 'A',
        'first_value': {
            'value': '',
            'period': '1999',
            'attributes': None,
        },
        'last_value': {
            'value': '1.4',
            'period': '2018',
            'attributes': None,
        },
        'dimensions': {
            'country': 'FRA',
            'frequency': 'A',
            'indicator': 'NYGDPMKTPKDZ',
        },
        'attributes': None,
    }
}

DATA_WB_GEM = {
    "DSD": {
        "provider": "WORLDBANK",
        "filepath": filepath("GemDataEXTR.zip"),
        "dataset_code": "GEM",
        "dsd_id": "GEM",
        "is_completed": True,
        "categories_key": "GEM",
        "categories_parents": None,
        "categories_root": [],
        "frequencies": ['A', 'M'],
        'last_update': datetime(2016, 4, 5, 15, 5, 11),
        "concept_keys": ['country'],
        "codelist_keys": ['country'],
        "codelist_count": {
            "country": 14,
        },        
        "dimension_keys": ['country'],
        "dimension_count": {
            "country": 14,
        },
        "attribute_keys": [],
        "attribute_count": None,
    },
    "series_accept": 28,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 3640,
    "series_key_first": "cpi-price-y-o-y-median-weighted-seas-adj-developing-asia-annually",
    "series_key_last": "cpi-price-y-o-y-median-weighted-seas-adj-world-wbg-members-monthly",
    "series_sample": {
        'provider_name': 'WORLDBANK',
        'dataset_code': 'GEM',
        'key': 'cpi-price-y-o-y-median-weighted-seas-adj-developing-asia-annually',
        'name': 'CPI Price, % y-o-y, median weighted, seas. adj. - Developing Asia - Annually',
        'frequency': 'A',
        'first_value': {
            'value': '6.22842',
            'period': '1997',
            'attributes': None,
        },
        'last_value': {
            'value': '1.474426',
            'period': '2016',
            'attributes': None,
        },
        'dimensions': {
            'country': 'widukind-asia-dev',
        },
        'attributes': None,
    }
}

class FetcherTestCase(BaseFetcherTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test_world_bank:FetcherTestCase
    
    FETCHER_KLASS = Fetcher
    
    DATASETS = {
        'GEP': DATA_WB_GEP,
        'GEM': DATA_WB_GEM,
    }
    
    DATASET_FIRST = "ADI"
    DATASET_LAST = "WGI"
    DEBUG_MODE = False

    def _load_files_gep(self):
        
        url = "http://api.worldbank.org/v2/sources?format=json&per_page=1000"
        self.register_url(url, 
                          filepath("sources.json"))
        
        url = "http://api.worldbank.org/v2/countries?per_page=1000&format=json"
        self.register_url(url, 
                          filepath("countries.json"))

        url = "http://api.worldbank.org/v2/sources/27/indicators?format=json&per_page=1000"
        self.register_url(url, 
                          filepath("gep-indicator.json"))

        url = "http://api.worldbank.org/v2/countries/FRA/indicators/NYGDPMKTPKDZ?(.*)"
        self.register_url(url, 
                          filepath("NYGDPMKTPKDZ-fra.json"))

    def _load_files_excel_gem(self):

        url = re.compile("http://api.worldbank.org/v2/sources?(.*)")
        self.register_url(url, 
                          filepath("sources.json"),
                          content_type="application/json")
        
        url = re.compile("http://api.worldbank.org/v2/countries?(.*)")
        self.register_url(url, 
                          filepath("countries.json"),
                          content_type="application/json")
        
        url = "http://siteresources.worldbank.org/INTPROSPECTS/Resources/GemDataEXTR.zip"
        self.register_url(url, 
                          self.DATASETS["GEM"]["DSD"]["filepath"],
                          **{"Last-Modified": "Tue, 05 Apr 2016 15:05:11 GMT"})

    @httpretty.activate
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    def test_load_datasets_first(self):

        dataset_code = "GEP"
        self._load_files_gep()
        self.assertLoadDatasetsFirst([dataset_code])

    @httpretty.activate     
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    def test_load_datasets_update(self):

        dataset_code = "GEP"
        self._load_files_gep()
        self.assertLoadDatasetsUpdate([dataset_code])

    @httpretty.activate     
    def test_build_data_tree(self):

        dataset_code = "GEP"
        self._load_files_gep()
        self.assertDataTree(dataset_code)
            
    @httpretty.activate
    @mock.patch("dlstats.fetchers.world_bank.WorldBankAPI.available_countries", available_countries)     
    def test_upsert_dataset_gep(self):

        # nosetests -s -v dlstats.tests.fetchers.test_world_bank:FetcherTestCase.test_upsert_dataset_gep
    
        dataset_code = "GEP"
        self._load_files_gep()
        self.assertProvider()
        dataset = self.assertDataset(dataset_code)        
        series_list = self.assertSeries(dataset_code)
        
        self.assertTrue(dataset["last_update"] > datetime(2016, 1, 6))
        self.assertEquals(series_list[0]["last_update_ds"], datetime(2016, 1, 6))
        self.assertEquals(series_list[-1]["last_update_ds"], datetime(2016, 1, 6))

    @httpretty.activate
    @mock.patch("dlstats.fetchers.world_bank.WorldBankAPI.available_countries", available_countries)
    def test_upsert_dataset_excel_gem(self):

        # nosetests -s -v dlstats.tests.fetchers.test_world_bank:FetcherTestCase.test_upsert_dataset_excel_gem
    
        dataset_code = "GEM"
        self._load_files_excel_gem()
        self.assertProvider()
        dataset = self.assertDataset(dataset_code)        
        series_list = self.assertSeries(dataset_code)
        
        self.assertEquals(dataset["last_update"], datetime(2016, 4, 5, 15, 5, 11))
        self.assertEquals(series_list[0]["last_update_ds"], datetime(2016, 4, 5, 15, 5, 11))
        self.assertEquals(series_list[-1]["last_update_ds"], datetime(2016, 4, 5, 15, 5, 11))

