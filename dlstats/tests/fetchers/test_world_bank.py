# -*- coding: utf-8 -*-

import os

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
        "concept_keys": ['country', 'obs-status'],
        "codelist_keys": ['country', 'obs-status'],
        "codelist_count": {
            "country": 1,
            "obs-status": 2,
        },        
        "dimension_keys": ['country'],
        "dimension_count": {
            "country": 1,
        },
        "attribute_keys": [],
        "attribute_count": None,
    },
    "series_accept": 1,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 20,
    "series_key_first": "NYGDPMKTPKDZ.FRA",
    "series_key_last": "NYGDPMKTPKDZ.FRA",
    "series_sample": {
        'provider_name': 'WORLDBANK',
        'dataset_code': 'GEP',
        'key': 'NYGDPMKTPKDZ.FRA',
        'name': 'Annual percentage growth rate of GDP at market prices based on constant 2010 US Dollars. - France',
        'frequency': 'A',
        'last_update': None,
        'first_value': {
            'value': '',
            'ordinal': 29,
            'period': '1999',
            'attributes': None,
        },
        'last_value': {
            'value': '1.4',
            'ordinal': 48,
            'period': '2018',
            'attributes': None,
        },
        'dimensions': {
            'country': 'FRA',
        },
        'attributes': None,
    }
}


class FetcherTestCase(BaseFetcherTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test_world_bank:FetcherTestCase
    
    FETCHER_KLASS = Fetcher
    
    DATASETS = {
        'GEP': DATA_WB_GEP
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

        url = "http://api.worldbank.org/v2/countries/FRA/indicators/NYGDPMKTPKDZ?format=json&per_page=1000"
        self.register_url(url, 
                          filepath("NYGDPMKTPKDZ-fra.json"))

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
        self.assertDataset(dataset_code)        
        self.assertSeries(dataset_code)

