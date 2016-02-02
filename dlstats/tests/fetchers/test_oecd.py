# -*- coding: utf-8 -*-

from pprint import pprint
import os
import datetime
import json

from dlstats.fetchers.oecd import OECD as Fetcher
from dlstats.fetchers.oecd import json_dataflow, json_data, load_dataflow, load_data

import unittest
from unittest import mock
import httpretty

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase, body_generator

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "oecd"))

DSD_OECD_MEI = {
    "provider": "OECD",
    "filepaths": {
        "dataflow": os.path.abspath(os.path.join(RESOURCES_DIR, "oecd-mei-dataflow.json")),
    },
    "dataset_code": "MEI",
    "dsd_id": "MEI",
    "is_completed": True,
    "categories_key": 'MEI',
    "categories_parents": None,
    "categories_root": ['EO', 'MEI'],    
    "concept_keys": ['UNIT', 'OBS_STATUS', 'SUBJECT', 'LOCATION', 'TIME_FORMAT', 'FREQUENCY', 'MEASURE', 'TIME_PERIOD', 'POWERCODE', 'REFERENCEPERIOD'],
    "codelist_keys": ['UNIT', 'OBS_STATUS', 'SUBJECT', 'LOCATION', 'TIME_FORMAT', 'FREQUENCY', 'MEASURE', 'TIME_PERIOD', 'POWERCODE', 'REFERENCEPERIOD'],
    "codelist_count": {
        "UNIT": 295,
        "OBS_STATUS": 14,
        "SUBJECT": 1097,
        "LOCATION": 64,
        "TIME_FORMAT": 5,
        "FREQUENCY": 3,
        "MEASURE": 24,
        "TIME_PERIOD": 1168,
        "POWERCODE": 32,
        "REFERENCEPERIOD": 68,    
    },
    "dimension_keys": ['LOCATION', 'SUBJECT', 'MEASURE', 'FREQUENCY'],
    "dimension_count": {
        "LOCATION": 64,
        "SUBJECT": 1097,
        "MEASURE": 24,
        "FREQUENCY": 3,
    },
    "attribute_keys": ['UNIT', 'POWERCODE', 'REFERENCEPERIOD', 'TIME_FORMAT', 'OBS_STATUS'],
    "attribute_count": {
        "UNIT": 295,
        "POWERCODE": 32,
        "REFERENCEPERIOD": 68,
        "TIME_FORMAT": 5,
        "OBS_STATUS": 14,
    }, 
}

DSD_OECD_EO = {
    "provider": "OECD",
    "filepaths": {
        "dataflow": os.path.abspath(os.path.join(RESOURCES_DIR, "oecd-eo-dataflow.json")),
    },
    "dataset_code": "EO",
    "dsd_id": "EO",
    "is_completed": True,
    "categories_key": 'EO',
    "categories_parents": None,
    "categories_root": ['EO', 'MEI'],    
    "concept_keys": ['FREQUENCY', 'VARIABLE', 'TIME_FORMAT', 'TIME_PERIOD', 'UNIT', 'POWERCODE', 'LOCATION', 'OBS_STATUS', 'REFERENCEPERIOD'],
    "codelist_keys": ['FREQUENCY', 'VARIABLE', 'TIME_FORMAT', 'TIME_PERIOD', 'UNIT', 'POWERCODE', 'LOCATION', 'OBS_STATUS', 'REFERENCEPERIOD'],
    "codelist_count": {
        "FREQUENCY": 2,
        "VARIABLE": 297,
        "TIME_FORMAT": 5,
        "TIME_PERIOD": 406,
        "UNIT": 295,
        "POWERCODE": 32,
        "LOCATION": 59,
        "OBS_STATUS": 14,
        "REFERENCEPERIOD": 68,
    },
    "dimension_keys": ['LOCATION', 'VARIABLE', 'FREQUENCY'],
    "dimension_count": {
        "LOCATION": 59,
        "VARIABLE": 297,
        "FREQUENCY": 2,
    },
    "attribute_keys": ['UNIT', 'POWERCODE', 'REFERENCEPERIOD', 'TIME_FORMAT', 'OBS_STATUS'],
    "attribute_count": {
        "UNIT": 295,
        "POWERCODE": 32,
        "REFERENCEPERIOD": 68,
        "TIME_FORMAT": 5,
        "OBS_STATUS": 14,
    }, 
}

DATA_OECD_MEI = {
    "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "oecd-mei-FRA.XTNTVA01.x.A.json")),
    "DSD": DSD_OECD_MEI,
    "series_accept": 4,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 208,
    "series_key_first": "FRA.XTNTVA01.NCML.A",
    "series_key_last": "FRA.XTNTVA01.CXML.A",
    "series_sample": {
        'provider_name': 'OECD',
        'dataset_code': 'MEI',
        'key': 'FRA.XTNTVA01.NCML.A',
        'name':  'France - International Trade > Net trade > Value (goods) > Total - National currency, monthly level - Annual',
        'frequency': 'A',
        'last_update': None,
        'first_value': {
            'value': '0.0589520349657065',
            'ordinal': -15,
            'period': '1955',
            'period_o': '1955',
            'attributes': None,
        },
        'last_value': {
            'value': '-72.3617',
            'ordinal': 44,
            'period': '2014',
            'period_o': '2014',
            'attributes': None,
        },
        'dimensions': {
          'FREQUENCY': 'A', 
          'LOCATION': 'FRA', 
          'MEASURE': 'NCML', 
          'SUBJECT': 'XTNTVA01'
        },
        'attributes': {
            'POWERCODE': '9', 
            'TIME_FORMAT': 'P1Y', 
            'UNIT': 'EUR'
        },
    }
}

DATA_OECD_EO = {
    "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "oecd-eo-AUS.CB.x.json")),
    "DSD": DSD_OECD_EO,
    "series_accept": 2,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 290,
    "series_key_first": "AUS.CB.A",
    "series_key_last": "AUS.CB.Q",
    "series_sample": {
        'provider_name': 'OECD',
        'dataset_code': 'EO',
        'key': 'AUS.CB.A',
        'name': 'Australia - Current account balance, value - Annual',
        'frequency': 'A',
        'last_update': None,
        'first_value': {
            'value': '-638000000.0',
            'ordinal': -10,
            'period': '1960',
            'period_o': '1960',
            'attributes': None,
        },
        'last_value': {
            'value': '-64025866378.843',
            'ordinal': 47,
            'period': '2017',
            'period_o': '2017',
            'attributes': None,
        },
        'dimensions': {
          'LOCATION': 'AUS',
          'VARIABLE': 'CB',
          'FREQUENCY': 'A'
        },
        'attributes': {
            'TIME_FORMAT': 'P1Y', 
            'UNIT': 'EUR',
            'POWERCODE': '0', 
        },
    }
}

ALL_DATASETS = {
    'MEI': DATA_OECD_MEI,
    'EO': DATA_OECD_EO
}

def _get_url_data_mock_MEI(self, flowkey):
    return "http://stats.oecd.org/sdmx-json/data/MEI/FRA.BPFAFD01.."

def _select_filter_dimension_mock_MEI(self):
    return 0, 4, 'LOCATION', ["FRA"]

def _get_url_data_mock_EO(self, flowkey):
    return "http://stats.oecd.org/sdmx-json/data/EO/AUS.CB."

def _select_filter_dimension_mock_EO(self):
    return 0, 3, 'LOCATION', ["FRA"]

class FetcherTestCase(BaseFetcherTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_oecd:FetcherTestCase
    
    FETCHER_KLASS = Fetcher
    DATASETS = ALL_DATASETS
    DATASET_FIRST = "EO"
    DATASET_LAST = "MEI"
    DEBUG_MODE = False
    
    def _load_url_structure(self, dataset_code):
        
        filepaths = self.DATASETS[dataset_code]["DSD"]["filepaths"]
        content_type = 'application/json'

        url = "http://stats.oecd.org/sdmx-json/dataflow/%s" % dataset_code
        self.register_url(url, 
                          filepaths["dataflow"],
                          content_type=content_type,
                          match_querystring=True)
        
    def _load_url_data(self, dataset_code, url):
        
        self.register_url(url, 
                          self.DATASETS[dataset_code]["filepath"],
                          content_type="application/vnd.sdmx.draft-sdmx-json+json;version=2.1",
                          match_querystring=False)
        

    @httpretty.activate  
    def test_load_dataflow(self):

        # nosetests -s -v dlstats.tests.fetchers.test_oecd:FetcherTestCase.test_load_dataflow

        dataset_code = "MEI"
        
        self._load_url_structure(dataset_code)
        
        url = "http://stats.oecd.org/sdmx-json/dataflow/%s" % dataset_code
        codes, filepath = load_dataflow(url, dataset_code)

        self.assertEqual(sorted(list(codes.keys())), 
                         sorted(['concepts', 'codelists', 
                                 'attribute_dataset_keys', 
                                 'attribute_observation_keys', 
                                 'header', 'dimension_keys']))
        
        self.assertEqual(codes["dimension_keys"], 
                         ['LOCATION', 'SUBJECT', 'MEASURE', 
                          'FREQUENCY', 'TIME_PERIOD'])
        
    @httpretty.activate  
    def test_load_data(self):

        # nosetests -s -v dlstats.tests.fetchers.test_oecd:FetcherTestCase.test_load_data

        dataset_code = "MEI"
        
        url = "http://stats.oecd.org/sdmx-json/data/MEI/FRA.BPFAFD01.."
        self._load_url_data(dataset_code, url)

        rows, filepath, status_code, response = load_data(url, dataset_code)
        
        """
        http://stats.oecd.org/restsdmx/sdmx.ashx/GetData/MEI/FRA.XTNTVA01..A
        http://stats.oecd.org/sdmx-json/data/MEI/FRA.XTNTVA01..A
        """
        
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]["key_o"], "0:0:0:0")
        self.assertEqual(rows[-1]["key_o"], "0:0:3:0")


    @httpretty.activate
    @mock.patch('dlstats.fetchers.oecd.OECD_Data._get_url_data', _get_url_data_mock_MEI)
    @mock.patch('dlstats.fetchers.oecd.OECD_Data._select_filter_dimension', _select_filter_dimension_mock_MEI)             
    def test_upsert_dataset_mei(self):

        # nosetests -s -v dlstats.tests.fetchers.test_oecd:FetcherTestCase.test_upsert_dataset_mei
        
        dataset_code = "MEI"
        
        self._load_url_structure(dataset_code)
        url = "http://stats.oecd.org/sdmx-json/data/MEI/FRA.BPFAFD01.."
        self._load_url_data(dataset_code, url)
    
        self.assertProvider()
        self.assertDataTree(dataset_code)
        self.assertDataset(dataset_code)        
        self.assertSeries(dataset_code)
        

    @httpretty.activate
    @mock.patch('dlstats.fetchers.oecd.OECD_Data._get_url_data', _get_url_data_mock_EO)
    @mock.patch('dlstats.fetchers.oecd.OECD_Data._select_filter_dimension', _select_filter_dimension_mock_EO)             
    def test_upsert_dataset_eo(self):

        # nosetests -s -v dlstats.tests.fetchers.test_oecd:FetcherTestCase.test_upsert_dataset_eo
        
        dataset_code = "EO"
        
        self._load_url_structure(dataset_code)
        url = "http://stats.oecd.org/sdmx-json/data/EO/AUS.CB."
        self._load_url_data(dataset_code, url)
    
        self.assertProvider()
        self.assertDataTree(dataset_code)
        self.assertDataset(dataset_code)        
        self.assertSeries(dataset_code)
        
