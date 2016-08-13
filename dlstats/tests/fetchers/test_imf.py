# -*- coding: utf-8 -*-

import re
from datetime import datetime
import os
from copy import deepcopy

from dlstats.fetchers.imf import IMF as Fetcher
from dlstats import constants

import httpretty
import unittest

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.base import BaseTestCase
from dlstats.tests.fetchers.base import BaseFetcherTestCase

from unittest import mock

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "imf"))

DATA_WEO = {
    "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "WEOApr2009all.xls")),
    "DSD": {
        "provider": "IMF",
        "filepath": None,
        "dataset_code": "WEO",
        "dsd_id": "WEO",
        "is_completed": True,
        "categories_key": "WEO",
        "categories_parents": [],
        "categories_root": ['BOFS', 'CDIS', 'COFER', 'COFR', 'CPI', 'CPIS', 'DOT', 'FAS', 'FSI', 'GFS', 'HPDD', 'ICSD', 'IFS', 'PCP', 'PGI', 'REO', 'RT', 'WCED', 'WEO', 'WEO-GROUPS', 'WoRLD'],
        "concept_keys": ['flag', 'iso', 'scale', 'units', 'weo-country-code', 'weo-subject-code'],
        "codelist_keys": ['flag', 'iso', 'scale', 'units', 'weo-country-code', 'weo-subject-code'],
        "codelist_count": {
            "flag": 1,
            "iso": 182,
            "scale": 3,
            "units": 12,
            "weo-country-code": 182,
            "weo-subject-code": 33,
        },        
        "dimension_keys": ['weo-subject-code', 'iso', 'units'],
        "dimension_count": {
            "weo-subject-code": 33,
            "iso": 182,
            "units": 12,
        },
        "attribute_keys": ['weo-country-code', 'scale', 'flag'],
        "attribute_count": {
            "weo-country-code": 182,
            "scale": 3,
            "flag": 1,
        },
    },
    "series_accept": 6006,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 210210,
    "series_key_first": "NGDP_R.AFG.0",
    "series_key_last": "BCA_NGDPD.ZWE.8",
    "series_sample": {
        'provider_name': 'IMF',
        'dataset_code': 'WEO',
        'key': 'NGDP_R.AFG.0',
        'name': 'Gross domestic product, constant prices - Afghanistan, Rep. of. - National currency',
        'frequency': 'A',
        'last_update': datetime(2009, 4, 1, 0, 0),
        'first_value': {
            'value': 'n/a',
            'period': '1980',
            'attributes': None,
        },
        'last_value': {
            'value': '536.521',
            'period': '2014',
            'attributes': {
                "flag": 'e'
            },
        },
        'dimensions': {
            'ISO': 'AFG',
            'WEO Subject Code': 'NGDP_R',
            'Units': '0',
        },
        'attributes': {
            'Scale': '0',
            'WEO Country Code': '512',
        }
    }
}

DATA_WEO_GROUPS = {
    "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "WEOApr2009alla.xls")),
    "DSD": {
        "provider": "IMF",
        "filepath": None,
        "dataset_code": "WEO-GROUPS",
        "dsd_id": "WEO-GROUPS",
        "is_completed": True,
        "categories_key": "WEO-GROUPS",
        "categories_parents": [],
        "categories_root": ['BOFS', 'CDIS', 'COFER', 'COFR', 'CPI', 'CPIS', 'DOT', 'FAS', 'FSI', 'GFS', 'HPDD', 'ICSD', 'IFS', 'PCP', 'PGI', 'REO', 'RT', 'WCED', 'WEO', 'WEO-GROUPS', 'WoRLD'],
        "concept_keys": ['flag', 'scale', 'units', 'weo-country-group-code', 'weo-subject-code'],
        "codelist_keys": ['flag', 'scale', 'units', 'weo-country-group-code', 'weo-subject-code'],
        "codelist_count": {
            "flag": 1,
            "scale": 2,
            "units": 14,
            "weo-country-group-code": 16,
            "weo-subject-code": 121,            
        },        
        "dimension_keys": ['weo-subject-code', 'weo-country-group-code', 'units'],
        "dimension_count": {
            "weo-subject-code": 121,
            "weo-country-group-code": 16,
            "units": 14,
        },
        "attribute_keys": ['scale', 'flag'],
        "attribute_count": {
            "scale": 2,
            "flag": 1,
        },
    },
    "series_accept": 1936,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 67760,
    "series_key_first": "NGDP_RPCH.1.0",
    "series_key_last": "PURANW.205.12",
    "series_sample": {
        'provider_name': 'IMF',
        'dataset_code': 'WEO-GROUPS',
        'key': 'NGDP_RPCH.1.0',
        'name': 'Gross domestic product, constant prices - World - Annual percent change',
        'frequency': 'A',
        'last_update': datetime(2009, 4, 1, 0, 0),
        'first_value': {
            'value': '2.016',
            'period': '1980',
            'attributes': None,
        },
        'last_value': {
            'value': '4.762',
            'period': '2014',
            'attributes': None,
        },
        'dimensions': {
            'WEO Country Group Code': '1',
            'WEO Subject Code': 'NGDP_RPCH',
            'Units': '0',
        },
        'attributes': None
    }
}


def get_dimensions_from_dsd(self, xml_dsd=None, provider_name=None, dataset_code=None, dsd_id=None):
    dimension_keys = ['FREQ', 'REF_AREA', 'INDICATOR', 'COUNTERPART_AREA']
    dimensions = {
        'FREQ': {'A': 'A'},
        "REF_AREA": {},#'US': 'US'},
        "INDICATOR": {},#'TMG_CIF_USD': 'TMG_CIF_USD'},
        "COUNTERPART_AREA": {}#'AN': 'AN', 'FR': 'FR', 'EE': 'EE'},
    }
    return dimension_keys, dimensions

DSD_IMF_DOT = {
    "provider": "IMF",
    "filepaths": {
        "datastructure": os.path.abspath(os.path.join(RESOURCES_DIR, "imf-dot-datastructure-2.1.json")),
    },
    "dataset_code": "DOT",
    "dataset_name": "Direction of Trade Statistics (DOTS)",
    "dsd_id": "DOT",
    "dsd_ids": ["DOT"],
    "dataflow_keys": ['DOT'],
    "is_completed": True,
    "categories_key": "DOT",
    "categories_parents": None,
    "categories_root": None,    
    "concept_keys": ['cmt', 'counterpart-area', 'freq', 'indicator', 'obs-status', 'obs-value', 'ref-area', 'time-format', 'time-period', 'unit-mult'],
    "codelist_keys": ['cmt', 'counterpart-area', 'freq', 'indicator', 'obs-status', 'ref-area', 'time-format', 'unit-mult'],
    "codelist_count": {
        "counterpart-area": 312,
        "freq": 6,
        "indicator": 4,
        "obs-status": 13,
        "ref-area": 249,
        "time-format": 6,
        "unit-mult": 31,
    },
    "dimension_keys": ['freq', 'ref-area', 'indicator', 'counterpart-area'],
    "dimension_count": {
        "freq": 6,
        "ref-area": 249,
        "indicator": 4,
        "counterpart-area": 312,
    },
    "attribute_keys": ['unit-mult', 'cmt', 'obs-status', 'time-format'],
    "attribute_count": {
        "unit-mult": 31,
        "cmt": 0,
        "obs-status": 13,
        "time-format": 6,
    },
}

DATA_IMF_DOT = {
    "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "imf-dot-data-compact-2.1.json")),
    "DSD": DSD_IMF_DOT,
    "kwargs": {
        "provider_name": "IMF",
        "dataset_code": "DOT",        
        "dsd_filepath": DSD_IMF_DOT["filepaths"]["datastructure"],
        "dsd_id": DSD_IMF_DOT["dsd_id"]        
    },
    "series_accept": 3,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 156,
    "series_key_first": 'DOT.A.US.TMG_CIF_USD.AN',
    "series_key_last": 'DOT.A.US.TMG_CIF_USD.EE',
    "series_sample": {
        "provider_name": "IMF",
        "dataset_code": "DOT",
        'key': 'DOT.A.US.TMG_CIF_USD.AN',
        'name': 'Annual - United States - Goods, Value of Imports, Cost, Insurance, Freight (CIF), US Dollars - Netherlands Antilles',
        'frequency': 'A',
        'last_update': None,
        'first_value': {
            'value': '170',
            'period': '1950',
            'attributes': {},
        },
        'last_value': {
            'value': '386.938923',
            'period': '2015',
            'attributes': {},
        },
        'dimensions': {
            'counterpart-area': 'an',
            'freq': 'a',
            'indicator': 'tmg-cif-usd',
            'ref-area': 'us'
        },
        'attributes': {
            'time-format': 'p1y', 
            'unit-mult': '6' 
        },
    }
}

def weo_urls_patch(self):
    return [
        "http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009all.xls",
    ]

def weo_groups_urls_patch(self):
    return [
        "http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009alla.xls",
    ]

def weo_urls_patch_revision(self):
    return [
        "http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009all.xls",
        #"http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009alla.xls",
        "http://www.imf.org/external/pubs/ft/weo/2009/02/weodata/WEOOct2009all.xls",
        #"http://www.imf.org/external/pubs/ft/weo/2009/02/weodata/WEOOct2009alla.xls",
        "http://www.imf.org/external/pubs/ft/weo/2010/01/weodata/WEOApr2010all.xls",
        #"http://www.imf.org/external/pubs/ft/weo/2010/01/weodata/WEOApr2010alla.xls",
    ]
    
class UtilsTestCase(BaseTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test_imf:UtilsTestCase
    
    def test_weo_extract_date(self):

        url = "http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOSep2006all.xls"
        date_str = re.match(".*WEO(\w{7})", url).groups()[0]
        release_date = datetime.strptime(date_str, "%b%Y")
        _date = (release_date.year, release_date.month, release_date.day)
        self.assertEqual(_date, (2006, 9, 1))
        
        url = "http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOSep2006alla.xls"
        date_str = re.match(".*WEO(\w{7})", url).groups()[0]
        release_date = datetime.strptime(date_str, "%b%Y")
        _date = (release_date.year, release_date.month, release_date.day)
        self.assertEqual(_date, (2006, 9, 1))
        
class FetcherTestCase(BaseFetcherTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test_imf:FetcherTestCase
    
    FETCHER_KLASS = Fetcher    
    DATASETS = {
        'WEO': DATA_WEO,
        'WEO-GROUPS': DATA_WEO_GROUPS,
        'DOT': deepcopy(DATA_IMF_DOT),
    }    
    DATASET_FIRST = "AFRREO"
    DATASET_LAST = "WoRLD"
    DEBUG_MODE = False

    def _load_files_weo(self, dataset_code):
        url = "http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009all.xls"
        self.register_url(url, 
                          self.DATASETS[dataset_code]["filepath"])

    def _load_files_weo_groups(self, dataset_code):
        url = "http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009alla.xls"
        self.register_url(url, 
                          self.DATASETS[dataset_code]["filepath"])
        
    def _load_files_json_dot(self, dataset_code):
        
        filepaths = self.DATASETS[dataset_code]["DSD"]["filepaths"]

        url = "http://dataservices.imf.org/REST/SDMX_JSON.svc/DataStructure/DOT"
        self.register_url(url, filepaths["datastructure"])
        
        url = "http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/DOT/A..."
        self.register_url(url, self.DATASETS[dataset_code]['filepath'])
        
    @httpretty.activate     
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    @mock.patch('dlstats.fetchers.imf.WeoData.weo_urls', weo_urls_patch)
    def test_load_datasets_first(self):

        dataset_code = "WEO"
        self._load_files_weo(dataset_code)
        self.assertLoadDatasetsFirst([dataset_code])

    @httpretty.activate     
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    @mock.patch('dlstats.fetchers.imf.WeoData.weo_urls', weo_urls_patch_revision)
    def test_load_datasets_update(self):
        
        dataset_code = "WEO"
        self._load_files_weo(dataset_code)
        self.assertLoadDatasetsUpdate([dataset_code])

    @httpretty.activate     
    def test_build_data_tree(self):

        # nosetests -s -v dlstats.tests.fetchers.test_imf:FetcherTestCase.test_build_data_tree

        dataset_code = "WEO"
        self.assertDataTree(dataset_code)
    
    @httpretty.activate     
    @mock.patch('dlstats.fetchers.imf.WeoData.weo_urls', weo_urls_patch)
    def test_upsert_dataset_weo(self):

        # nosetests -s -v dlstats.tests.fetchers.test_imf:FetcherTestCase.test_upsert_dataset_weo
        
        dataset_code = "WEO"
        self._load_files_weo(dataset_code)
    
        self.assertProvider()
        self.assertDataset(dataset_code)        
        self.assertSeries(dataset_code)

    @httpretty.activate     
    @mock.patch('dlstats.fetchers.imf.WeoGroupsData.weo_urls', weo_groups_urls_patch)
    def test_upsert_dataset_weo_groups(self):

        # nosetests -s -v dlstats.tests.fetchers.test_imf:FetcherTestCase.test_upsert_dataset_weo_groups
        
        dataset_code = "WEO-GROUPS"
        self._load_files_weo_groups(dataset_code)
    
        self.assertProvider()
        self.assertDataset(dataset_code)        
        self.assertSeries(dataset_code)

    @httpretty.activate     
    @mock.patch('dlstats.fetchers.imf.WeoData.weo_urls', weo_urls_patch_revision)
    def test_revisions_weo(self):

        # nosetests -s -v dlstats.tests.fetchers.test_imf:FetcherTestCase.test_revisions_weo
        
        dataset_code = "WEO"

        self.register_url("http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009all.xls", 
                          os.path.abspath(os.path.join(RESOURCES_DIR, "WEOApr2009all.xls")))

        self.register_url("http://www.imf.org/external/pubs/ft/weo/2009/02/weodata/WEOOct2009all.xls", 
                          os.path.abspath(os.path.join(RESOURCES_DIR, "WEOOct2009all.xls")))

        self.register_url("http://www.imf.org/external/pubs/ft/weo/2010/01/weodata/WEOApr2010all.xls", 
                          os.path.abspath(os.path.join(RESOURCES_DIR, "WEOApr2010all.xls")))
    
        #self.assertLoadDatasetsUpdate([dataset_code])
        self.assertProvider()
        result = self.fetcher.wrap_upsert_dataset(dataset_code)
        self.assertIsNotNone(result)
        
        query = {
            'provider_name': self.fetcher.provider_name,
            "dataset_code": dataset_code
        }

        dataset = self.db[constants.COL_DATASETS].find_one(query)
        self.assertIsNotNone(dataset)
        
        if self.is_debug:
            self._debug_dataset(dataset)
            
        count_revisions = self.db[constants.COL_SERIES_ARCHIVES].count(query)
        
        self.assertEqual(count_revisions, 9055)

    @httpretty.activate     
    @mock.patch('dlstats.fetchers.imf.IMF_JSON_Data._get_dimensions_from_dsd', get_dimensions_from_dsd)
    def test_upsert_dataset_dot(self):

        # nosetests -s -v dlstats.tests.fetchers.test_imf:FetcherTestCase.test_upsert_dataset_dot
        
        dataset_code = "DOT"
        self._load_files_json_dot(dataset_code)
        self.assertProvider()
        self.assertDataset(dataset_code)        
        self.assertSeries(dataset_code)
