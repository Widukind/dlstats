# -*- coding: utf-8 -*-

import re
from datetime import datetime
import os

from dlstats.fetchers.imf import IMF as Fetcher
from dlstats import constants

import httpretty
import unittest

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.base import BaseTestCase
from dlstats.tests.fetchers.base import BaseFetcherTestCase
from dlstats.tests.resources import xml_samples

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
        "categories_parents": None,
        "categories_root": ['DOT', 'WEO'],
        "concept_keys": ['ISO', 'Scale', 'Units', 'WEO Country Code', 'WEO Subject Code', 'flag'],
        "codelist_keys": ['ISO', 'Scale', 'Units', 'WEO Country Code', 'WEO Subject Code', 'flag'],
        "codelist_count": {
            "ISO": 182,
            "Scale": 4,
            "Units": 12,
            "WEO Country Code": 182,
            "WEO Subject Code": 23,
            "flag": 1,
        },        
        "dimension_keys": ['WEO Subject Code', 'ISO', 'WEO Country Code', 'Units'],
        "dimension_count": {
            "WEO Subject Code": 23,
            "ISO": 182,
            "WEO Country Code": 182,
            "Units": 12,
        },
        "attribute_keys": ['Scale', 'flag'],
        "attribute_count": {
            "Scale": 4,
            "flag": 1,
        },
    },
    "series_accept": 6006,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 210210,
    "series_key_first": "NGDP_R.AFG.0",
    "series_key_last": "BCA.ZWE.8", #FIXME: BCA_NGDPD.ZWE.8",
    "series_sample": {
        'provider_name': 'IMF',
        'dataset_code': 'WEO',
        'key': 'NGDP_R.AFG.0',
        'name': 'Gross domestic product, constant prices - Afghanistan, Rep. of. - National currency',
        'frequency': 'A',
        'last_update': datetime(2009, 4, 1, 0, 0),
        'first_value': {
            'value': 'n/a',
            'ordinal': 10,
            'period': '1980',
            'period_o': '1980',
            'attributes': None,
        },
        'last_value': {
            'value': '536.521',
            'ordinal': 44,
            'period': '2014',
            'period_o': '2014',
            'attributes': {
                "flag": 'e'
            },
        },
        'dimensions': {
            'ISO': 'AFG',
            'WEO Country Code': '512',
            'WEO Subject Code': 'NGDP_R',
            'Units': '0',
        },
        'attributes': {
            'Scale': '0',
        }
    }
}

LOCAL_DATASETS_UPDATE = {
    "DOT": {
        "concept_keys": ['CMT', 'FREQ', 'INDICATOR', 'OBS_STATUS', 'REF_AREA', 'SCALE', 'SERIESCODE', 'TIME_FORMAT', 'VIS_AREA'],
        "codelist_keys": ['CMT', 'FREQ', 'INDICATOR', 'OBS_STATUS', 'REF_AREA', 'SCALE', 'SERIESCODE', 'TIME_FORMAT', 'VIS_AREA'],
        "codelist_count": {
            "CMT": 0,
            "FREQ": 1,
            "INDICATOR": 2,
            "OBS_STATUS": 0,
            "REF_AREA": 1,
            "SCALE": 1,
            "SERIESCODE": 0,
            "TIME_FORMAT": 1,
            "VIS_AREA": 1,
        },
        "dimension_keys": ['REF_AREA', 'INDICATOR', 'VIS_AREA', 'FREQ', 'SCALE'],
        "dimension_count": {
            "REF_AREA": 1,
            "INDICATOR": 2,
            "VIS_AREA": 1,
            "FREQ": 1,
            "SCALE": 1,
        },
        "attribute_keys": ['SERIESCODE', 'CMT', 'OBS_STATUS', 'TIME_FORMAT'],
        "attribute_count": {
            "SERIESCODE": 0,
            "CMT": 0,
            "OBS_STATUS": 0,
            "TIME_FORMAT": 1,
        },
    },
}

def weo_urls_patch(self):
    return [
        "http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009all.xls",
    ]

def weo_urls_patch_revision(self):
    return [
        "http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009all.xls",
        #"http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009alla.xls",
        "http://www.imf.org/external/pubs/ft/weo/2009/02/weodata/WEOOct2009all.xls",
        #"http://www.imf.org/external/pubs/ft/weo/2009/02/weodata/WEOOct2009alla.xls",
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
        'DOT': xml_samples.DATA_IMF_DOT,        
    }    
    DATASET_FIRST = "AFRREO"
    DATASET_LAST = "WoRLD"
    DEBUG_MODE = False

    def _load_files_weo(self, dataset_code):
        url = "http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009all.xls"
        self.register_url(url, 
                          self.DATASETS[dataset_code]["filepath"])
        
    def _load_files_xml(self, dataset_code):
        
        filepaths = self.DATASETS[dataset_code]["DSD"]["filepaths"]

        url = "http://dataservices.imf.org/REST/SDMX_XML.svc/DataStructure/%s" % dataset_code
        self.register_url(url, filepaths["datastructure"])
        
        url = "http://dataservices.imf.org/REST/SDMX_XML.svc/CompactData/%s" % dataset_code
        self.register_url(url, self.DATASETS[dataset_code]['filepath'])
        

    @httpretty.activate     
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    @mock.patch('dlstats.fetchers.imf.WeoData.weo_urls', weo_urls_patch)
    def test_load_datasets_first(self):

        dataset_code = "WEO"
        self._load_files_weo(dataset_code)
        self.assertLoadDatasetsFirst([dataset_code])

    #@httpretty.activate     
    #@unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    #@mock.patch('dlstats.fetchers.imf.WeoData.weo_urls', weo_urls_patch_revision)
    @unittest.skipIf(True, "TODO")
    def test_load_datasets_update(self):

        # nosetests -s -v dlstats.tests.fetchers.test_imf:FetcherTestCase.test_load_datasets_update
        
        dataset_code = "WEO"

        self.register_url("http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009all.xls", 
                          os.path.abspath(os.path.join(RESOURCES_DIR, "WEOApr2009all.xls")))

        #self.register_url("http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009alla.xls", 
        #                  os.path.abspath(os.path.join(RESOURCES_DIR, "WEOApr2009alla.xls")))

        self.register_url("http://www.imf.org/external/pubs/ft/weo/2009/02/weodata/WEOOct2009all.xls", 
                          os.path.abspath(os.path.join(RESOURCES_DIR, "WEOOct2009all.xls")))

        #self.register_url("http://www.imf.org/external/pubs/ft/weo/2009/02/weodata/WEOOct2009alla.xls", 
        #                  os.path.abspath(os.path.join(RESOURCES_DIR, "WEOOct2009alla.xls")))
    
        self.assertLoadDatasetsUpdate([dataset_code])
        
        query = {"provider_name": "IMF", 
                 "dataset_code": dataset_code, 
                 "values.revisions": {"$ne": None}}
                
        series_count_revisions = self.db[constants.COL_SERIES].count(query)
        
        self.assertEqual(series_count_revisions, 665)

    @httpretty.activate     
    @unittest.skipIf(True, "TODO")
    def test_build_data_tree(self):

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
    def test_upsert_dataset_dot(self):

        # nosetests -s -v dlstats.tests.fetchers.test_imf:FetcherTestCase.test_upsert_dataset_dot
        
        dataset_code = "DOT"
        self._load_files_xml(dataset_code)
        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])
        self.assertProvider()
        self.assertDataset(dataset_code)        
        self.assertSeries(dataset_code)
