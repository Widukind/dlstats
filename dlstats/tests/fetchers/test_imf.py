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
        "concept_keys": ['flag', 'iso', 'scale', 'units', 'weo-country-code', 'weo-subject-code'],
        "codelist_keys": ['flag', 'iso', 'scale', 'units', 'weo-country-code', 'weo-subject-code'],
        "codelist_count": {
            "flag": 1,
            "iso": 182,
            "scale": 4,
            "units": 12,
            "weo-country-code": 182,
            "weo-subject-code": 23,
        },        
        "dimension_keys": ['weo-subject-code', 'iso', 'weo-country-code', 'units'],
        "dimension_count": {
            "weo-subject-code": 23,
            "iso": 182,
            "weo-country-code": 182,
            "units": 12,
        },
        "attribute_keys": ['scale', 'flag'],
        "attribute_count": {
            "scale": 4,
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

def get_dimensions_from_dsd(self, xml_dsd=None, provider_name=None, dataset_code=None, dsd_id=None):
    dimension_keys = ['REF_AREA', 'INDICATOR', 'VIS_AREA', 'FREQ', 'SCALE']
    dimensions = {
        "REF_AREA": {},
        "INDICATOR": {},
        "VIS_AREA": {},
        'FREQ': {'A': 'A'},
        "SCALE": {},
    }
    return dimension_keys, dimensions

LOCAL_DATASETS_UPDATE = {
    "DOT": {
        "concept_keys": ['cmt', 'freq', 'indicator', 'obs-status', 'ref-area', 'scale', 'seriescode', 'time-format', 'vis-area'],
        "codelist_keys": ['cmt', 'freq', 'indicator', 'obs-status', 'ref-area', 'scale', 'seriescode', 'time-format', 'vis-area'],
        "codelist_count": {
            "cmt": 0,
            "freq": 3,
            "indicator": 4,
            "obs-status": 13,
            "ref-area": 248,
            "scale": 16,
            "seriescode": 0,
            "time-format": 6,
            "vis-area": 311,
        },
        "dimension_keys": ['ref-area', 'indicator', 'vis-area', 'freq', 'scale'],
        "dimension_count": {
            "ref-area": 248,
            "indicator": 4,
            "vis-area": 311,
            "freq": 3,
            "scale": 16,
        },
        "attribute_keys": ['seriescode', 'cmt', 'obs-status', 'time-format'],
        "attribute_count": {
            "seriescode": 0,
            "cmt": 0,
            "obs-status": 13,
            "time-format": 6,
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
        
#@unittest.skipIf(True, "TODO")
class FetcherTestCase(BaseFetcherTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test_imf:FetcherTestCase
    
    FETCHER_KLASS = Fetcher    
    DATASETS = {
        'WEO': DATA_WEO,
        'DOT': deepcopy(xml_samples.DATA_IMF_DOT),
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
        
        url = "http://dataservices.imf.org/REST/SDMX_XML.svc/CompactData/%s/...A." % dataset_code
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
    @mock.patch('dlstats.fetchers.imf.IMF_XML_Data._get_dimensions_from_dsd', get_dimensions_from_dsd)
    def test_upsert_dataset_dot(self):

        # nosetests -s -v dlstats.tests.fetchers.test_imf:FetcherTestCase.test_upsert_dataset_dot
        
        dataset_code = "DOT"
        self._load_files_xml(dataset_code)
        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])
        self.assertProvider()
        self.assertDataset(dataset_code)        
        self.assertSeries(dataset_code)
