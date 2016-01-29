# -*- coding: utf-8 -*-

from datetime import datetime
import os

from dlstats.fetchers.imf import IMF as Fetcher
from dlstats.fetchers.imf import DATASETS as FETCHER_DATASETS

import httpretty

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase

import unittest
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
        "categories_root": ['WEO'],
        "concept_keys": ['WEO Subject Code', 'ISO', 'WEO Country Code', 'Units', 'Scale', 'flag'],
        "codelist_keys": ['WEO Subject Code', 'ISO', 'WEO Country Code', 'Units', 'Scale', 'flag'],
        "codelist_count": {
            "WEO Country Code": 182,
            "Country": 182,
            "Subject": 33,
            "Scale": 4,
            "Units": 12,        
            "flag": 1,
        },        
        "dimension_keys": ['WEO Subject Code', 'ISO', 'WEO Country Code', 'Units'],
        "dimension_count": {
            "WEO Subject Code": 33,
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

def weo_urls_patch():
    return [
        "http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009all.xls"
    ]
    
class FetcherTestCase(BaseFetcherTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test_imf:FetcherTestCase
    
    FETCHER_KLASS = Fetcher    
    DATASETS = {
        'WEO': DATA_WEO
    }    
    DATASET_FIRST = "WEO"
    DATASET_LAST = "WEO"
    DEBUG_MODE = False

    def _common(self, dataset_code):
        """
        TODO:
        http://www.imf.org/external/pubs/ft/weo/2015/02/weodata/download.aspx
        url = "http://www.imf.org/external/ns/cs.aspx?id=28"
        """
        
        url = "http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009all.xls"
        self.register_url(url, 
                          self.DATASETS[dataset_code]["filepath"])
    
    @httpretty.activate     
    @mock.patch('dlstats.fetchers.imf.weo_urls', weo_urls_patch)
    def test_upsert_dataset_weo(self):

        # nosetests -s -v dlstats.tests.fetchers.test_imf:FetcherTestCase.test_upsert_dataset_weo
        
        dataset_code = "WEO"
        
        self._common(dataset_code)
    
        self.assertProvider()
        self.assertDataTree(dataset_code)    
        self.assertDataset(dataset_code)        
        self.assertSeries(dataset_code)

    @httpretty.activate     
    @mock.patch('dlstats.fetchers.imf.weo_urls', weo_urls_patch)
    @unittest.skipIf(True, "TODO")    
    def test_updated_dataset(self):

        # nosetests -s -v dlstats.tests.fetchers.test_imf:FetcherTestCase.test_updated_dataset
        
        """
        Use WEOApr2009all.xls -> WEOApr2010all.xls
        Verify revisions
        """
        
        dataset_code = "WEO"
        self._common(dataset_code)
    
        self.assertProvider()
        self.assertDataset(dataset_code)        

        result = self.fetcher.upsert_dataset(dataset_code)
        self.assertIsNone(result)

