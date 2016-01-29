# -*- coding: utf-8 -*-

from copy import deepcopy
from datetime import datetime
import os

import pytz

from dlstats.fetchers.ecb import ECB as Fetcher

import unittest
import httpretty

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase, body_generator
from dlstats.tests.resources import xml_samples

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "ecb"))

STATSCAL_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "statscal.htm"))

ALL_DATAFLOW_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-all-dataflow.xml"))

LOCAL_DATASETS_UPDATE = {
    "EXR": {
        "codelist_keys": ['OBS_COM', 'DOM_SER_IDS', 'EXR_TYPE', 'COVERAGE', 'UNIT_INDEX_BASE', 'COLLECTION', 'FREQ', 'BREAKS', 'EXR_SUFFIX', 'DECIMALS', 'TITLE', 'OBS_STATUS', 'COMPILATION', 'PUBL_MU', 'SOURCE_PUB', 'TITLE_COMPL', 'NAT_TITLE', 'PUBL_PUBLIC', 'CURRENCY', 'OBS_PRE_BREAK', 'CURRENCY_DENOM', 'OBS_CONF', 'PUBL_ECB', 'UNIT', 'TIME_FORMAT', 'SOURCE_AGENCY', 'UNIT_MULT'],
        "codelist_count": {
            "OBS_COM": 0,
            "DOM_SER_IDS": 0,
            "EXR_TYPE": 36,
            "COVERAGE": 0,
            "UNIT_INDEX_BASE": 0,
            "COLLECTION": 10,
            "FREQ": 10,
            "BREAKS": 0,
            "EXR_SUFFIX": 6,
            "DECIMALS": 16,
            "TITLE": 0,
            "OBS_STATUS": 17,
            "COMPILATION": 0,
            "PUBL_MU": 0,
            "SOURCE_PUB": 0,
            "TITLE_COMPL": 0,
            "NAT_TITLE": 0,
            "PUBL_PUBLIC": 0,
            "CURRENCY": 349,
            "OBS_PRE_BREAK": 0,
            "CURRENCY_DENOM": 349,
            "OBS_CONF": 4,
            "PUBL_ECB": 0,
            "UNIT": 330,
            "TIME_FORMAT": 0,
            "SOURCE_AGENCY": 893,
            "UNIT_MULT": 11,                       
        },
    }
}

class FetcherTestCase(BaseFetcherTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase
    
    FETCHER_KLASS = Fetcher
    DATASETS = {
        'EXR': deepcopy(xml_samples.DATA_ECB_SPECIFIC)
    }
    DATASET_FIRST = "AME"
    DATASET_LAST = "YC"
    DEBUG_MODE = False
    
    def _load_files(self, dataset_code):

        filepaths = self.DATASETS[dataset_code]["DSD"]["filepaths"]
        dsd_content_type = 'application/vnd.sdmx.structure+xml;version=2.1'

        url = "http://sdw-wsrest.ecb.int/service/dataflow/ECB"
        self.register_url(url, 
                          ALL_DATAFLOW_FP, #filepaths["dataflow"]
                          content_type=dsd_content_type,
                          match_querystring=True)

        url = "http://sdw-wsrest.ecb.int/service/categoryscheme/ECB"
        self.register_url(url, 
                          filepaths["categoryscheme"],
                          content_type=dsd_content_type,
                          match_querystring=True)
        
        url = "http://sdw-wsrest.ecb.int/service/categorisation/ECB"
        self.register_url(url, 
                          filepaths["categorisation"],
                          content_type=dsd_content_type,
                          match_querystring=True)

        url = "http://sdw-wsrest.ecb.int/service/conceptscheme/ECB"
        self.register_url(url, 
                          filepaths["conceptscheme"],
                          content_type=dsd_content_type,
                          match_querystring=True)
        
        url = "http://sdw-wsrest.ecb.int/service/datastructure/ECB/ECB_EXR1?references=children"
        self.register_url(url, 
                          filepaths["datastructure"],
                          content_type=dsd_content_type,
                          match_querystring=True)
        
        url = "http://sdw-wsrest.ecb.int/service/data/EXR"
        self.register_url(url, 
                          self.DATASETS[dataset_code]['filepath'],
                          content_type='application/vnd.sdmx.structurespecificdata+xml;version=2.1')
        
    @httpretty.activate     
    def test_upsert_dataset_exr(self):

        # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase.test_upsert_dataset_exr

        self._collections_is_empty()
         
        dataset_code = 'EXR'

        self.DATASETS[dataset_code]["DSD"]["codelist_keys"] = LOCAL_DATASETS_UPDATE[dataset_code]["codelist_keys"]
        self.DATASETS[dataset_code]["DSD"]["codelist_count"] = LOCAL_DATASETS_UPDATE[dataset_code]["codelist_count"]

        self._load_files(dataset_code)
        
        self.assertProvider()
        self.assertDataTree(dataset_code)
        self.assertDataset(dataset_code)
        self.assertSeries(dataset_code)

    @httpretty.activate
    def test_parse_agenda(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase.test_parse_agenda
        
        httpretty.register_uri(httpretty.GET,
                               "http://www.ecb.europa.eu/press/calendars/statscal/html/index.en.html",
                               body=body_generator(STATSCAL_FP),
                               status=200,
                               streaming=True,
                               content_type='text/html')

        model = {'dataflow_key': 'BP6',
                 'reference_period': 'Q4 2016',
                 'scheduled_date': '10/04/2017 10:00 CET'}

        #TODO: test first and last
        self.assertEqual(list(self.fetcher.parse_agenda())[-1], model)

    @httpretty.activate
    def test_get_calendar(self):

        # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase.test_get_calendar
        
        dataset_code = 'EXR'
        self._load_files(dataset_code)        
        
        httpretty.register_uri(httpretty.GET,
                               "http://www.ecb.europa.eu/press/calendars/statscal/html/index.en.html",
                               body=body_generator(STATSCAL_FP),
                               status=200,
                               streaming=True,
                               content_type='text/html')

        calendar_first = {
            'action': 'update_node',
            'kwargs': {'dataset_code': 'SEC', 'provider_name': 'ECB'},
            'period_type': 'date',
            'period_kwargs': {
                'run_date': datetime(2016, 1, 13, 10, 0),
                'timezone': pytz.timezone('CET')
            },
        }        

        calendar_last = {
            'action': 'update_node',
            'period_type': 'date',
            'kwargs': {'dataset_code': 'IVF', 'provider_name': 'ECB'},
            'period_kwargs': {
                'run_date': datetime(2017, 2, 20, 10, 0),
                'timezone': pytz.timezone('CET')
            },
        }

        calendars = [a for a in self.fetcher.get_calendar()]
        self.assertEqual(len(calendars), 138)
        self.assertEqual(calendar_first, calendars[0])
        self.assertEqual(calendar_last, calendars[-1])
 
    
