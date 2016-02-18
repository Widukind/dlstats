# -*- coding: utf-8 -*-

from copy import deepcopy
from datetime import datetime
import os

import pytz

from dlstats.fetchers.ecb import ECB as Fetcher

import httpretty
import unittest

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase, body_generator
from dlstats.tests.resources import xml_samples

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "ecb"))

STATSCAL_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "statscal.htm"))

ALL_DATAFLOW_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-all-dataflow.xml"))

LOCAL_DATASETS_UPDATE = {
    "EXR": {
        "concept_keys": ['BREAKS', 'COLLECTION', 'COMPILATION', 'COVERAGE', 'CURRENCY', 'CURRENCY_DENOM', 'DECIMALS', 'DOM_SER_IDS', 'EXR_SUFFIX', 'EXR_TYPE', 'FREQ', 'NAT_TITLE', 'OBS_COM', 'OBS_CONF', 'OBS_PRE_BREAK', 'OBS_STATUS', 'PUBL_ECB', 'PUBL_MU', 'PUBL_PUBLIC', 'SOURCE_AGENCY', 'SOURCE_PUB', 'TIME_FORMAT', 'TITLE', 'TITLE_COMPL', 'UNIT', 'UNIT_INDEX_BASE', 'UNIT_MULT'],
        "codelist_keys": ['BREAKS', 'COLLECTION', 'COMPILATION', 'COVERAGE', 'CURRENCY', 'CURRENCY_DENOM', 'DECIMALS', 'DOM_SER_IDS', 'EXR_SUFFIX', 'EXR_TYPE', 'FREQ', 'NAT_TITLE', 'OBS_COM', 'OBS_CONF', 'OBS_PRE_BREAK', 'OBS_STATUS', 'PUBL_ECB', 'PUBL_MU', 'PUBL_PUBLIC', 'SOURCE_AGENCY', 'SOURCE_PUB', 'TIME_FORMAT', 'TITLE', 'TITLE_COMPL', 'UNIT', 'UNIT_INDEX_BASE', 'UNIT_MULT'],
        "codelist_count": {
            "BREAKS": 0,
            "COLLECTION": 1,
            "COMPILATION": 0,
            "COVERAGE": 0,
            "CURRENCY": 2,
            "CURRENCY_DENOM": 1,
            "DECIMALS": 2,
            "DOM_SER_IDS": 0,
            "EXR_SUFFIX": 1,
            "EXR_TYPE": 1,
            "FREQ": 4,
            "NAT_TITLE": 0,
            "OBS_COM": 1,
            "OBS_CONF": 0,
            "OBS_PRE_BREAK": 0,
            "OBS_STATUS": 1,
            "PUBL_ECB": 0,
            "PUBL_MU": 0,
            "PUBL_PUBLIC": 0,
            "SOURCE_AGENCY": 1,
            "SOURCE_PUB": 0,
            "TIME_FORMAT": 0,
            "TITLE": 0,
            "TITLE_COMPL": 0,
            "UNIT": 2,
            "UNIT_INDEX_BASE": 0,
            "UNIT_MULT": 1,
        },
        "dimension_keys": ['FREQ', 'CURRENCY', 'CURRENCY_DENOM', 'EXR_TYPE', 'EXR_SUFFIX'],
        "dimension_count": {
            "FREQ": 4,
            "CURRENCY": 2,
            "CURRENCY_DENOM": 1,
            "EXR_TYPE": 1,
            "EXR_SUFFIX": 1,
        },
        "attribute_keys": ['TIME_FORMAT', 'OBS_STATUS', 'OBS_CONF', 'OBS_PRE_BREAK', 'OBS_COM', 'BREAKS', 'COLLECTION', 'DOM_SER_IDS', 'PUBL_ECB', 'PUBL_MU', 'PUBL_PUBLIC', 'UNIT_INDEX_BASE', 'COMPILATION', 'COVERAGE', 'DECIMALS', 'NAT_TITLE', 'SOURCE_AGENCY', 'SOURCE_PUB', 'TITLE', 'TITLE_COMPL', 'UNIT', 'UNIT_MULT'],
        "attribute_count": {
            "TIME_FORMAT": 0,
            "OBS_STATUS": 1,
            "OBS_CONF": 0,
            "OBS_PRE_BREAK": 0,
            "OBS_COM": 1,
            "BREAKS": 0,
            "COLLECTION": 1,
            "DOM_SER_IDS": 0,
            "PUBL_ECB": 0,
            "PUBL_MU": 0,
            "PUBL_PUBLIC": 0,
            "UNIT_INDEX_BASE": 0,
            "COMPILATION": 0,
            "COVERAGE": 0,
            "DECIMALS": 2,
            "NAT_TITLE": 0,
            "SOURCE_AGENCY": 1,
            "SOURCE_PUB": 0,
            "TITLE": 0,
            "TITLE_COMPL": 0,
            "UNIT": 2,
            "UNIT_MULT": 1,
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
        
        url = "http://sdw-wsrest.ecb.int/service/datastructure/ECB/ECB_EXR1?references=all"
        self.register_url(url, 
                          filepaths["datastructure"],
                          content_type=dsd_content_type,
                          match_querystring=True)
        
        url = "http://sdw-wsrest.ecb.int/service/data/EXR"
        self.register_url(url, 
                          self.DATASETS[dataset_code]['filepath'],
                          content_type='application/vnd.sdmx.structurespecificdata+xml;version=2.1')

    @httpretty.activate     
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    def test_load_datasets_first(self):

        dataset_code = 'EXR'
        self._load_files(dataset_code)
        self.assertLoadDatasetsFirst([dataset_code])

    @httpretty.activate     
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    def test_load_datasets_update(self):

        dataset_code = 'EXR'
        self._load_files(dataset_code)
        self.assertLoadDatasetsUpdate([dataset_code])

    @httpretty.activate     
    def test_build_data_tree(self):

        dataset_code = 'EXR'
        self._load_files(dataset_code)
        self.assertDataTree(dataset_code)
        
    @httpretty.activate     
    def test_upsert_dataset_exr(self):

        # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase.test_upsert_dataset_exr

        dataset_code = 'EXR'
        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])
        self._load_files(dataset_code)
        
        self.assertProvider()
        self.assertDataset(dataset_code)
        self.assertSeries(dataset_code)
        
    @httpretty.activate
    def test__parse_agenda(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase.test__parse_agenda
        
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
        self.assertEqual(list(self.fetcher._parse_agenda())[-1], model)

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
 
    
