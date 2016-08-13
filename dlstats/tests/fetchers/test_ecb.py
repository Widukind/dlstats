# -*- coding: utf-8 -*-

from copy import deepcopy
from datetime import datetime
import os

from dlstats.fetchers.ecb import ECB as Fetcher

import httpretty
import unittest
from unittest import mock

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase, body_generator
from dlstats.tests.resources import xml_samples

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "ecb"))

STATSCAL_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "statscal.htm"))

ALL_DATAFLOW_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-all-dataflow.xml"))

def get_dimensions_from_dsd(self, xml_dsd=None, provider_name=None, dataset_code=None, dsd_id=None):
    dimension_keys = ['FREQ', 'CURRENCY', 'CURRENCY_DENOM', 'EXR_TYPE', 'EXR_SUFFIX']
    dimensions = {
        'FREQ': {'A': 'A'},
        'CURRENCY': {},
        'CURRENCY_DENOM': {},
        'EXR_SUFFIX': {},
        'EXR_TYPE': {},
    }
    return dimension_keys, dimensions

def get_dimensions_from_dsd_SAFE(self, xml_dsd=None, provider_name=None, dataset_code=None, dsd_id=None):
    dimension_keys = ['FREQ', 'REF_AREA', 'FIRM_SIZE', 'FIRM_SECTOR', 
                      'FIRM_TURNOVER', 'FIRM_AGE', 'FIRM_OWNERSHIP', 
                      'SAFE_QUESTION', 'SAFE_ITEM', 'SAFE_ANSWER', 
                      'SAFE_FILTER', 'SAFE_DENOM']
    dimensions = {
        'FREQ': {'H': 'H'},
        'REF_AREA': {'ES': 'ES'},
        'FIRM_SIZE': {'MED': 'MED'},
        'FIRM_SECTOR': {},
        'FIRM_TURNOVER': {},
        'FIRM_AGE': {},
        'FIRM_OWNERSHIP': {},
        'SAFE_QUESTION': {},
        'SAFE_ITEM': {},
        'SAFE_ANSWER': {'NT': 'NT'},
        'SAFE_FILTER': {},
        'SAFE_DENOM': {},
    }
    return dimension_keys, dimensions

LOCAL_DATASETS_UPDATE = {
    "EXR": {
        "concept_keys": ['breaks', 'collection', 'compilation', 'coverage', 'currency', 'currency-denom', 'decimals', 'dom-ser-ids', 'exr-suffix', 'exr-type', 'freq', 'nat-title', 'obs-com', 'obs-conf', 'obs-pre-break', 'obs-status', 'publ-ecb', 'publ-mu', 'publ-public', 'source-agency', 'source-pub', 'time-format', 'title', 'title-compl', 'unit', 'unit-index-base', 'unit-mult'],
        "codelist_keys": ['breaks', 'collection', 'compilation', 'coverage', 'currency', 'currency-denom', 'decimals', 'dom-ser-ids', 'exr-suffix', 'exr-type', 'freq', 'nat-title', 'obs-com', 'obs-conf', 'obs-pre-break', 'obs-status', 'publ-ecb', 'publ-mu', 'publ-public', 'source-agency', 'source-pub', 'time-format', 'title', 'title-compl', 'unit', 'unit-index-base', 'unit-mult'],
        "codelist_count": {
            "breaks": 0,
            "collection": 10,
            "compilation": 0,
            "coverage": 0,
            "currency": 349,
            "currency-denom": 349,
            "decimals": 16,
            "dom-ser-ids": 0,
            "exr-suffix": 6,
            "exr-type": 36,
            "freq": 10,
            "nat-title": 0,
            "obs-com": 0,
            "obs-conf": 4,
            "obs-pre-break": 0,
            "obs-status": 17,
            "publ-ecb": 0,
            "publ-mu": 0,
            "publ-public": 0,
            "source-agency": 893,
            "source-pub": 0,
            "time-format": 0,
            "title": 0,
            "title-compl": 0,
            "unit": 330,
            "unit-index-base": 0,
            "unit-mult": 10,
        },
        "dimension_keys": ['freq', 'currency', 'currency-denom', 'exr-type', 'exr-suffix'],
        "dimension_count": {
            "freq": 10,
            "currency": 349,
            "currency-denom": 349,
            "exr-type": 36,
            "exr-suffix": 6,
        },
        "attribute_keys": ['time-format', 'obs-status', 'obs-conf', 'obs-pre-break', 'obs-com', 'breaks', 'collection', 'dom-ser-ids', 'publ-ecb', 'publ-mu', 'publ-public', 'unit-index-base', 'compilation', 'coverage', 'decimals', 'nat-title', 'source-agency', 'source-pub', 'title', 'title-compl', 'unit', 'unit-mult'],
        "attribute_count": {
            "time-format": 0,
            "obs-status": 17,
            "obs-conf": 4,
            "obs-pre-break": 0,
            "obs-com": 0,
            "breaks": 0,
            "collection": 10,
            "dom-ser-ids": 0,
            "publ-ecb": 0,
            "publ-mu": 0,
            "publ-public": 0,
            "unit-index-base": 0,
            "compilation": 0,
            "coverage": 0,
            "decimals": 16,
            "nat-title": 0,
            "source-agency": 893,
            "source-pub": 0,
            "title": 0,
            "title-compl": 0,
            "unit": 330,
            "unit-mult": 10,
        }, 
    }
}

DSD_ECB_SAFE = {
    "provider": "ECB",
    "filepaths": deepcopy(xml_samples.DATA_ECB_SPECIFIC["DSD"]["filepaths"]),
    "dataset_code": "SAFE",
    "dataset_name": None,
    "dsd_id": "SAFE",
    "dsd_ids": ["SAFE"],
    "dataflow_keys": ['SAFE'],
    "is_completed": True,
    "concept_keys": ['breaks', 'collection', 'compilation', 'decimals', 'firm-age', 'firm-ownership', 'firm-sector', 'firm-size', 'firm-turnover', 'freq', 'obs-com', 'obs-conf', 'obs-pre-break', 'obs-status', 'publ-ecb', 'publ-mu', 'publ-public', 'ref-area', 'safe-answer', 'safe-denom', 'safe-filter', 'safe-item', 'safe-question', 'time-format', 'title', 'title-compl', 'unit', 'unit-mult'],
    "codelist_keys": ['breaks', 'collection', 'compilation', 'decimals', 'firm-age', 'firm-ownership', 'firm-sector', 'firm-size', 'firm-turnover', 'freq', 'obs-com', 'obs-conf', 'obs-pre-break', 'obs-status', 'publ-ecb', 'publ-mu', 'publ-public', 'ref-area', 'safe-answer', 'safe-denom', 'safe-filter', 'safe-item', 'safe-question', 'time-format', 'title', 'title-compl', 'unit', 'unit-mult'],
    "codelist_count": {
        "breaks": 0,
        "collection": 10,
        "compilation": 0,
        "decimals": 16,
        "firm-age": 6,
        "firm-ownership": 14,
        "firm-sector": 5,
        "firm-size": 6,
        "firm-turnover": 9,
        "freq": 10,
        "obs-com": 0,
        "obs-conf": 9,
        "obs-pre-break": 0,
        "obs-status": 17,
        "publ-ecb": 0,
        "publ-mu": 0,
        "publ-public": 0,
        "ref-area": 528,
        "safe-answer": 145,
        "safe-denom": 8,
        "safe-filter": 3,
        "safe-item": 82,
        "safe-question": 41,
        "time-format": 0,
        "title": 0,
        "title-compl": 0,
        "unit": 330,
        "unit-mult": 10,
    },
    "dimension_keys": ['freq', 'ref-area', 'firm-size', 'firm-sector', 'firm-turnover', 'firm-age', 'firm-ownership', 'safe-question', 'safe-item', 'safe-answer', 'safe-filter', 'safe-denom'],
    "dimension_count": {
        "freq": 10,
        "ref-area": 528,
        "firm-size": 6,
        "firm-sector": 5,
        "firm-turnover": 9,
        "firm-age": 6,
        "firm-ownership": 14,
        "safe-question": 41,
        "safe-item": 82,
        "safe-answer": 145,
        "safe-filter": 3,
        "safe-denom": 8,
    },
    "attribute_keys": ['time-format', 'obs-status', 'obs-conf', 'obs-pre-break', 'obs-com', 'breaks', 'collection', 'publ-ecb', 'publ-mu', 'publ-public', 'compilation', 'decimals', 'title', 'title-compl', 'unit', 'unit-mult'],      
    "attribute_count": {
        "time-format": 0,
        "obs-status": 17,
        "obs-conf": 9,
        "obs-pre-break": 0,
        "obs-com": 0,
        "breaks": 0,
        "collection": 10,
        "publ-ecb": 0,
        "publ-mu": 0,
        "publ-public": 0,
        "compilation": 0,
        "decimals": 16,
        "title": 0,
        "title-compl": 0,
        "unit": 330,
        "unit-mult": 10,
    },
}                        

DSD_ECB_SAFE["filepaths"]["datastructure"] = os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-SAFE-datastructure-2.1.xml"))
        
DATA_ECB_SAFE = {
    "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-data-SAFE-partial.xml")),
    "klass": "XMLSpecificData_2_1_ECB",
    "DSD": DSD_ECB_SAFE,
    "kwargs": {
        "provider_name": "ECB",
        "dataset_code": "SAFE",
        "dsd_filepath": DSD_ECB_SAFE["filepaths"]["datastructure"],
    },
    "series_accept": 368,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 3436,
    "series_key_first": 'H.ES.MED.A.0.0.0.D0.ZZZZ.NT.AL.UN',
    "series_key_last": 'H.ES.MED.A.0.0.0.Q9.FTCR.NT.FL.WN',
    "series_sample": {
        "provider_name": "ECB",
        "dataset_code": "SAFE",
        'key': 'H.ES.MED.A.0.0.0.D0.ZZZZ.NT.AL.UN',
        'name': 'Residency of the firm Spain - Medium-sized firms - All sectors - All turnover breakdowns included - All ages included - All types of ownership, all export classes included - Question D0. Characteristics of the firm - country of residence - Total - Including not applicable responses - Unweighted number of responses',
        'frequency': 'S',
        'last_update': None,
        'first_value': {
            'value': '240',
            'period': '2009-S1',
            'attributes': {
                "OBS_STATUS": "A",
                "OBS_CONF": "F",
            },
        },
        'last_value': {
            'value': '307',
            'period': '2015-S2',
            'attributes': {
                "OBS_STATUS": "A",
                "OBS_CONF": "F",
            },
        },
        'dimensions': {
            'firm-age': '0',
            'firm-ownership': '0',
            'firm-sector': 'a',
            'firm-size': 'med',
            'firm-turnover': '0',
            'freq': 'h',
            'ref-area': 'es',
            'safe-answer': 'nt',
            'safe-denom': 'un',
            'safe-filter': 'al',
            'safe-item': 'zzzz',
            'safe-question': 'd0'                       
        },
        'attributes': {
            'collection': 'v',
            'decimals': '0',
            'unit': 'pure-numb',
            'unit-mult': '0'                       
        },
    }
}

class FetcherTestCase(BaseFetcherTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase
    
    FETCHER_KLASS = Fetcher
    DATASETS = {
        'EXR': deepcopy(xml_samples.DATA_ECB_SPECIFIC),
        'SAFE': deepcopy(DATA_ECB_SAFE),
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
        
        if dataset_code == 'EXR':
            url = "http://sdw-wsrest.ecb.int/service/datastructure/ECB/ECB_EXR1?references=all"
            self.register_url(url, 
                              filepaths["datastructure"],
                              content_type=dsd_content_type,
                              match_querystring=True)
            
            url = "http://sdw-wsrest.ecb.int/service/data/EXR/A...."
            self.register_url(url, 
                              self.DATASETS[dataset_code]['filepath'],
                              content_type='application/vnd.sdmx.structurespecificdata+xml;version=2.1')

        elif dataset_code == 'SAFE':
            url = "http://sdw-wsrest.ecb.int/service/datastructure/ECB/ECB_SAFE?references=all"
            self.register_url(url, 
                              filepaths["datastructure"],
                              content_type=dsd_content_type,
                              match_querystring=True)
    
            url = "http://sdw-wsrest.ecb.int/service/data/SAFE/H..........."
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
        
        # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase.test_build_data_tree

        dataset_code = 'EXR'
        self._load_files(dataset_code)
        self.assertDataTree(dataset_code)
        
    @httpretty.activate
    @mock.patch('dlstats.fetchers.ecb.ECB_Data._get_dimensions_from_dsd', get_dimensions_from_dsd)
    def test_upsert_dataset_exr(self):

        # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase.test_upsert_dataset_exr
        
        dataset_code = 'EXR'
        self.DATASETS[dataset_code]["series_sample"]["attributes"].pop("TITLE", None)
        self.DATASETS[dataset_code]["series_sample"]["attributes"].pop("TITLE_COMPL", None)
        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])
        self._load_files(dataset_code)
        
        self.assertProvider()
        dataset = self.assertDataset(dataset_code)
        series_list = self.assertSeries(dataset_code)
        
        for series in series_list:
            self.assertEquals(series["last_update_ds"], dataset["last_update"])

    @httpretty.activate
    @mock.patch('dlstats.fetchers.ecb.ECB_Data._get_dimensions_from_dsd', get_dimensions_from_dsd_SAFE)
    def test_upsert_dataset_safe(self):

        # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase.test_upsert_dataset_safe
        
        dataset_code = 'SAFE'
        self.DATASETS[dataset_code]["series_sample"]["attributes"].pop("TITLE", None)
        self.DATASETS[dataset_code]["series_sample"]["attributes"].pop("TITLE_COMPL", None)
        self._load_files(dataset_code)

        self.assertProvider()
        dataset = self.assertDataset(dataset_code)
        series_list = self.assertSeries(dataset_code)
        
        for series in series_list:
            self.assertEquals(series["last_update_ds"], dataset["last_update"])
        
    @httpretty.activate
    @unittest.skipIf(True, "FIXME")
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
    @unittest.skipIf(True, "FIXME")
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
            'action': 'update-dataset',
            'kwargs': {'dataset_code': 'SEC', 'provider_name': 'ECB'},
            'period_type': 'date',
            'period_kwargs': {
                'run_date': datetime(2016, 1, 13, 10, 0),
                'timezone': 'CET' 
            },
        }        

        calendar_last = {
            'action': 'update-dataset',
            'period_type': 'date',
            'kwargs': {'dataset_code': 'IVF', 'provider_name': 'ECB'},
            'period_kwargs': {
                'run_date': datetime(2017, 2, 20, 10, 0),
                'timezone': 'CET' 
            },
        }

        calendars = [a for a in self.fetcher.get_calendar()]
        self.assertEqual(len(calendars), 138)
        self.assertEqual(calendar_first, calendars[0])
        self.assertEqual(calendar_last, calendars[-1])
 
    
