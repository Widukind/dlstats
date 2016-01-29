# -*- coding: utf-8 -*-

import io
import datetime
import os

from dlstats import constants
from dlstats.fetchers import bis
from dlstats.fetchers.bis import BIS as Fetcher
from dlstats.fetchers.bis import DATASETS as FETCHER_DATASETS

import httpretty

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR, BaseTestCase, BaseDBTestCase
from dlstats.tests.fetchers.base import BaseFetcherTestCase, body_generator

import unittest

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "bis"))

DATA_BIS_DSRP = {
    "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "full_bis_dsr_csv.zip")),
    "DSD": {
        "provider": "BIS",
        "filepath": None,
        "dataset_code": "DSRP",
        "dsd_id": "DSRP",
        "is_completed": True,
        "categories_key": "DSRP",
        "categories_parents": None,
        "categories_root": ['CBS', 'CNFS', 'DSRP', 'DSS', 'EERI', 'LBS-DISS', 'PP-LS', 'PP-SS'],
        "concept_keys": ['Frequency', "Borrowers' country", 'Borrowers'],
        "codelist_keys": ['Frequency', "Borrowers' country", 'Borrowers'],
        "codelist_count": {
            "Frequency": 1,
            "Borrowers' country": 32,
            "Borrowers": 3,
        },        
        "dimension_keys": ['Frequency', "Borrowers' country", 'Borrowers'],
        "dimension_count": {
            "Frequency": 1,
            "Borrowers' country": 32,    
            "Borrowers": 3
        },
        "attribute_keys": [],
        "attribute_count": None,
    },
    "series_accept": 66,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 4290,
    "series_key_first": "Q:AU:H",
    "series_key_last": "Q:ZA:P",
    "series_sample": {
        'provider_name': 'BIS',
        'dataset_code': 'DSRP',
        'key': 'Q:AU:H',
        'name': 'Quarterly - Australia - Households & NPISHs',
        'frequency': 'Q',
        'last_update': None,
        'first_value': {
            'value': '10',
            'ordinal': 116,
            'period': '1999-Q1',
            'period_o': '1999-Q1',
            'attributes': None,
        },
        'last_value': {
            'value': '15.3',
            'ordinal': 180,
            'period': '2015-Q1',
            'period_o': '2015-Q1',
            'attributes': None,
        },
        'dimensions': {
            'Frequency': 'Q',
            "Borrowers' country": 'AU',
            'Borrowers': 'H',
        },
        'attributes': None,
    }
}

#---AGENDA
AGENDA_FP = os.path.abspath(os.path.join(RESOURCES_DIR, 'agenda.html'))

class BISUtilsTestCase(BaseTestCase):
    """BIS Utils
    """
    
    @unittest.skipIf(True, "TODO")    
    def test_load_read_csv(self):

        # nosetests -s -v dlstats.tests.fetchers.test_bis:BISUtilsTestCase.test_load_read_csv
        
        d = FETCHER_DATASETS.copy()
        print()
        for dataset_code, dataset in d.items():
            if dataset_code != "DSRP":
                continue
            filepath = FETCHER_DATASETS[dataset_code]["filepath"]
            datas = dataset['datas']            
            fileobj = io.StringIO(datas)#, newline=os.linesep)
            rows, headers, release_date, dimension_keys, periods = bis.local_read_csv(fileobj=fileobj)
            self.assertTrue('KEY' in headers)
            line1 = bis.csv_dict(headers, next(rows))
            #TODO: test values ?
            print(dataset_code, dimension_keys, line1)
            self.assertEqual(len(dimension_keys), FETCHER_DATASETS[dataset_code]["dimensions_count"])
            #pprint(line1)

class FetcherTestCase(BaseFetcherTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test_bis:FetcherTestCase
    
    FETCHER_KLASS = Fetcher
    
    DATASETS = {
        'DSRP': DATA_BIS_DSRP
    }
    
    DATASET_FIRST = "CBS"
    DATASET_LAST = "PP-SS"
    DEBUG_MODE = False

    def _common(self, dataset_code):
        url = FETCHER_DATASETS[dataset_code]["url"]
        self.register_url(url, 
                          self.DATASETS[dataset_code]["filepath"])
    
    @httpretty.activate     
    def test_upsert_dataset_dsrp(self):

        # nosetests -s -v dlstats.tests.fetchers.test_bis:FetcherTestCase.test_upsert_dataset_dsrp
        
        dataset_code = "DSRP"
        
        self._common(dataset_code)
    
        self.assertProvider()
        self.assertDataTree(dataset_code)    
        self.assertDataset(dataset_code)        
        self.assertSeries(dataset_code)

    @httpretty.activate     
    def test_dsrp_updated(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:FetcherTestCase.test_dsrp_updated
        
        #Updated for revisions or other field: start/end date
        
        dataset_code = "DSRP"
        
        self._common(dataset_code)
        self.assertDataset(dataset_code)        
        self.assertSeries(dataset_code)
        
        query = {"provider_name": self.fetcher.provider_name,
                 "dataset_code": dataset_code}

        old_bson = self.db[constants.COL_SERIES].find_one(query)
        _id = old_bson["_id"]
        #print("KEY : ", old_bson["key"])
        
        '''Change first value for one series'''
        old_value = old_bson["values"][0]["value"]  
        old_bson["values"][0]["value"] = "100"
        query_update = {"$set": {"values.0": old_bson["values"][0]}}
        result = self.db[constants.COL_SERIES].find_one_and_update({"_id": _id}, 
                                                                    query_update)
        self.assertIsNotNone(result)

        '''Change last_update for dataset'''        
        doc = self.db[constants.COL_DATASETS].find_one(query)
        old_date = doc["last_update"]
        new_date = datetime.datetime(old_date.year -1, old_date.month, old_date.day)
        ds_query_update = {"$set": {"last_update": new_date}}
        result = self.db[constants.COL_DATASETS].find_one_and_update({"_id": doc["_id"]},
                                                                     ds_query_update)
        
        '''Reload upsert_dataset process'''
        result = self.fetcher.upsert_dataset(dataset_code)
        self.assertIsNotNone(result)

        '''Load series modified'''        
        series = self.db[constants.COL_SERIES].find_one({"_id": _id})
        self.assertIsNotNone(series)
        
        self.assertTrue("revisions" in series["values"][0])
        self.assertEqual(series["values"][0]["value"], old_value)
        self.assertEqual(series["values"][0]["revisions"][0]["value"], "100")
        

    @httpretty.activate     
    @unittest.skipIf(True, "TODO")
    def test_upsert_dataset_eeri(self):

        # nosetests -s -v dlstats.tests.fetchers.test_bis:FetcherTestCase.test_upsert_dataset_eeri
        #full_bis_eer_csv.zip
        dataset_code = "EERI"


class CalendarTestCase(BaseDBTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_bis:CalendarTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)
        
        self.fetcher = bis.BIS(db=self.db)
        self.dataset_code = None
        self.dataset = None        
        self.filepath = None
        
    def _common_test_agenda(self):

        httpretty.register_uri(httpretty.GET, 
                               "http://www.bis.org/statistics/relcal.htm?m=6|37|68",
                               body=body_generator(AGENDA_FP),
                               match_querystring=True,
                               status=200,
                               content_type='application/octet-stream;charset=UTF-8',
                               streaming=True)

    @httpretty.activate
    def test_parse_agenda(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:LightBISDatasetsDBTestCase.test_parse_agenda
        
        self._common_test_agenda()
        
        attempt = [
            [None, None, 
             datetime.datetime(2015, 12, 1, 0, 0), 
             datetime.datetime(2016, 1, 1, 0, 0), 
             datetime.datetime(2016, 2, 1, 0, 0), 
             datetime.datetime(2016, 3, 1, 0, 0), 
             datetime.datetime(2016, 4, 1, 0, 0), 
             datetime.datetime(2016, 5, 1, 0, 0)],
            [
                 'Banking statistics',  # dataset CBS
                 'Locational', 
                 '6',   #December 2015 : 6 (Q2 2015+)
                 '22',  #January 2016  : 22* (Q3 2015)
                 None,  #February 2016 : None
                 '6',   #March  2016   : 6 (Q3 2015+)
                 '22',  #April 2016    : 22* (Q4 2015)
                 None   #May 2016      : None
            ],
             ['Banking statistics', 'Consolidated', '6', '22', None, '6', '22', None],
             ['Debt securities statistics', 'International', '6', None, None, '6', None, None],
             ['Debt securities statistics', 'Domestic and total', '6', None, None, '6', None, None],
             ['Derivatives statistics', 'OTC', '6', None, None, '6', None, '13'],
             ['Derivatives statistics', 'Exchange-traded', '6', None, None, '6', None, None],
             ['Global liquidity indicators', None, '6', None, None, '6', None, None],
             ['Credit to non-financial sector', None, '6', None, None, '6', None, None],
             ['Debt service ratio', None, '6', None, None, '6', None, None],
             ['Property prices', 'Detailed data', '18', '22', '19', '18', '22', '20'],
             ['Property prices', 'Selected', None, None, '19', None, None, '20'],
             ['Property prices', 'long', None, None, '19', None, None, '20'],
             ['Effective exchange rates', None, '16', '18', '16', '16', '18', '17'],
             ['BIS Statistical Bulletin', None, '6', None, None, '6', None, None]
        ]
        
        agenda = self.fetcher.parse_agenda()
        self.assertEqual(agenda, attempt)
                    
    @httpretty.activate
    def test_get_calendar(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:LightBISDatasetsDBTestCase.test_get_calendar

        self._common_test_agenda()
        
        self.maxDiff = None

        calendar = list(self.fetcher.get_calendar())

        self.assertEqual(len(calendar), 26)

        attempt = [
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'LBS-DISS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2015, 12, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'LBS-DISS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 1, 22, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'LBS-DISS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 3, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'LBS-DISS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 4, 22, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'CBS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2015, 12, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'CBS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 1, 22, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'CBS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 3, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'CBS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 4, 22, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'DSS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2015, 12, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'DSS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 3, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'DSS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2015, 12, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'DSS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 3, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'CNFS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2015, 12, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'CNFS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 3, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'DSRP', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2015, 12, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'DSRP', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 3, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'PP-SS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 2, 19, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'PP-SS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 5, 20, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'PP-LS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 2, 19, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'PP-LS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 5, 20, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'EERI', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2015, 12, 16, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'EERI', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 1, 18, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'EERI', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 2, 16, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'EERI', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 3, 16, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'EERI', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 4, 18, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'EERI', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 5, 17, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'}]

        self.assertEqual(calendar, attempt)

