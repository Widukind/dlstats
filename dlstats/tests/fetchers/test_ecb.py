# -*- coding: utf-8 -*-

from collections import OrderedDict
from datetime import datetime
import os
from pprint import pprint
import json

import pytz
import pandas

from dlstats.fetchers._commons import Providers
from dlstats.fetchers.ecb import ECB as Fetcher
from dlstats.fetchers import schemas
from dlstats import constants

import unittest
from unittest import mock
import httpretty

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase, body_generator

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "ecb"))

CATEGORYSCHEME_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-categoryscheme.xml"))
DATA_TREE_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-data-tree.json"))

STATSCAL_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "statscal.htm"))

# 88 sans /ECB: http://sdw-wsrest.ecb.int/service/categoryscheme/ECB/?references=parentsandsiblings 
DATAFLOW_COUNT = 58

ALL_DATASETS = {
    'EXR': { #http://sdw-wsrest.ecb.int/service/data/EXR/.ARS+AUD.EUR.SP00.A
        'dataflow-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-EXR-dataflow.xml")),
        'data-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-data-specific-X.ARS+AUD.EUR.SP00.A-2.1.xml")),
        #'data-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-data-X.ARS+AUD.NOK.EUR.SP00.A.xml")),
        'datastructure-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-ECB_EXR1-datastructure.xml")),
        'series_count': 8, #exclus les frequency H
        'first_series': {
            "key": "A.ARS.EUR.SP00.A",
            "name": "A-ARS-EUR-SP00-A",
            "frequency": "A",
            "first_value": "0.895263095238095",
            "first_date": "2001",
            "last_value": "10.252814453125001",
            "last_date": "2015",
        },
        'last_series': {
            "key": "Q.AUD.EUR.SP00.A",
            "name": "Q-AUD-EUR-SP00-A",
            "frequency": "Q",
            "first_value": "1.769871428571429",
            "first_date": "1999-Q1",
            "last_value": "1.520512307692308",
            "last_date": "2015-Q4",
        },
    },
}

class FetcherTestCase(BaseFetcherTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase
    
    FETCHER_KLASS = Fetcher
    DATASETS = ALL_DATASETS
    
    def _register_urls_data_tree(self):

        #?references=parentsandsiblings
        url_categoryscheme = "http://sdw-wsrest.ecb.int/service/categoryscheme/ECB"
        self.register_url(url_categoryscheme, 
                          CATEGORYSCHEME_FP,
                          content_type='application/vnd.sdmx.structure+xml;version=2.1',
                          match_querystring=True)
        
    def _register_urls_data(self, dataset_code):

        #?references=all
        url_dataflow_for_dataset = "http://sdw-wsrest.ecb.int/service/dataflow/ECB/EXR"
        httpretty.register_uri(httpretty.GET, 
                               url_dataflow_for_dataset,
                               body=body_generator(ALL_DATASETS[dataset_code]['dataflow-fp']),
                               match_querystring=True,
                               status=200,
                               streaming=True,
                               content_type='application/vnd.sdmx.structure+xml;version=2.1')

        # Appelé par pandaSDMX quand key dans data request        
        url_datastructure = "http://sdw-wsrest.ecb.int/service/datastructure/ECB/ECB_EXR1?references=children"# % dataset_code
        httpretty.register_uri(httpretty.GET, 
                               url_datastructure,
                               body=body_generator(ALL_DATASETS[dataset_code]['datastructure-fp']),
                               match_querystring=True,
                               status=200,
                               streaming=True,
                               content_type='application/vnd.sdmx.structure+xml;version=2.1')

        url_data = "http://sdw-wsrest.ecb.int/service/data/EXR"
        httpretty.register_uri(httpretty.GET, 
                               url_data,
                               body=body_generator(ALL_DATASETS[dataset_code]['data-fp']), 
                               match_querystring=True,
                               status=200,
                               streaming=True,
                               content_type='application/vnd.sdmx.structurespecificdata+xml;version=2.1'
                               )

    @httpretty.activate     
    def test_build_data_tree(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase.test_build_data_tree
        
        self._register_urls_data_tree()
        
        self.fetcher.build_data_tree()
        
        #self.maxDiff = None

        provider = self.fetcher.provider
        self.assertEqual(provider.count_data_tree(), 12)               
        
        """
        pprint(provider.data_tree)
        with open(DATA_TREE_FP, "w") as fp:
            json.dump(provider.data_tree, fp, sort_keys=False)
        """        
        
        new_provider = Providers(fetcher=self.fetcher, **provider.bson)

        with open(DATA_TREE_FP) as fp:
            local_data_tree = json.load(fp, object_pairs_hook=OrderedDict)
            new_provider.data_tree = local_data_tree
            #self.assertEqual(provider.data_tree, new_provider.data_tree)
        
        filter_datasets = provider.datasets(category_filter="ECB.MOBILE_NAVI.06")
        self.assertEqual(len(filter_datasets), 6)
        self.assertEqual(filter_datasets[0]["dataset_code"], "BOP")
        self.assertEqual(filter_datasets[-1]["dataset_code"], "TRD")
        
        for d in provider.data_tree:
            schemas.data_tree_schema(d)
            
        provider.update_database()
        
        doc = self.db[constants.COL_PROVIDERS].find_one({"name": self.fetcher.provider_name})
        self.assertIsNotNone(doc)
        for i, d in enumerate(doc["data_tree"]):
            self.assertEqual(doc["data_tree"][i], provider.data_tree[i])
            
        count = len(self.fetcher.datasets_list())
        self.assertEqual(count, DATAFLOW_COUNT)        

    #@httpretty.activate     
    @unittest.skipIf(True, "FIXME")    
    def test_upsert_dataset_exr(self):

        # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase.test_upsert_dataset_exr
         
        dataset_code = 'EXR'

        self._register_urls_data(dataset_code)
        
        #TODO: analyse result
        result = self.fetcher.upsert_dataset(dataset_code)
        self.assertIsNotNone(result)
        
        self.assertDatasetOK(dataset_code)        
        self.assertSeriesOK(dataset_code)

        
           
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
        
        self._register_urls_data_tree()        
        
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
 
    @unittest.skipIf(True, "TODO")    
    def test_is_valid_frequency(self):
        pass
    
    
