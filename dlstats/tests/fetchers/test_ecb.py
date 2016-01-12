# -*- coding: utf-8 -*-

from collections import OrderedDict
from io import StringIO
from datetime import datetime
import os
from pprint import pprint
import json

import pytz
import pandas

from dlstats.fetchers._commons import Datasets, Providers
from dlstats.fetchers.ecb import ECB as Fetcher, ECB_Data as FetcherData, ContinueRequest
from dlstats.fetchers import schemas
from dlstats import constants

import unittest
from unittest import mock
import httpretty

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR, BaseDBTestCase

"""
Load files with httpie tools:
    http "http://sdw-wsrest.ecb.int/service/dataflow/ECB" > ecb-dataflow.xml
    http "http://sdw-wsrest.ecb.int/service/categoryscheme/ECB/?references=parentsandsiblings" > ecb-categoryscheme.xml
    http "http://sdw-wsrest.ecb.int/service/dataflow/ECB/EXR?references=all" > ecb-EXR-dataflow.xml
    http "http://sdw-wsrest.ecb.int/service/data/EXR/M.NOK.EUR.SP00.A" > ecb-data-M.NOK.EUR.SP00.A.xml
    http "http://sdw-wsrest.ecb.int/service/data/EXR/.ARS+AUD.EUR.SP00.A" > ecb-data-X.ARS+AUD.NOK.EUR.SP00.A.xml
    
    http "http://sdw-wsrest.ecb.int/service/datastructure/ECB/ECB_EXR1?references=all" > ecb-ECB_EXR1-datastructure.xml
    
"""
RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "ecb"))

CATEGORYSCHEME_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-categoryscheme.xml"))
DATA_TREE_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-data-tree.json"))

DATAFLOW_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-dataflow.xml"))
STATSCAL_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "statscal.htm"))

# 88 sans /ECB: http://sdw-wsrest.ecb.int/service/categoryscheme/ECB/?references=parentsandsiblings 
DATAFLOW_COUNT = 58

DATASETS = {
    'EXR': {
        'dataflow-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-EXR-dataflow.xml")),
        'data-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-data-M.NOK.EUR.SP00.A.xml")),
        #'data-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-data-X.ARS+AUD.NOK.EUR.SP00.A.xml")),        
        'datastructure-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-ECB_EXR1-datastructure.xml")),
        'series_count': 1
    },
}

#TODO: use tests.utils
def _body(filepath):
    '''body for large file'''
    with open(filepath, 'rb') as fp:
        for line in fp:
            yield line        
        
class Mock_Data(FetcherData):
    
    """
    def __init__(self, **kwargs):
        self._series = []
        super().__init__(**kwargs)

    def __next__(self):          
        try:      
            _series = next(self.rows)
            if not _series:
                raise StopIteration()
        except ContinueRequest:
            _series = next(self.rows)
            
        bson = self.build_series(_series)
        self._series.append(bson)
        return bson
    """
    
    def get_dim_select(self):
        #return [None]
        return [{"FREQ": "M"}]

"""    
def mock_upsert_dataset(self, dataset_code):

    dataset = Datasets(provider_name=self.provider_name, 
                       dataset_code=dataset_code,
                       #sname=dataflow.name.en,
                       doc_href=None,
                       last_update=datetime(2015, 12, 24),
                       fetcher=self)
    
    self._data = Mock_Data(dataset=dataset)
    dataset.series.data_iterator = self._data
    result = dataset.update_database()

    return result
"""

class FetcherTestCase(BaseDBTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)
        self.fetcher = Fetcher(db=self.db)

    def _register_urls_data_tree(self):
        
        #?references=parentsandsiblings
        url_categoryscheme = "http://sdw-wsrest.ecb.int/service/categoryscheme/ECB"
        httpretty.register_uri(httpretty.GET, 
                               url_categoryscheme,
                               body=_body(CATEGORYSCHEME_FP),
                               match_querystring=True,
                               status=200,
                               streaming=True,
                               content_type='application/vnd.sdmx.structure+xml;version=2.1')
        
    def _register_urls_data(self, dataset_code):

        url_dataflow_for_dataset = "http://sdw-wsrest.ecb.int/service/dataflow/ECB/EXR?references=all"
        httpretty.register_uri(httpretty.GET, 
                               url_dataflow_for_dataset,
                               body=_body(DATASETS[dataset_code]['dataflow-fp']),
                               match_querystring=True,
                               status=200,
                               streaming=True,
                               content_type='application/vnd.sdmx.structure+xml;version=2.1')

        # Appelé par pandaSDMX quand key dans data request        
        url_datastructure = "http://sdw-wsrest.ecb.int/service/datastructure/ECB/ECB_EXR1?references=all"# % dataset_code
        httpretty.register_uri(httpretty.GET, 
                               url_datastructure,
                               body=_body(DATASETS[dataset_code]['datastructure-fp']),
                               match_querystring=True,
                               status=200,
                               streaming=True,
                               content_type='application/vnd.sdmx.structure+xml;version=2.1')

        def request_callback(request, uri, headers):
            #print("request : ", request)
            #print("uri : ", uri)
            #print("headers : ", headers)
            #uri :  http://sdw-wsrest.ecb.int/service/data/EXR/M....
            return (200, {"Content-Type": 'application/vnd.sdmx.genericdata+xml;version=2.1'}, _body(DATASETS[dataset_code]['data-fp']))
    
        #http://sdw-wsrest.ecb.int/service/data/EXR/A.ARS...
        #http://sdw-wsrest.ecb.int/service/data/EXR/M.NOK.EUR.SP00.A
        url_data = "http://sdw-wsrest.ecb.int/service/data/EXR/M...." #% dataset_code
        httpretty.register_uri(httpretty.GET, 
                               url_data,
                               body=_body(DATASETS[dataset_code]['data-fp']), #request_callback, 
                               match_querystring=True,
                               status=200,
                               streaming=True,
                               content_type='application/vnd.sdmx.genericdata+xml;version=2.1')

        url_data = "http://sdw-wsrest.ecb.int/service/data/EXR"
        httpretty.register_uri(httpretty.GET, 
                               url_data,
                               body=_body(DATASETS[dataset_code]['data-fp']), 
                               match_querystring=True,
                               status=200,
                               streaming=True,
                               content_type='application/vnd.sdmx.genericdata+xml;version=2.1')

    @httpretty.activate     
    def test_headers(self):

        # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase.test_headers


        self._register_urls_data_tree()        

        dataset_code = 'EXR'
        self._register_urls_data(dataset_code)

        response = self.fetcher.sdmx.get(resource_type='categoryscheme', url='http://sdw-wsrest.ecb.int/service/categoryscheme/ECB?references=parentsandsiblings')
        self.assertEqual(response.http_headers['server'], 'Python/HTTPretty')
        self.assertEqual(response.url, 'http://sdw-wsrest.ecb.int/service/categoryscheme/ECB?references=parentsandsiblings')
        self.assertEqual(response.http_headers['content-type'], 'application/vnd.sdmx.structure+xml;version=2.1')
        
        response = self.fetcher.sdmx.get(resource_type='data', 
                                 resource_id=dataset_code,
                                 key={"FREQ": "M"})        
        self.assertEqual(response.http_headers['server'], 'Python/HTTPretty')
        self.assertEqual(response.url, 'http://sdw-wsrest.ecb.int/service/data/EXR/M....')
        self.assertEqual(response.http_headers['content-type'], 'application/vnd.sdmx.genericdata+xml;version=2.1')

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
        
        filter_datasets = provider.datasets(_filter="ECB.MOBILE_NAVI.06")
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
         
           
    @httpretty.activate     
    @mock.patch('dlstats.fetchers.ecb.ECB_Data', Mock_Data)
    def test_upsert_dataset(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase.test_upsert_dataset
        
        dataset_code = 'EXR'
        
        self._register_urls_data(dataset_code)
        
        result = self.fetcher.upsert_dataset(dataset_code)
        self.assertIsNotNone(result)
        
        query = {
            'provider_name': self.fetcher.provider_name,
            "dataset_code": dataset_code
        }

        dataset = self.db[constants.COL_DATASETS].find_one(query)
        self.assertIsNotNone(dataset)
        
        attribute_list = {'OBS_STATUS': [['A', 'Normal value']]}
        self.assertEqual(dataset['attribute_list'], attribute_list)        

        dimension_list = {
            'COLLECTION': [['A', 'Average of observations through period']],
            'CURRENCY': [['NOK', 'Norwegian krone']],
            'CURRENCY_DENOM': [['EUR', 'Euro']],
            'DECIMALS': [['4', 'Four']],
            'EXR_SUFFIX': [['A', 'Average or standardised measure for given frequency']],
            'EXR_TYPE': [['SP00', 'Spot']],
            'FREQ': [['M', 'Monthly']],
            'SOURCE_AGENCY': [['4F0', 'European Central Bank (ECB)']],
            'UNIT': [['NOK', 'Norwegian krone']],
            'UNIT_MULT': [['0', 'Units']]
        }
        self.assertEqual(dataset['dimension_list'], dimension_list)        

        series_list = self.db[constants.COL_SERIES].find(query)
        count = series_list.count()
        self.assertEqual(count, 1)#DATASETS[dataset_code]['series_count'])
        
        # https://sdw-wsrest.ecb.int/service/data/EXR/M.NOK.EUR.SP00.A
        query['key'] = "M.NOK.EUR.SP00.A" 
        series_sample = self.db[constants.COL_SERIES].find_one(query)
        self.assertIsNotNone(series_sample)
        
        #1990-01
        self.assertEqual(series_sample["values"][0], "8.651225")

        #2015-10
        self.assertEqual(series_sample["values"][-1], "9.464159090909094")
        
        frequency = series_sample["frequency"]
        self.assertEqual(frequency, "M")
        
        start_date = str(pandas.Period(ordinal=series_sample["start_date"], freq=frequency))
        self.assertEqual(start_date, '1999-01')

        end_date = str(pandas.Period(ordinal=series_sample["end_date"], freq=frequency))
        self.assertEqual(end_date, '2015-12')
        
        self.assertEqual(series_sample['dimensions'], {'SOURCE_AGENCY': '4F0', 'UNIT': 'NOK', 'UNIT_MULT': '0', 'CURRENCY': 'NOK', 'EXR_SUFFIX': 'A', 'EXR_TYPE': 'SP00', 'CURRENCY_DENOM': 'EUR', 'COLLECTION': 'A', 'DECIMALS': '4', 'FREQ': 'M'})

        series_dimensions = {
            'SOURCE_AGENCY': '4F0', 
            'UNIT': 'NOK', 
            'UNIT_MULT': '0', 
            'CURRENCY': 'NOK', 
            'EXR_SUFFIX': 'A', 
            'EXR_TYPE': 'SP00', 
            'CURRENCY_DENOM': 'EUR', 
            'COLLECTION': 'A', 
            'DECIMALS': '4', 
            'FREQ': 'M'
        } 
        self.assertEqual(series_sample['dimensions'], series_dimensions)
        
        self.assertEqual(len(series_sample['values']), len(series_sample['attributes']['OBS_STATUS']))

    @httpretty.activate
    def test_parse_agenda(self):
        with open(STATSCAL_FP) as fp:
            body = fp.read()
        httpretty.register_uri(httpretty.GET,
                               "http://www.ecb.europa.eu/press/calendars/statscal/html/index.en.html",
                               body=body,
                               #match_querystring=True,
                               status=200,
                               content_type='text/html')
        model = {'dataflow_key': 'BP6',
                 'reference_period': 'Q4 2016',
                 'scheduled_date': '10/04/2017 10:00 CET'}

        self.assertEqual(list(self.fetcher.parse_agenda())[-1], model)

    @httpretty.activate
    def test_get_calendar(self):
        with open(STATSCAL_FP) as fp:
            body_st = fp.read()
        httpretty.register_uri(httpretty.GET,
                               "http://www.ecb.europa.eu/press/calendars/statscal/html/index.en.html",
                               body=body_st,
                               #match_querystring=True,
                               status=200,
                               content_type='text/html')
        with open(DATAFLOW_FP) as fp:
            body_df = fp.read()
        url_dataflow = "http://sdw-wsrest.ecb.int/service/dataflow/ECB"
        httpretty.register_uri(httpretty.GET,
                               url_dataflow,
                               body=body_df,
                               #match_querystring=True,
                               status=200,
                               content_type='application/vnd.sdmx.structure+xml;version=2.1')
        model = {'action': 'update_node',
                 'kwargs': {'dataset_code': 'IVF', 'provider_name': 'ECB'},
                 'period_kwargs': {'run_date': datetime(2017, 2, 20, 10, 0),
                                   'timezone': pytz.timezone('CET')},
                 'period_type': 'date'}

        self.assertEqual(model, [a for a in self.fetcher.get_calendar()][-1])
 
    @unittest.skipIf(True, "TODO")    
    def test_is_valid_frequency(self):
        pass
    
    @unittest.skipIf(True, "TODO")    
    def test_get_series(self):
        pass
    
    @unittest.skipIf(True, "TODO")    
    def test_build_series(self):
        pass
    
