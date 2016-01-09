# -*- coding: utf-8 -*-

from datetime import datetime
import os
from pprint import pprint

import pandas

from dlstats.fetchers._commons import Datasets
from dlstats.fetchers.ecb import ECB as Fetcher, ECB_Data as FetcherData, ContinueRequest
from dlstats import constants

import unittest
from unittest import mock
import httpretty

from dlstats.tests.base import RESOURCES_DIR, BaseDBTestCase

"""
Load files with httpie tools:
    http "http://sdw-wsrest.ecb.int/service/dataflow/ECB" > ecb-dataflow.xml
    http "http://sdw-wsrest.ecb.europa.eu/service/dataflow/ECB/EXR?references=all" > ecb-EXR-dataflow.xml
    http "http://sdw-wsrest.ecb.europa.eu/service/datastructure/ECB/ECB_EXR1?references=all" > ecb-ECB_EXR1-datastructure.xml
    http "http://sdw-wsrest.ecb.int/service/data/EXR/M.NOK.EUR.SP00.A" > ecb-data-M.NOK.EUR.SP00.A.xml
    http "http://sdw-wsrest.ecb.int/service/data/EXR/.ARS+AUD.EUR.SP00.A" > ecb-data-X.ARS+AUD.NOK.EUR.SP00.A.xml
"""
DATAFLOW_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-dataflow.xml"))

DATAFLOW_COUNT = 56

DATASETS = {
    'EXR': {
        'dataflow-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-EXR-dataflow.xml")),
        'data-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-data-M.NOK.EUR.SP00.A.xml")),
        #'data-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-data-X.ARS+AUD.NOK.EUR.SP00.A.xml")),        
        'datastructure-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "ecb-ECB_EXR1-datastructure.xml")),
        'series_count': 1
    },
}

class Mock_Data(FetcherData):
    
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
    
    def get_dim_select(self):
        #return [None]
        return [{"FREQ": "M"}]
    
def mock_upsert_dataset(self, dataset_code):

    self.load_structure(force=False)
    
    if not dataset_code in self._dataflows:
        raise Exception("This dataset is unknown" + dataset_code)
    
    dataflow = self._dataflows[dataset_code]
    
    dataset = Datasets(provider_name=self.provider_name, 
                       dataset_code=dataset_code,
                       name=dataflow.name.en,
                       doc_href=None,
                       last_update=datetime(2015, 12, 24),
                       fetcher=self)
    
    self._data = Mock_Data(dataset=dataset)
    dataset.series.data_iterator = self._data
    result = dataset.update_database()

    return result


class FetcherTestCase(BaseDBTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)
        self.fetcher = Fetcher(db=self.db)
        
    def _load_dataset(self, dataset_code):

        #TODO: use tests.utils
        def _body(filepath):
            '''body for large file'''
            with open(filepath, 'rb') as fp:
                for line in fp:
                    yield line        
        
        #http://sdw-wsrest.ecb.int/service/dataflow/ECB
        url_dataflow = "http://sdw-wsrest.ecb.int/service/dataflow/ECB"
        httpretty.register_uri(httpretty.GET, 
                               url_dataflow,
                               body=_body(DATAFLOW_FP),
                               #match_querystring=True,
                               status=200,
                               streaming=True,
                               content_type='application/vnd.sdmx.structure+xml;version=2.1')

        url_dataflow_for_dataset = "http://sdw-wsrest.ecb.int/service/dataflow/ECB/EXR?references=all"
        httpretty.register_uri(httpretty.GET, 
                               url_dataflow_for_dataset,
                               body=_body(DATASETS[dataset_code]['dataflow-fp']),
                               match_querystring=True,
                               status=200,
                               streaming=True,
                               content_type='application/vnd.sdmx.structure+xml;version=2.1')
        
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

        dataset_code = 'EXR'
        
        self._load_dataset(dataset_code)

        response = self.fetcher.sdmx.get(resource_type='dataflow')
        self.assertEqual(response.http_headers['server'], 'Python/HTTPretty')
        self.assertEqual(response.url, 'http://sdw-wsrest.ecb.int/service/dataflow/ECB')
        self.assertEqual(response.http_headers['content-type'], 'application/vnd.sdmx.structure+xml;version=2.1')
        
        response = self.fetcher.sdmx.get(resource_type='data', 
                                 resource_id=dataset_code,
                                 key={"FREQ": "M"})        
        self.assertEqual(response.http_headers['server'], 'Python/HTTPretty')
        self.assertEqual(response.url, 'http://sdw-wsrest.ecb.int/service/data/EXR/M....')
        self.assertEqual(response.http_headers['content-type'], 'application/vnd.sdmx.genericdata+xml;version=2.1')
   
    @httpretty.activate     
    @mock.patch('dlstats.fetchers.ecb.ECB.upsert_dataset', mock_upsert_dataset)    
    def test_load_dataset(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_ecb:FetcherTestCase.test_load_dataset
        
        dataset_code = 'EXR'
        
        self._load_dataset(dataset_code)
        
        self.fetcher.load_structure()
        
        self.assertEqual(len(self.fetcher._dataflows.keys()), DATAFLOW_COUNT)
        
        self.assertTrue(dataset_code in self.fetcher._dataflows)

        result = self.fetcher.upsert_dataset(dataset_code)
        
        query = {
            'provider_name': self.fetcher.provider_name,
            "dataset_code": dataset_code
        }

        dataset = self.db[constants.COL_DATASETS].find_one(query)
        self.assertIsNotNone(dataset)
        
        series_list = self.db[constants.COL_SERIES].find(query)
        
        #print(self.fetcher._data._series)
        
        count = series_list.count()
        
        #print("count : ", len(self.fetcher._data._series))
        
        self.assertEqual(count, 1)#DATASETS[dataset_code]['series_count'])
        
        # https://sdw-wsrest.ecb.europa.eu/service/data/EXR/M.NOK.EUR.SP00.A
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
        
        
    @unittest.skipIf(True, "TODO")
    def test_dimensions_to_dict(self):
        pass
    
    @unittest.skipIf(True, "TODO")
    def test_select_short_dimension(self):
        pass
    
    @unittest.skipIf(True, "TODO")    
    def test_is_valid_frequency(self):
        pass
    
    @unittest.skipIf(True, "TODO")    
    def test_get_series(self):
        pass
    
    @unittest.skipIf(True, "TODO")    
    def test_build_series(self):
        pass
    
    @httpretty.activate     
    @mock.patch('dlstats.fetchers.ecb.ECB.upsert_dataset', mock_upsert_dataset)    
    @unittest.skipIf(True, "TODO")
    def test_is_updated(self):
        pass

