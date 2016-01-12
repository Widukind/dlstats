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
STATSCAL_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "statscal.htm"))

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

    @httpretty.activate
    def test_parse_agenda(self):
        with open(STATSCAL_FP) as fp:
            body = fp.read()
        httpretty.register_uri(httpretty.GET,
                               "http://www.ecb.europa.eu/press/calendars/statscal/html/index.en.html",
                               body=body,
                               #match_querystring=True,
                               status=200,
                               streaming=True,
                               content_type='text/html')
        model = [{'dataflow_key': 'BP6',
                  'reference_period': 'Q3 2015',
                  'scheduled_date': '13/01/2016 10:00 CET'},
                 {'dataflow_key': 'SEC',
                 'reference_period': 'Nov-2015',
                 'scheduled_date': '13/01/2016 10:00 CET'},
                {'dataflow_key': 'QSA',
                 'reference_period': 'Q3 2015',
                 'scheduled_date': '14/01/2016 10:00 CET'},
                {'dataflow_key': 'IRS',
                 'reference_period': 'Dec-2015',
                 'scheduled_date': '14/01/2016 10:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Nov-2015',
                 'scheduled_date': '19/01/2016 10:00 CET'},
                {'dataflow_key': 'BLS',
                 'reference_period': 'Jan-2016',
                 'scheduled_date': '19/01/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': 'Dec-2015',
                 'scheduled_date': '20/01/2016 10:00 CET'},
                {'dataflow_key': 'ICP',
                 'reference_period': 'Dec-2015',
                 'scheduled_date': '21/01/2016 11:00 CET'},
                {'dataflow_key': 'IVF',
                 'reference_period': 'Nov-2015',
                 'scheduled_date': '21/01/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': 'Q3 2015',
                 'scheduled_date': '26/01/2016 10:00 CET'},
                {'dataflow_key': 'QSA',
                 'reference_period': 'Q3 2015',
                 'scheduled_date': '28/01/2016 10:00 CET'},
                {'dataflow_key': 'RA6',
                 'reference_period': 'Dec-2015',
                 'scheduled_date': '29/01/2016 12:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Dec-2015',
                 'scheduled_date': '29/01/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Dec-2015',
                 'scheduled_date': '29/01/2016 10:00 CET'},
                {'dataflow_key': 'TGB',
                 'reference_period': 'Dec-2015',
                 'scheduled_date': '01/02/2016 10:00 CET'},
                {'dataflow_key': 'MIR',
                 'reference_period': 'Dec-2015',
                 'scheduled_date': '04/02/2016 10:00 CET'},
                {'dataflow_key': 'STP',
                 'reference_period': 'Jan-2016',
                 'scheduled_date': '05/02/2016 14:00 CET'},
                {'dataflow_key': 'EXR',
                 'reference_period': 'Jan-2016',
                 'scheduled_date': '08/02/2016 10:00 CET'},
                {'dataflow_key': 'QSA',
                 'reference_period': 'Q3 2015',
                 'scheduled_date': '08/02/2016 10:00 CET'},
                {'dataflow_key': 'SEC',
                 'reference_period': 'Dec-2015',
                 'scheduled_date': '10/02/2016 10:00 CET'},
                {'dataflow_key': 'IRS',
                 'reference_period': 'Jan-2016',
                 'scheduled_date': '10/02/2016 10:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Dec-2015',
                 'scheduled_date': '18/02/2016 10:00 CET'},
                {'dataflow_key': 'FVC',
                 'reference_period': 'Q4 2015',
                 'scheduled_date': '22/02/2016 10:00 CET'},
                {'dataflow_key': 'IVF',
                 'reference_period': 'Dec-2015 / Q4 2015',
                 'scheduled_date': '22/02/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': 'Jan-2016',
                 'scheduled_date': '23/02/2016 10:00 CET'},
                {'dataflow_key': 'ICP',
                 'reference_period': 'Jan-2016',
                 'scheduled_date': '25/02/2016 11:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Jan-2016 / Q4 2015',
                 'scheduled_date': '25/02/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Jan-2016 / Q4 2015',
                 'scheduled_date': '25/02/2016 10:00 CET'},
                {'dataflow_key': 'RA6',
                 'reference_period': 'Jan-2016',
                 'scheduled_date': '29/02/2016 12:00 CET'},
                {'dataflow_key': 'TGB',
                 'reference_period': 'Jan-2016',
                 'scheduled_date': '01/03/2016 10:00 CET'},
                {'dataflow_key': 'MIR',
                 'reference_period': 'Jan-2016',
                 'scheduled_date': '02/03/2016 10:00 CET'},
                {'dataflow_key': 'EXR',
                 'reference_period': 'Feb-2016',
                 'scheduled_date': '07/03/2016 10:00 CET'},
                {'dataflow_key': 'STP',
                 'reference_period': 'Feb-2016',
                 'scheduled_date': '07/03/2016 14:00 CET'},
                {'dataflow_key': 'SEC',
                 'reference_period': 'Jan-2016',
                 'scheduled_date': '10/03/2016 10:00 CET'},
                {'dataflow_key': 'IRS',
                 'reference_period': 'Feb-2016',
                 'scheduled_date': '10/03/2016 10:00 CET'},
                {'dataflow_key': 'ICP',
                 'reference_period': 'Feb-2016',
                 'scheduled_date': '17/03/2016 11:00 CET'},
                {'dataflow_key': 'IVF',
                 'reference_period': 'Jan-2016',
                 'scheduled_date': '17/03/2016 10:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Jan-2016',
                 'scheduled_date': '21/03/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': 'Feb-2016',
                 'scheduled_date': '22/03/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Feb-2016',
                 'scheduled_date': '29/03/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Feb-2016',
                 'scheduled_date': '29/03/2016 10:00 CET'},
                {'dataflow_key': 'RA6',
                 'reference_period': 'Feb-2016',
                 'scheduled_date': '31/03/2016 12:00 CET'},
                {'dataflow_key': 'TGB',
                 'reference_period': 'Feb-2016',
                 'scheduled_date': '01/04/2016 10:00 CET'},
                {'dataflow_key': 'ICPF',
                 'reference_period': 'Q4 2015',
                 'scheduled_date': '01/04/2016 10:00 CET'},
                {'dataflow_key': 'MIR',
                 'reference_period': 'Feb-2016',
                 'scheduled_date': '04/04/2016 10:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Q4 2015',
                 'scheduled_date': '07/04/2016 10:00 CET'},
                {'dataflow_key': 'STP',
                 'reference_period': 'Mar-2016',
                 'scheduled_date': '07/04/2016 14:00 CET'},
                {'dataflow_key': 'EXR',
                 'reference_period': 'Mar-2016',
                 'scheduled_date': '11/04/2016 10:00 CET'},
                {'dataflow_key': 'QSA',
                 'reference_period': 'Q4 2015',
                 'scheduled_date': '11/04/2016 10:00 CET'},
                {'dataflow_key': 'SEC',
                 'reference_period': 'Feb-2016',
                 'scheduled_date': '12/04/2016 10:00 CET'},
                {'dataflow_key': 'IRS',
                 'reference_period': 'Mar-2016',
                 'scheduled_date': '12/04/2016 10:00 CET'},
                {'dataflow_key': 'ICP',
                 'reference_period': 'Mar-2016',
                 'scheduled_date': '14/04/2016 11:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Feb-2016',
                 'scheduled_date': '19/04/2016 10:00 CET'},
                {'dataflow_key': 'IVF',
                 'reference_period': 'Feb-2016',
                 'scheduled_date': '19/04/2016 10:00 CET'},
                {'dataflow_key': 'BLS',
                 'reference_period': 'Apr-2016',
                 'scheduled_date': '19/04/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': 'Mar-2016',
                 'scheduled_date': '20/04/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': '2015',
                 'scheduled_date': '21/04/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': '2015',
                 'scheduled_date': '25/04/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': 'Q4 2015',
                 'scheduled_date': '26/04/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Mar-2016',
                 'scheduled_date': '27/04/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Mar-2016',
                 'scheduled_date': '27/04/2016 10:00 CET'},
                {'dataflow_key': 'RA6',
                 'reference_period': 'Mar-2016',
                 'scheduled_date': '29/04/2016 12:00 CET'},
                {'dataflow_key': 'QSA',
                 'reference_period': 'Q4 2015',
                 'scheduled_date': '29/04/2016 10:00 CET'},
                {'dataflow_key': 'TGB',
                 'reference_period': 'Mar-2016',
                 'scheduled_date': '02/05/2016 10:00 CET'},
                {'dataflow_key': 'MIR',
                 'reference_period': 'Mar-2016',
                 'scheduled_date': '03/05/2016 10:00 CET'},
                {'dataflow_key': 'EXR',
                 'reference_period': 'Apr-2016',
                 'scheduled_date': '09/05/2016 10:00 CET'},
                {'dataflow_key': 'STP',
                 'reference_period': 'Apr-2016',
                 'scheduled_date': '10/05/2016 14:00 CET'},
                {'dataflow_key': 'SEC',
                 'reference_period': 'Mar-2016',
                 'scheduled_date': '13/05/2016 10:00 CET'},
                {'dataflow_key': 'IRS',
                 'reference_period': 'Apr-2016',
                 'scheduled_date': '13/05/2016 10:00 CET'},
                {'dataflow_key': 'ICP',
                 'reference_period': 'Apr-2016',
                 'scheduled_date': '18/05/2016 11:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Mar-2016',
                 'scheduled_date': '20/05/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': 'Apr-2016',
                 'scheduled_date': '24/05/2016 10:00 CET'},
                {'dataflow_key': 'IVF',
                 'reference_period': 'Mar-2016 / Q1 2016',
                 'scheduled_date': '24/05/2016 10:00 CET'},
                {'dataflow_key': 'FVC',
                 'reference_period': 'Q1 2016',
                 'scheduled_date': '24/05/2016 10:00 CET'},
                {'dataflow_key': 'RA6',
                 'reference_period': 'Apr-2016',
                 'scheduled_date': '31/05/2016 12:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Apr-2016 / Q1 2016',
                 'scheduled_date': '31/05/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Apr-2016 / Q1 2016',
                 'scheduled_date': '31/05/2016 10:00 CET'},
                {'dataflow_key': 'TGB',
                 'reference_period': 'Apr-2016',
                 'scheduled_date': '01/06/2016 10:00 CET'},
                {'dataflow_key': 'MIR',
                 'reference_period': 'Apr-2016',
                 'scheduled_date': '03/06/2016 10:00 CET'},
                {'dataflow_key': 'STP',
                 'reference_period': 'May-2016',
                 'scheduled_date': '07/06/2016 14:00 CET'},
                {'dataflow_key': 'SEC',
                 'reference_period': 'Apr-2016',
                 'scheduled_date': '10/06/2016 10:00 CET'},
                {'dataflow_key': 'IRS',
                 'reference_period': 'May-2016',
                 'scheduled_date': '10/06/2016 10:00 CET'},
                {'dataflow_key': 'EXR',
                 'reference_period': 'May-2016',
                 'scheduled_date': '13/06/2016 10:00 CET'},
                {'dataflow_key': 'ICP',
                 'reference_period': 'May-2016',
                 'scheduled_date': '16/06/2016 11:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Apr-2016',
                 'scheduled_date': '17/06/2016 10:00 CET'},
                {'dataflow_key': 'IVF',
                 'reference_period': 'Apr-2016',
                 'scheduled_date': '21/06/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': 'May-2016',
                 'scheduled_date': '22/06/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'May-2016',
                 'scheduled_date': '27/06/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'May-2016',
                 'scheduled_date': '27/06/2016 10:00 CET'},
                {'dataflow_key': 'RA6',
                 'reference_period': 'May-2016',
                 'scheduled_date': '30/06/2016 12:00 CET'},
                {'dataflow_key': 'TGB',
                 'reference_period': 'May-2016',
                 'scheduled_date': '01/07/2016 10:00 CET'},
                {'dataflow_key': 'ICPF',
                 'reference_period': 'Q1 2016',
                 'scheduled_date': '01/07/2016 10:00 CET'},
                {'dataflow_key': 'MIR',
                 'reference_period': 'May-2016',
                 'scheduled_date': '05/07/2016 10:00 CET'},
                {'dataflow_key': 'STP',
                 'reference_period': 'Jun-2016',
                 'scheduled_date': '07/07/2016 14:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Q1 2016',
                 'scheduled_date': '08/07/2016 10:00 CET'},
                {'dataflow_key': 'EXR',
                 'reference_period': 'Jun-2016',
                 'scheduled_date': '11/07/2016 10:00 CET'},
                {'dataflow_key': 'SEC',
                 'reference_period': 'May-2016',
                 'scheduled_date': '12/07/2016 10:00 CET'},
                {'dataflow_key': 'IRS',
                 'reference_period': 'Jun-2016',
                 'scheduled_date': '12/07/2016 10:00 CET'},
                {'dataflow_key': 'ICP',
                 'reference_period': 'Jun-2016',
                 'scheduled_date': '15/07/2016 11:00 CET'},
                {'dataflow_key': 'BLS',
                 'reference_period': 'Jul-2016',
                 'scheduled_date': '19/07/2016 10:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'May-2016',
                 'scheduled_date': '20/07/2016 10:00 CET'},
                {'dataflow_key': 'IVF',
                 'reference_period': 'May-2016',
                 'scheduled_date': '20/07/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': 'Jun-2016',
                 'scheduled_date': '20/07/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': 'Q1 2016',
                 'scheduled_date': '25/07/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Jun-2016',
                 'scheduled_date': '27/07/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Jun-2016',
                 'scheduled_date': '27/07/2016 10:00 CET'},
                {'dataflow_key': 'RA6',
                 'reference_period': 'Jun-2016',
                 'scheduled_date': '29/07/2016 12:00 CET'},
                {'dataflow_key': 'TGB',
                 'reference_period': 'Jun-2016',
                 'scheduled_date': '01/08/2016 10:00 CET'},
                {'dataflow_key': 'MIR',
                 'reference_period': 'Jun-2016',
                 'scheduled_date': '02/08/2016 10:00 CET'},
                {'dataflow_key': 'STP',
                 'reference_period': 'Jul-2016',
                 'scheduled_date': '05/08/2016 14:00 CET'},
                {'dataflow_key': 'EXR',
                 'reference_period': 'Jul-2016',
                 'scheduled_date': '08/08/2016 10:00 CET'},
                {'dataflow_key': 'SEC',
                 'reference_period': 'Jun-2016',
                 'scheduled_date': '10/08/2016 10:00 CET'},
                {'dataflow_key': 'IRS',
                 'reference_period': 'Jul-2016',
                 'scheduled_date': '10/08/2016 10:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Jun-2016',
                 'scheduled_date': '18/08/2016 10:00 CET'},
                {'dataflow_key': 'IVF',
                 'reference_period': 'Jun-2016 / Q2 2016',
                 'scheduled_date': '18/08/2016 10:00 CET'},
                {'dataflow_key': 'FVC',
                 'reference_period': 'Q2 2016',
                 'scheduled_date': '18/08/2016 10:00 CET'},
                {'dataflow_key': 'ICP',
                 'reference_period': 'Jul-2016',
                 'scheduled_date': '18/08/2016 11:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': 'Jul-2016',
                 'scheduled_date': '23/08/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Jul-2016 / Q2 2016',
                 'scheduled_date': '26/08/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Jul-2016 / Q2 2016',
                 'scheduled_date': '26/08/2016 10:00 CET'},
                {'dataflow_key': 'RA6',
                 'reference_period': 'Jul-2016',
                 'scheduled_date': '31/08/2016 12:00 CET'},
                {'dataflow_key': 'TGB',
                 'reference_period': 'Jul-2016',
                 'scheduled_date': '01/09/2016 10:00 CET'},
                {'dataflow_key': 'MIR',
                 'reference_period': 'Jul-2016',
                 'scheduled_date': '01/09/2016 10:00 CET'},
                {'dataflow_key': 'STP',
                 'reference_period': 'Aug-2016',
                 'scheduled_date': '07/09/2016 14:00 CET'},
                {'dataflow_key': 'EXR',
                 'reference_period': 'Aug-2016',
                 'scheduled_date': '12/09/2016 10:00 CET'},
                {'dataflow_key': 'IRS',
                 'reference_period': 'Aug-2016',
                 'scheduled_date': '12/09/2016 10:00 CET'},
                {'dataflow_key': 'SEC',
                 'reference_period': 'Jul-2016',
                 'scheduled_date': '12/09/2016 10:00 CET'},
                {'dataflow_key': 'ICP',
                 'reference_period': 'Aug-2016',
                 'scheduled_date': '15/09/2016 11:00 CET'},
                {'dataflow_key': 'PSS',
                 'reference_period': '2015',
                 'scheduled_date': '16/09/2016 10:00 CET'},
                {'dataflow_key': 'IVF',
                 'reference_period': 'Jul-2016',
                 'scheduled_date': '16/09/2016 10:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Jul-2016',
                 'scheduled_date': '19/09/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': 'Aug-2016',
                 'scheduled_date': '22/09/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Aug-2016',
                 'scheduled_date': '27/09/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Aug-2016',
                 'scheduled_date': '27/09/2016 10:00 CET'},
                {'dataflow_key': 'RA6',
                 'reference_period': 'Aug-2016',
                 'scheduled_date': '30/09/2016 12:00 CET'},
                {'dataflow_key': 'ICPF',
                 'reference_period': 'Q2 2016',
                 'scheduled_date': '30/09/2016 10:00 CET'},
                {'dataflow_key': 'TGB',
                 'reference_period': 'Aug-2016',
                 'scheduled_date': '04/10/2016 10:00 CET'},
                {'dataflow_key': 'MIR',
                 'reference_period': 'Aug-2016',
                 'scheduled_date': '04/10/2016 10:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Q2 2016',
                 'scheduled_date': '07/10/2016 10:00 CET'},
                {'dataflow_key': 'EXR',
                 'reference_period': 'Sep-2016',
                 'scheduled_date': '10/10/2016 10:00 CET'},
                {'dataflow_key': 'STP',
                 'reference_period': 'Sep-2016',
                 'scheduled_date': '10/10/2016 14:00 CET'},
                {'dataflow_key': 'SEC',
                 'reference_period': 'Aug-2016',
                 'scheduled_date': '13/10/2016 10:00 CET'},
                {'dataflow_key': 'IRS',
                 'reference_period': 'Sep-2016',
                 'scheduled_date': '13/10/2016 10:00 CET'},
                {'dataflow_key': 'ICP',
                 'reference_period': 'Sep-2016',
                 'scheduled_date': '17/10/2016 11:00 CET'},
                {'dataflow_key': 'BLS',
                 'reference_period': 'Oct-2016',
                 'scheduled_date': '18/10/2016 10:00 CET'},
                {'dataflow_key': 'IVF',
                 'reference_period': 'Aug-2016',
                 'scheduled_date': '18/10/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': 'Sep-2016',
                 'scheduled_date': '19/10/2016 10:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Aug-2016',
                 'scheduled_date': '20/10/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': '2015',
                 'scheduled_date': '21/10/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': 'Q2 2016',
                 'scheduled_date': '25/10/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': '2015',
                 'scheduled_date': '25/10/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Sep-2016',
                 'scheduled_date': '27/10/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Sep-2016',
                 'scheduled_date': '27/10/2016 10:00 CET'},
                {'dataflow_key': 'RA6',
                 'reference_period': 'Sep-2016',
                 'scheduled_date': '31/10/2016 12:00 CET'},
                {'dataflow_key': 'TGB',
                 'reference_period': 'Sep-2016',
                 'scheduled_date': '02/11/2016 10:00 CET'},
                {'dataflow_key': 'MIR',
                 'reference_period': 'Sep-2016',
                 'scheduled_date': '03/11/2016 10:00 CET'},
                {'dataflow_key': 'EXR',
                 'reference_period': 'Oct-2016',
                 'scheduled_date': '07/11/2016 10:00 CET'},
                {'dataflow_key': 'STP',
                 'reference_period': 'Oct-2016',
                 'scheduled_date': '08/11/2016 14:00 CET'},
                {'dataflow_key': 'SEC',
                 'reference_period': 'Sep-2016',
                 'scheduled_date': '11/11/2016 10:00 CET'},
                {'dataflow_key': 'IRS',
                 'reference_period': 'Oct-2016',
                 'scheduled_date': '11/11/2016 10:00 CET'},
                {'dataflow_key': 'ICP',
                 'reference_period': 'Oct-2016',
                 'scheduled_date': '17/11/2016 11:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Sep-2016',
                 'scheduled_date': '18/11/2016 10:00 CET'},
                {'dataflow_key': 'FVC',
                 'reference_period': 'Q3 2016',
                 'scheduled_date': '21/11/2016 10:00 CET'},
                {'dataflow_key': 'IVF',
                 'reference_period': 'Sep-2016 / Q3 2016',
                 'scheduled_date': '21/11/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': 'Oct-2016',
                 'scheduled_date': '22/11/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Oct-2016 / Q3 2016',
                 'scheduled_date': '28/11/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Oct-2016 / Q3 2016',
                 'scheduled_date': '28/11/2016 10:00 CET'},
                {'dataflow_key': 'RA6',
                 'reference_period': 'Oct-2016',
                 'scheduled_date': '30/11/2016 12:00 CET'},
                {'dataflow_key': 'TGB',
                 'reference_period': 'Oct-2016',
                 'scheduled_date': '01/12/2016 10:00 CET'},
                {'dataflow_key': 'MIR',
                 'reference_period': 'Oct-2016',
                 'scheduled_date': '02/12/2016 10:00 CET'},
                {'dataflow_key': 'STP',
                 'reference_period': 'Nov-2016',
                 'scheduled_date': '07/12/2016 14:00 CET'},
                {'dataflow_key': 'EXR',
                 'reference_period': 'Nov-2016',
                 'scheduled_date': '12/12/2016 10:00 CET'},
                {'dataflow_key': 'IRS',
                 'reference_period': 'Nov-2016',
                 'scheduled_date': '12/12/2016 10:00 CET'},
                {'dataflow_key': 'SEC',
                 'reference_period': 'Oct-2016',
                 'scheduled_date': '12/12/2016 10:00 CET'},
                {'dataflow_key': 'ICP',
                 'reference_period': 'Nov-2016',
                 'scheduled_date': '16/12/2016 11:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Oct-2016',
                 'scheduled_date': '20/12/2016 10:00 CET'},
                {'dataflow_key': 'IVF',
                 'reference_period': 'Oct-2016',
                 'scheduled_date': '20/12/2016 10:00 CET'},
                {'dataflow_key': 'GFS',
                 'reference_period': 'Nov-2016',
                 'scheduled_date': '22/12/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Nov-2016',
                 'scheduled_date': '29/12/2016 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Nov-2016',
                 'scheduled_date': '29/12/2016 10:00 CET'},
                {'dataflow_key': 'RA6',
                 'reference_period': 'Nov-2016',
                 'scheduled_date': '30/12/2016 12:00 CET'},
                {'dataflow_key': 'TGB',
                 'reference_period': 'Nov-2016',
                 'scheduled_date': '02/01/2017 10:00 CET'},
                {'dataflow_key': 'MIR',
                 'reference_period': 'Nov-2016',
                 'scheduled_date': '04/01/2017 10:00 CET'},
                {'dataflow_key': 'STP',
                 'reference_period': 'Dec-2016',
                 'scheduled_date': '06/01/2017 14:00 CET'},
                {'dataflow_key': 'SEC',
                 'reference_period': 'Nov-2016',
                 'scheduled_date': '11/01/2017 10:00 CET'},
                {'dataflow_key': 'IRS',
                 'reference_period': 'Dec-2016',
                 'scheduled_date': '11/01/2017 10:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Q3 2016',
                 'scheduled_date': '13/01/2017 10:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Nov-2016',
                 'scheduled_date': '19/01/2017 10:00 CET'},
                {'dataflow_key': 'IVF',
                 'reference_period': 'Nov-2016',
                 'scheduled_date': '20/01/2017 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Dec-2016',
                 'scheduled_date': '27/01/2017 10:00 CET'},
                {'dataflow_key': 'BSI',
                 'reference_period': 'Dec-2016',
                 'scheduled_date': '27/01/2017 10:00 CET'},
                {'dataflow_key': 'RA6',
                 'reference_period': 'Dec-2016',
                 'scheduled_date': '31/01/2017 12:00 CET'},
                {'dataflow_key': 'TGB',
                 'reference_period': 'Dec-2016',
                 'scheduled_date': '01/02/2017 10:00 CET'},
                {'dataflow_key': 'MIR',
                 'reference_period': 'Dec-2016',
                 'scheduled_date': '02/02/2017 10:00 CET'},
                {'dataflow_key': 'SEC',
                 'reference_period': 'Dec-2016',
                 'scheduled_date': '10/02/2017 10:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Dec-2016',
                 'scheduled_date': '17/02/2017 10:00 CET'},
                {'dataflow_key': 'FVC',
                 'reference_period': 'Q4 2016',
                 'scheduled_date': '20/02/2017 10:00 CET'},
                {'dataflow_key': 'IVF',
                 'reference_period': 'Dec-2016 / Q4 2016',
                 'scheduled_date': '20/02/2017 10:00 CET'},
                {'dataflow_key': 'RA6',
                 'reference_period': 'Jan-2017',
                 'scheduled_date': '28/02/2017 12:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Jan-2017',
                 'scheduled_date': '22/03/2017 10:00 CET'},
                {'dataflow_key': 'RA6',
                 'reference_period': 'Feb-2017',
                 'scheduled_date': '31/03/2017 12:00 CET'},
                {'dataflow_key': 'BP6',
                 'reference_period': 'Q4 2016',
                 'scheduled_date': '10/04/2017 10:00 CET'}]
        self.assertEqual(list(self.fetcher.parse_agenda()), model)
 
        
        
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

if __name__ == '__main__':
    unittest.main()

