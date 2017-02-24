# -*- coding: utf-8 -*-

from datetime import datetime
import os

from dlstats.fetchers.bls import Bls as Fetcher
from dlstats.fetchers.bls import SeriesIterator, BlsData, get_date, get_ordinal_from_period
from dlstats.fetchers._commons import Datasets

import httpretty
import unittest

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase



RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "bls"))

def get_filepath(name):
    return os.path.abspath(os.path.join(RESOURCES_DIR, name))

BLS_HTML_PAGES = [
    ("https://www.bls.gov/data/", "Databases, Tables & Calculators by Subject.html")
]

DATA_CU = {
    "filepath": get_filepath("cu/cu.html"),
    "dirname": get_filepath("cu"),
    "code_list_files": ['area', 'base', 'footnote', 'item', 'period', 'periodicity'],
    "series_file" : "series",
    "DSD": {
        "filepath": None,
        "dataset_code": "cu",
        "dsd_id": "cu",
        "is_completed": True,
        "categories_key": "cu",
        "categories_parents": ["BLS", "Inflation"],
        "categories_root": ['BLS'],
        "concept_keys": ['area', 'item', 'seasonal', 'periodicity', 'base', 'base-period', 'footnote'],
        "codelist_keys": ['area', 'item', 'seasonal', 'periodicity', 'base', 'base-period', 'footnote'],
        "codelist_count": {
            'area': 3,
            'item':None,
            'seasonal': None,
            'periodicity': None,
            'base': None,
            'base_period': None,
            'footnote': None,
        },                
        "dimension_keys": ['area', 'item', 'seasonal', 'periodicity', 'base', 'base-period'],
        "dimension_count": {
            'area': None,
            'item':None,
            'seasonal': None,
            'periodicity': None,
            'base': None,
            'base_period': None,
        },
        "attribute_keys": ['footnote'],
        "attribute_count": {
            'footnotes': None,
        },
    },
    "series_accept": 22,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 462,
    "series_key_first": "0",
    "series_key_last": "21",
    "series_sample": {
        'provider_name': 'BLS',
        'dataset_code': 'cu',
        'key': '0',
        'name': 'Consumption of Households',
        'frequency': 'A',
        'last_update': None,
        'first_value': {
            'value': '105.7',
            'period': '1994',
            'attributes': None,
        },
        'last_value': {
            'value': '95.2',
            'period': '2014',
            'attributes': None
        },
        'dimensions': {
            "concept" : "0"
        },
        'attributes': None
    }
}


class FetcherTestCase(BaseFetcherTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test_bls:FetcherTestCase
    
    FETCHER_KLASS = Fetcher    
    DATASETS = {
        'cu': DATA_CU,
    }    
    DATASET_FIRST = "cu"
    DATASET_LAST = "cu"
    #DEBUG_MODE = False
    
    def _load_files(self, dataset_code=None):
        
        for url, filename in BLS_HTML_PAGES:
            filepath = get_filepath(filename)
            self.assertTrue(os.path.exists(filepath))
            self.register_url(url, filepath, content_type='text/html')
            
    def _load_files_dataset_cu(self):
        url = "https://download.bls.gov/pub/time.series/cu/"
        self.register_url(url, self.DATASETS["cu"]["filepath"],
                          content_type='text/html')
        url = "https://download.bls.gov/pub/time.series/cu/cu.series"
        self.register_url(url, self.DATASETS["cu"]["dirname"]+"/cu.series",
                          content_type='text')
        url = "https://download.bls.gov/pub/time.series/cu/cu.data.0.Current"
        self.register_url(url, self.DATASETS["cu"]["dirname"]+"/cu.data.0.Current",
                          content_type='text')
        url = "https://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems"
        self.register_url(url, self.DATASETS["cu"]["dirname"]+"/cu.data.1.AllItems",
                          content_type='text')
        url = "https://download.bls.gov/pub/time.series/cu/cu.data.10.OtherWest"
        self.register_url(url, self.DATASETS["cu"]["dirname"]+"/cu.data.10.OtherWest",
                          content_type='text')
        url = "https://download.bls.gov/pub/time.series/cu/cu.data.20.USCommoditiesServicesSpecial"
        self.register_url(url, self.DATASETS["cu"]["dirname"]+"/cu.data.20.USCommoditiesServicesSpecial",
                          content_type='text')
        for name in self.DATASETS["cu"]["code_list_files"]:
            url = "https://download.bls.gov/pub/time.series/cu/cu." + name
            self.register_url(url, self.DATASETS["cu"]["dirname"]+'/cu.'+name,
                              content_type='text')
            

    @httpretty.activate     
    def test_get_date(self):
        date1 = get_date('1971','M01','M')[0]
        period1 = get_ordinal_from_period(date1,freq='M')
        self.assertEqual(period1,12)
        date1 = get_date('1971','M12','M')[0]
        period1 = get_ordinal_from_period(date1,freq='M')
        self.assertEqual(period1,23)
        date1 = get_date('1971','M13','M')[0]
        period1 = get_ordinal_from_period(date1,freq='A')
        self.assertEqual(period1,1)
        date1 = get_date('1971','S1','S')[0]
        period1 = get_ordinal_from_period(date1,freq='S')
        self.assertEqual(period1,2)
        date1 = get_date('1971','S2','S')[0]
        period1 = get_ordinal_from_period(date1,freq='S')
        self.assertEqual(period1,3)

    @httpretty.activate     
    def test_iter_row(self):
        self._load_files_dataset_cu()
        url = "https://download.bls.gov/pub/time.series/cu/cu.data.10.OtherWest"
        filename = 'cu.data.10.OtherWest'
        si = SeriesIterator(url,filename,None,True)
        row = next(si.iter_row(url,filename,None,True))
        self.assertEqual(row,['CUUR0400AA0', '1966', 'M12', '53.3', ''])

    @httpretty.activate     
    def test_get_value(self):
        self._load_files_dataset_cu()
        url = "https://download.bls.gov/pub/time.series/cu/cu.data.10.OtherWest"
        filename = 'cu.data.10.OtherWest'
        si = SeriesIterator(url,filename,None,True)
        row = ['CUUR0400AA0', '1966', 'M12', '53.3', '']
        period = -37
        value_target = {
            'attributes': None,
            'period': "-37",
            'value': "53.3"
        }
        value = si.get_value(row,period)
        self.assertEqual(value,value_target)

    @httpretty.activate     
    def test_fill_series(self):
        self._load_files_dataset_cu()
        url = "https://download.bls.gov/pub/time.series/cu/cu.data.10.OtherWest"
        filename = 'cu.data.10.OtherWest'
        si = SeriesIterator(url,filename,None,True)
        series_in = [{
            'attributes': None,
            'period': "-37",
            'value': "53.3"
        }]
        previous_period = -37
        period = -34
        series_out = si.fill_series(series_in,previous_period,period)
        series_target = [
            {
                'attributes': None,
                'period': "-37",
                'value': "53.3"
            },  
            {
                'attributes': None,
                'period': "-36",
                'value': "nan"
            },  
            {
                'attributes': None,
                'period': "-35",
                'value': "nan"
            },  
        ]
        self.assertEqual(series_out,series_target)

    @httpretty.activate     
    def test_SeriesIterator(self):
        self._load_files_dataset_cu()
        url = "https://download.bls.gov/pub/time.series/cu/cu.data.10.OtherWest"
        filename = 'cu.data.10.OtherWest'
        si = SeriesIterator(url,filename,None,True)
        series = next(si)
        values = series['values']
        self.assertEqual(len(values),12*(2016-1966) + 2) 
        value_1 = { 
            'attributes': None,
            'period': "-37",
            'value': "53.3"
        }
        self.assertEqual(values[0],value_1)
        value_2 = {
            'attributes': None,
            'period': "-36",
            'value': "nan"
        }
        self.assertEqual(values[1],value_2)
        value_last = {
            'attributes': None,
            'period': str(12*(2017-1970)),
            'value': "405.426"
        }
        self.assertEqual(values[-1],value_last)
        values_annual = series['values_annual']
        self.assertEqual(len(values_annual),2016-1967 + 1) 
        value_1 = { 
            'attributes': None,
            'period': "-3",
            'value': "53.9"
        }
        self.assertEqual(values_annual[0],value_1)
        value_2 = {
            'attributes': None,
            'period': "-2",
            'value': "55.9"
        }
        self.assertEqual(values_annual[1],value_2)
        value_last = {
            'attributes': None,
            'period': str(2016-1970),
            'value': "400.402"
        }
        self.assertEqual(values_annual[-1],value_last)
        si = SeriesIterator(url,filename,None,True)
        count = 0
        for s in si:
            count +=1
        self.assertEqual(count,2)
        
    @httpretty.activate     
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    def test_load_datasets_first(self):

        dataset_code = "cu"
        self._load_files(dataset_code)
        self._load_files_dataset_cu()
        self.assertLoadDatasetsFirst([dataset_code])

    @httpretty.activate     
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    def test_load_datasets_update(self):

        dataset_code = "cu"
        self._load_files(dataset_code)
        self._load_files_dataset_cu()
        self.assertLoadDatasetsUpdate([dataset_code])

    @httpretty.activate
    @unittest.skipUnless(False, 'not yet implemented')
    def test_build_data_tree(self):

        dataset_code = "cu"
        self._load_files(dataset_code)
        self.assertDataTree(dataset_code)
        
    @httpretty.activate
    def test_BlsData(self):
        self._load_files_dataset_cu()
        url = "https://download.bls.gov/pub/time.series/cu/"
        fetcher = Fetcher(db=self.db)
        dataset = Datasets(provider_name='BLS', 
                           dataset_code='cu', 
                           name='All Urban Consumers', 
                           doc_href='', 
                           last_update='2017-02-07', 
                           fetcher=fetcher)
        bls_data = BlsData(dataset,url)
        self.assertEqual(len(bls_data.data_iterators),2)
        count = 0
        while True:
            try:
                bson = next(bls_data)
            except StopIteration:
                break
            count += 1
        self.assertEqual(count,7)
        
    @httpretty.activate     
    def test_upsert_dataset_cu(self):

        # nosetests -s -v dlstats.tests.fetchers.test_bls:FetcherTestCase.test_upsert_dataset_cu
        
        dataset_code = "cu"
        self._load_files(dataset_code)
        self._load_files_dataset_cu()
    
        self.assertProvider()
        dataset = self.assertDataset(dataset_code)        
        series_list = self.assertSeries(dataset_code)
        
        dataset["last_update"] = datetime(2017, 2 , 13)

        for series in series_list:
            self.assertEquals(series["last_update_ds"], dataset["last_update"])

