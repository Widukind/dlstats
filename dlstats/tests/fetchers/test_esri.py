# -*- coding: utf-8 -*-

"""
Created on Thu Nov 26 10:20:04 2015
@author: salimeh
"""

"""
Created on Thu Nov 26 10:20:04 2015
@author: salimeh
"""

import io
import tempfile
import datetime
import os
import pandas
from pprint import pprint
from urllib.parse import urlparse, urljoin
from urllib.request import url2pathname, pathname2url
import httpretty
import re

from dlstats.fetchers._commons import Datasets
from dlstats.fetchers import esri
from dlstats import constants

import unittest
from unittest import mock

from dlstats.tests.base import RESOURCES_DIR, BaseTestCase, BaseDBTestCase

PROVIDER_NAME = 'Esri'


dataset_codes = ['gaku-mg1522', 'gaku-mfy1522', 'def-qg1522', ]
dataset_codes = os.listdir('./tests/resources/esri/sna')
for i,d in enumerate(dataset_codes):
    dataset_codes[i] = d.replace('.csv','')

DATASETS = {d:{} for d in dataset_codes}

for d in DATASETS:
    DATASETS[d]['filename'] = d + '.csv'

DATASETS["series_names"] = ['nan', 'GDP (Expenditure Approach)', 'Private Consumption' ,'Consumption of Households' ,'Excluding Imputed Rent' ,
                            'Private Residential Investment' ,'Private Non-Resi.Investment' ,'Change in Private Inventories',
                            'Government Consumption' ,'Public Investment', 'Change in Public Inventories', 'Goods & Services, Net Exports',
                            'Goods & Services, Exports', 'Goods & Services, Imports','nan, nan',
                            'Income from/to the Rest of the World, Net','Income from/to the Rest of the World, Receipt',
                            'Income from/to the Rest of the World, Payment','GNI', 'nan',
                            'Domestic Demand','Private Demand', 'Public Demand', 'nan','Gross Fixed Capital Formation', 'nan',
                            'GDP, Excluding FISIM', 'Consumption of Households, Excluding FISIM', 'Export, Excluding FISIM' , 'Import, Excluding FISIM',
                            'Trading Gains/Losses', 'GDI', 'Residual']

DATASETS['gaku-mg1532']['name'] = 'Nominal Gross Domestic Product (original series)'
DATASETS['gaku-mg1532']['dimension_count'] = 1
DATASETS['gaku-mg1532']['series_count'] = len(DATASETS['series_names']) - 5
DATASETS['gaku-mg1532']['last_update'] = datetime.datetime(2015,12,10)
DATASETS['gaku-mg1532']['filename'] = 'gaku-mg1532.csv'
DATASETS['gaku-mfy1532']['name'] = 'Annual Nominal GDP (Fiscal Year)' 
DATASETS['gaku-mfy1532']['dimension_count'] = 1
DATASETS['gaku-mfy1532']['series_count'] = 25
DATASETS['gaku-mfy1532']['last_update'] = datetime.datetime(2015,12,10)
DATASETS['gaku-mfy1532']['filename'] = 'gaku-mfy1532.csv'
DATASETS['def-qg1532']['name'] = 'Deflators (original series)'
DATASETS['def-qg1532']['dimension_count'] = 1
DATASETS['def-qg1532']['series_count'] = 25
DATASETS['def-qg1532']['last_update'] = datetime.datetime(2015,12,10)
DATASETS['def-qg1532']['filename'] = 'def-qg1532.csv'

def make_url(self):
    import tempfile
    filepath = os.path.abspath(os.path.join(tempfile.gettempdir(), 
                                            PROVIDER_NAME, 
                                            "%s.csv" % self.dataset_code))
    return "file:%s" % pathname2url(filepath)

def local_get(url, *args, **kwargs):
    "Fetch a stream from local files."
    from requests import Response

    p_url = urlparse(url)
    if p_url.scheme != 'file':
        raise ValueError("Expected file scheme")

    filename = url2pathname(p_url.path)
    response = Response()
    response.status_code = 200
    response.raw = open(filename, 'rb')
    return response

def get_filepath(dataset_code):
    """Copy resource file to temp
    
    Return local filepath of file
    """
    from shutil import copyfile
    filename = DATASETS[dataset_code]['filename']
    test_resource_filepath = os.path.join('./tests/resources/esri/sna',filename)
    dirpath = os.path.join(tempfile.gettempdir(), PROVIDER_NAME)
    filepath = os.path.abspath(os.path.join(dirpath, filename))
    
    if os.path.exists(filepath):
        os.remove(filepath)
        
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    res = copyfile(test_resource_filepath,filepath)

    return filepath

def get_simple_file(filepath):
    with open(filepath) as fh:
        return fh.read()

def fake_release_date(arg):
    return datetime.datetime(1900,1,1)

class FakeDataset():
    def __init__(self,code):
        self.provider_name = 'Esri'
        self.dataset_code = code
        self.dimension_list = []
        self.attribute_list = []
        
@mock.patch('requests.get', local_get)
@mock.patch('dlstats.fetchers.esri.EsriData.make_url',make_url)
@mock.patch('dlstats.fetchers.esri.EsriData.get_release_date',fake_release_date)
class EsriFixSeriesTestCase(BaseTestCase):
    """ESRI fixing series names
    """

    # nosetests -s -v dlstats.tests.fetchers.test_esri:EsriFixSeriesTestCase

    def test_fix_series_name(self):

        # nosetests -s -v dlstats.tests.fetchers.test_esri:EsriFixSeriesTestCase.test_fix_series_name
        
        for d in dataset_codes:
            if re.match('gaku',d):
                print(d)
                filepath = get_filepath(d)
                self.assertTrue(os.path.exists(filepath))
                
                dataset = FakeDataset(d)
                dataset.last_update = datetime.datetime(2015,12,25)
                e = esri.EsriData(dataset,filename = d)
                
                variable_names = e.fix_series_names()
                for v in variable_names:
                    self.assertIn(v,DATASETS['series_names'])

class EsriParseDatesTestCase(BaseTestCase):

    def test_parse_dates(self):

        # nosetests -s -v dlstats.tests.fetchers.test_esri:EsriParseDatesTestCase.test_parse_dates

        dates = ['1994/4-3.', '1995/4-3.', '1996/4-3.']
        self.assertEqual(esri.parse_dates(dates),
                         ('A',
                          pandas.Period('1994',freq='A').ordinal,
                          pandas.Period('1996',freq='A').ordinal))

        dates = ['1994/ 1- 3.', '4- 6.', '7- 9.', '10-12.',
                 '1995/ 1- 3.', '4- 6.', '7- 9.', '10-12.',
                 '1996/ 1- 3.', '4- 6.', '7- 9.', '10-12.']
        self.assertEqual(esri.parse_dates(dates),
                         ('Q',
                          pandas.Period('1994Q1',freq='Q').ordinal,
                          pandas.Period('1996Q4',freq='Q').ordinal))

        dates = ['1994/ 4- 6.', '7- 9.', '10-12.',
                 '1995/ 1- 3.', '4- 6.', '7- 9.', '10-12.',
                 '1996/ 1- 3.', '4- 6.', '7- 9.']
        self.assertEqual(esri.parse_dates(dates),
                         ('Q',
                          pandas.Period('1994Q2',freq='Q').ordinal,
                          pandas.Period('1996Q3',freq='Q').ordinal))

        dates = ['1994/ 7- 9.', '10-12.',
                 '1995/ 1- 3.', '4- 6.', '7- 9.', '10-12.',
                 '1996/ 1- 3.', '4- 6.']
        self.assertEqual(esri.parse_dates(dates),
                         ('Q',
                          pandas.Period('1994Q3',freq='Q').ordinal,
                          pandas.Period('1996Q2',freq='Q').ordinal))

        dates = ['1994/ 10-12.',
                 '1995/ 1- 3.', '4- 6.', '7- 9.', '10-12.',
                 '1996/ 1- 3.']
        self.assertEqual(esri.parse_dates(dates),
                         ('Q',
                          pandas.Period('1994Q4',freq='Q').ordinal,
                          pandas.Period('1996Q1',freq='Q').ordinal))

class EsriDatasetsDBTestCase(BaseDBTestCase):
    """Fetchers Tests - with DB
    
    sources from DATASETS[dataset_code]['datas'] written in zip file
    """
    
    # nosetests -s -v dlstats.tests.fetchers.test_esri:EsriDatasetsDBTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)
        self.fetcher = esri.Esri(db=self.db)
        self.dataset_code = None
        self.dataset = None        
        self.filepath = None

    @mock.patch('requests.get', local_get)
    @mock.patch('dlstats.fetchers.esri.EsriData.make_url', make_url)    
    def _common_tests(self):

        self._collections_is_empty()
        
        self.filepath = get_filepath(self.dataset_code)
        self.assertTrue(os.path.exists(self.filepath))
        
        # provider.update_database
        self.fetcher.provider.update_database()
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": self.fetcher.provider_name})
        self.assertIsNotNone(provider)
        
        # upsert_categories
#        self.fetcher.upsert_categories()
#        category = self.db[constants.COL_CATEGORIES].find_one({"provider_name": self.fetcher.provider_name, 
#                                                               "categoryCode": self.dataset_code})
#        self.assertIsNotNone(category)
        
        dataset = Datasets(provider_name=self.fetcher.provider_name, 
                           dataset_code=self.dataset_code, 
                           name=DATASETS[self.dataset_code]['name'],
                           last_update=DATASETS[self.dataset_code]['last_update'],
                           fetcher=self.fetcher)

        # manual Data for iterator
        fetcher_data = esri.EsriData(dataset,filename=DATASETS[self.dataset_code]['filename']) 
        dataset.series.data_iterator = fetcher_data
        dataset.last_update = DATASETS[self.dataset_code]['last_update']
        dataset.update_database()

        self.dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                            "dataset_code": self.dataset_code})
        
        self.assertIsNotNone(self.dataset)
        
        self.assertEqual(len(self.dataset["dimension_list"]), DATASETS[self.dataset_code]["dimension_count"])
        
        series = self.db[constants.COL_SERIES].find({"provider_name": self.fetcher.provider_name, 
                                                     "dataset_code": self.dataset_code})

        self.assertEqual(series.count(), DATASETS[self.dataset_code]['series_count'])
        
        
    def test_gaku_mg1532(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_esri:EsriDatasetsDBTestCase.test_gaku_mg1532
                
        self.dataset_code = 'gaku-mg1532'
        
        self._common_tests()        

        attempt = DATASETS['gaku-mg1532']
        
        dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                            "dataset_code": self.dataset_code})

        self.assertEqual(dataset['name'], attempt['name'])
        self.assertEqual(dataset['provider_name'], 'esri')
        self.assertEqual(dataset['last_update'], attempt['last_update'])
        index = 0
        dimensions = []
        for name in DATASETS['series_names']:
            if (name != 'nan') and (name != 'nan, nan'):
                dimensions.append([str(index), name])
                index += 1
        self.maxDiff = None
        self.assertEqual(dataset['dimension_list'], {'concept': dimensions})
        self.assertEqual(dataset['attribute_list'], {})
        self.assertEqual(dataset['doc_href'], None)
                    
        
        series = self.db[constants.COL_SERIES].find_one({"provider_name": self.fetcher.provider_name, 
                                                         "dataset_code": self.dataset_code,
                                                         "key": '0'})
        self.assertIsNotNone(series)
        
        d = series['dimensions']
        self.assertEqual(d["concept"], '0')
        self.assertEqual(series['frequency'], 'Q')
        self.assertEqual(series['start_date'], pandas.Period('1994Q1',freq='Q').ordinal)
        self.assertEqual(series['end_date'], pandas.Period('2015Q3',freq='Q').ordinal)
        self.assertEqual(series['values'][0], '119,879.2')
        self.assertEqual(series['values'][-1], '122,343.3')

        series = self.db[constants.COL_SERIES].find_one({"provider_name": self.fetcher.provider_name, 
                                                         "dataset_code": self.dataset_code,
                                                         "key": '24'})
        self.assertIsNotNone(series)
        
        d = series['dimensions']
        self.assertEqual(d["concept"], '24')
        self.assertEqual(series['frequency'], 'Q')
        self.assertEqual(series['start_date'], pandas.Period('1994Q1',freq='Q').ordinal)
        self.assertEqual(series['end_date'], pandas.Period('2015Q3',freq='Q').ordinal)
        self.assertEqual(series['values'][0], '8,252.0')
        self.assertEqual(series['values'][-1], '23,560.8')

    def test_gaku_mfy1532(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_esri:EsriDatasetsDBTestCase.test_gaku_mg1532
                
        self.dataset_code = 'gaku-mfy1532'
        
        self._common_tests()        

        attempt = DATASETS['gaku-mfy1532']
        
        dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                            "dataset_code": self.dataset_code})

        self.assertEqual(dataset['name'], attempt['name'])
        self.assertEqual(dataset['provider_name'], 'esri')
        self.assertEqual(dataset['last_update'], attempt['last_update'])
        index = 0
        dimensions = []
        for name in DATASETS['series_names']:
            if (name != 'nan') and (name != 'nan, nan'):
                dimensions.append([str(index), name])
                index += 1
        self.maxDiff = None
        self.assertEqual(dataset['dimension_list'], {'concept': dimensions})
        self.assertEqual(dataset['attribute_list'], {})
        self.assertEqual(dataset['doc_href'], None)
                    
        
        series = self.db[constants.COL_SERIES].find_one({"provider_name": self.fetcher.provider_name, 
                                                         "dataset_code": self.dataset_code,
                                                         "key": '0'})
        self.assertIsNotNone(series)
        
        d = series['dimensions']
        self.assertEqual(d["concept"], '0')
        self.assertEqual(series['frequency'], 'A')
        self.assertEqual(series['start_date'], pandas.Period('1994',freq='A').ordinal)
        self.assertEqual(series['end_date'], pandas.Period('2014',freq='A').ordinal)
        self.assertEqual(series['values'][0], '495,612.2')
        self.assertEqual(series['values'][-1], '489,623.4')

        series = self.db[constants.COL_SERIES].find_one({"provider_name": self.fetcher.provider_name, 
                                                         "dataset_code": self.dataset_code,
                                                         "key": '24'})
        self.assertIsNotNone(series)
        
        d = series['dimensions']
        self.assertEqual(d["concept"], '24')
        self.assertEqual(series['frequency'], 'A')
        self.assertEqual(series['start_date'], pandas.Period('1994',freq='A').ordinal)
        self.assertEqual(series['end_date'], pandas.Period('2014',freq='A').ordinal)
        self.assertEqual(series['values'][0], '35,177.4')
        self.assertEqual(series['values'][-1], '99,695.5')

    def test_def_qg1532(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_esri:EsriDatasetsDBTestCase.test_gaku_mg1532
                
        self.dataset_code = 'def-qg1532'
        
        self._common_tests()        

        attempt = DATASETS['def-qg1532']
        
        dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                            "dataset_code": self.dataset_code})

        self.assertEqual(dataset['name'], attempt['name'])
        self.assertEqual(dataset['provider_name'], 'esri')
        self.assertEqual(dataset['last_update'], attempt['last_update'])
        index = 0
        dimensions = []
        for name in DATASETS['series_names']:
            if (name != 'nan') and (name != 'nan, nan'):
                dimensions.append([str(index), name])
                index += 1
        self.maxDiff = None
        self.assertEqual(dataset['dimension_list'], {'concept': dimensions})
        self.assertEqual(dataset['attribute_list'], {})
        self.assertEqual(dataset['doc_href'], None)
                    
        
        series = self.db[constants.COL_SERIES].find_one({"provider_name": self.fetcher.provider_name, 
                                                         "dataset_code": self.dataset_code,
                                                         "key": '0'})
        self.assertIsNotNone(series)
        
        d = series['dimensions']
        self.assertEqual(d["concept"], '0')
        self.assertEqual(series['frequency'], 'Q')
        self.assertEqual(series['start_date'], pandas.Period('1994Q1',freq='Q').ordinal)
        self.assertEqual(series['end_date'], pandas.Period('2015Q3',freq='Q').ordinal)
        self.assertEqual(series['values'][0], '109.8')
        self.assertEqual(series['values'][-1], '93.1')

        series = self.db[constants.COL_SERIES].find_one({"provider_name": self.fetcher.provider_name, 
                                                         "dataset_code": self.dataset_code,
                                                         "key": '24'})
        self.assertIsNotNone(series)
        
        d = series['dimensions']
        self.assertEqual(d["concept"], '24')
        self.assertEqual(series['frequency'], 'Q')
        self.assertEqual(series['start_date'], pandas.Period('1994Q1',freq='Q').ordinal)
        self.assertEqual(series['end_date'], pandas.Period('2015Q3',freq='Q').ordinal)
        self.assertEqual(series['values'][0], '86.7')
        self.assertEqual(series['values'][-1], '116.6')

@unittest.skipIf(True,'TODO')
class LightEsriDatasetsDBTestCase(BaseDBTestCase):
    """Fetchers Tests - with DB and lights sources
    
    1. Créer un fichier zip à partir des données du dict DATASETS
    
    2. Execute le fetcher normalement et en totalité
    """
    
    # nosetests -s -v dlstats.tests.fetchers.test_esri:LightEsriDatasetsDBTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)
        self.fetcher = esri.Esri(db=self.db)
        self.dataset_code = None
        self.dataset = None        
        self.filepath = None
        
    @mock.patch('requests.get', local_get)
    @mock.patch('dlstats.fetchers.esri.EsriData.make_url', make_url)    
    def _common_tests(self):

        self._collections_is_empty()

        # Write czv/zip file in local directory
        filepath = get_filepath(self.dataset_code)
        self.assertTrue(os.path.exists(filepath))
        # Replace dataset url by local filepath
        DATASETS[self.dataset_code]['url'] = "file:%s" % pathname2url(filepath)

        self.fetcher.provider.update_database()
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": self.fetcher.provider_name})
        self.assertIsNotNone(provider)
        
#        self.fetcher.upsert_categories()
#        category = self.db[constants.COL_CATEGORIES].find_one({"provider_name": self.fetcher.provider_name, 
#                                                               "categoryCode": self.dataset_code})
#        self.assertIsNotNone(category)

        self.fetcher.upsert_dataset(self.dataset_code)
        
        self.dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                            "dataset_code": self.dataset_code})
        self.assertIsNotNone(self.dataset)

        series = self.db[constants.COL_SERIES].find({"provider_name": self.fetcher.provider_name, 
                                                     "dataset_code": self.dataset_code})

        self.assertEqual(series.count(), DATASETS[self.dataset_code]['series_count'])

    def test_datasets(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_esri:LightEsriDatasetsDBTestCase.test_datasets

        for d in dataset_codes: 
            self.dataset_code = d        

            self._common_tests()

    @unittest.skipIf(True,'TODO')
    @mock.patch('requests.get', local_get)
    @mock.patch('dlstats.fetchers.esri.EsriData.make_url', make_url)    
    def test_selected_datasets(self):

        # nosetests -s -v dlstats.tests.fetchers.test_esri:LightEsriDatasetsDBTestCase.test_selected_datasets()

        self.fetcher.upsert_categories()

        self.fetcher.selected_codes = ['nama_10_gdp']

        datasets = self.fetcher.get_selected_datasets()

        for d in datasets:
            # Write czv/zip file in local directory
            filepath = get_filepath(d)
            self.assertTrue(os.path.exists(filepath))
            # Replace dataset url by local filepath
            DATASETS[d]['url'] = "file:%s" % pathname2url(filepath)

        self.fetcher.upsert_selected_datasets()
        
    @unittest.skipIf(True,'TODO')
    @mock.patch('requests.get', local_get)
    @mock.patch('dlstats.fetchers.esri.EsriData.make_url', make_url)    
    def test_upsert_all_datasets(self):

        # nosetests -s -v dlstats.tests.fetchers.test_esri:LightEsriDatasetsDBTestCase.test_upsert_all_datasets

        self.fetcher.upsert_categories()
        
        self.fetcher.selected_codes = ['nama_10_gdp','cat1']

        datasets = self.fetcher.get_selected_datasets()

        for d in datasets:
            # Write czv/zip file in local directory
            filepath = get_filepath(d)
            self.assertTrue(os.path.exists(filepath))
            # Replace dataset url by local filepath
            DATASETS[d]['url'] = "file:%s" % pathname2url(filepath)

        self.fetcher.upsert_all_datasets()

        categories = self.db[constants.COL_CATEGORIES].find({"provider_name": self.fetcher.provider_name, 
                                                             "exposed": True})
        self.assertEqual(categories.count(),3)


        # faking update
        global TABLE_OF_CONTENT
        tc_orig = TABLE_OF_CONTENT
        tc = TABLE_OF_CONTENT.decode(encoding='UTF_8')
        tc = tc.replace('last_update>26.10.2015','last_update>01.11.2015')
        TABLE_OF_CONTENT = tc.encode(encoding='UTF_8')

        self.fetcher.upsert_all_datasets()
        
        dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                               "dataset_code": 'nama_10_gdp'})
        self.assertEqual(dataset['last_update'],datetime.datetime(2015,11,1))

        dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                               "dataset_code": 'dset1'})
        self.assertEqual(dataset['last_update'],datetime.datetime(2015,11,1))

        dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                               "dataset_code": 'dset2'})
        self.assertEqual(dataset['last_update'],datetime.datetime(2015,11,1))

        # restoring TABLE_OF_CONTENT to original
        TABLE_OF_CONTENT = tc_orig
        
        #TODO: meta_datas tests  

@unittest.skipUnless('FULL_REMOTE_TEST' in os.environ, "Skip - not full remote test")
class FullEsriDatasetsDBTestCase(BaseDBTestCase):
    """Fetchers Tests - with DB and real download sources
    
    1. Télécharge ou utilise des fichiers existants
    
    2. Execute le fetcher normalement et en totalité
    """
    
    # FULL_REMOTE_TEST=1 nosetests -s -v dlstats.tests.fetchers.test_esri:FullEsriDatasetsDBTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)
        self.fetcher = esri.Esri(db=self.db)
        self.dataset_code = None
        self.dataset = None        
        self.filepath = None
        
    #@mock.patch('requests.get', local_get)
    def _common_tests(self):

        self._collections_is_empty()

        self.fetcher.provider.update_database()
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": self.fetcher.provider_name})
        self.assertIsNotNone(provider)
        
        self.fetcher.upsert_categories()
        category = self.db[constants.COL_CATEGORIES].find_one({"provider_name": self.fetcher.provider_name, 
                                                               "categoryCode": self.dataset_code})
        self.assertIsNotNone(category)
        
        self.fetcher.upsert_dataset(self.dataset_code)
        
        self.dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                            "dataset_code": self.dataset_code})
        self.assertIsNotNone(self.dataset)

        series = self.db[constants.COL_SERIES].find({"provider_name": self.fetcher.provider_name, 
                                                     "dataset_code": self.dataset_code})

        series_count = series.count()
        self.assertTrue(series_count > 1)
        print(self.dataset_code, series_count)

    def test_nama_10_gdp(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_esri:FullEsriDatasetsDBTestCase.test_nama_10_gdp

        self.dataset_code = 'nama_10_gdp'        

        self._common_tests()
        
        #self.fail("test")

        #TODO: meta_datas tests  

class EsriUrlsTestCase(BaseTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test_esri:EsriUrlsTestCase

    def setUp(self):
        BaseTestCase.setUp(self)
        self.fetcher = esri.Esri()
        
    @httpretty.activate
    def test_esri_parse(self):

        # nosetests -s -v dlstats.tests.fetchers.test_esri:EsriUrlsTestCase.test_parse_QGDP

        url = esri.INDEX_URL
        httpretty.register_uri(httpretty.GET, url,
                               body = get_simple_file('./tests/resources/esri/Statistics - Cabinet Office Home Page.html'))
        httpretty.register_uri(httpretty.GET,urljoin(url,"en/sna/sokuhou/sokuhou_top.html"),
                               body = get_simple_file('./tests/resources/esri/Quarterly Estimates of GDP - National Accounts.html'))
        httpretty.register_uri(httpretty.GET, esri.DATABASES['QGDP']['url_base'] + esri.DATABASES['QGDP']['filename'],
                               body = get_simple_file('./tests/resources/esri/Quarterly Estimates of GDP - Release Archive - National Accounts.html'))
        httpretty.register_uri(httpretty.GET, esri.DATABASES['QGDP']['url_base'] + '2015/toukei_2015.html',
                               body = get_simple_file('./tests/resources/esri/Quarterly Estimates of GDP - Release Archive - 2015 - National Accounts.html'))
        httpretty.register_uri(httpretty.GET, esri.DATABASES['QGDP']['url_base'] + '2015/qe153_2/gdemenuea.html',
                               body = get_simple_file('./tests/resources/esri/Jul.-Sep. 2015 (The 2nd preliminary) - National Accounts.html'))
        httpretty.register_uri(httpretty.GET,urljoin(esri.INDEX_URL,"en/stat/di/di-e.html"),
                               body = get_simple_file('./tests/resources/esri/Indexes of Business Conditions:ESRI - Cabinet Office Home Page.html'))
        httpretty.register_uri(httpretty.GET,urljoin(esri.INDEX_URL,"en/stat/juchu/juchu-e.html"),
                               body = get_simple_file('./tests/resources/esri/Machinery Orders :ESRI - Cabinet Office Home Page.html'))
        httpretty.register_uri(httpretty.GET,urljoin(esri.INDEX_URL,"en/stat/shouhi/shouhi-e.html"),
                               body = get_simple_file('./tests/resources/esri/Consumer Confidence Survey:ESRI - Cabinet Office Home Page.html'))
        httpretty.register_uri(httpretty.GET,urljoin(esri.INDEX_URL,"en/stat/hojin/hojin-e.html"),
                               body = get_simple_file('./tests/resources/esri/Business Outlook Survey:ESRI - Cabinet Office Home Page.html'))
        httpretty.register_uri(httpretty.GET,urljoin(esri.INDEX_URL,"en/stat/ank/ank-e.html"),
                               body = get_simple_file('./tests/resources/esri/Annual Survey of Corporate Behavior:ESRI - Cabinet Office Home Page.html'))
        
        esri.parse_esri_site()

        self.fetcher.upsert_categories()

        self.fetcher.make_datasets_dict()
