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

PROVIDER_NAME = 'ESRI'


dataset_codes = ['gaku-mg', 'gaku-mfy', 'def-qg', ]
dataset_codes = os.listdir('./tests/resources/esri/sna')
for i,d in enumerate(dataset_codes):
    dataset_codes[i] = d.replace('1532.csv','')

DATASETS = {d:{} for d in dataset_codes}

for d in DATASETS:
    DATASETS[d]['filename'] = d + '1532.csv'

DATASETS["series_names"] = ['nan', 'GDP (Expenditure Approach)', 'Private Consumption' ,'Consumption of Households' ,'Excluding Imputed Rent' ,
                            'Private Residential Investment' ,'Private Non-Resi.Investment' ,'Change in Private Inventories',
                            'Government Consumption' ,'Public Investment', 'Change in Public Inventories', 'Goods & Services, Net Exports',
                            'Goods & Services, Exports', 'Goods & Services, Imports','nan, nan',
                            'Income from/to the Rest of the World, Net','Income from/to the Rest of the World, Receipt',
                            'Income from/to the Rest of the World, Payment','GNI', 'nan',
                            'Domestic Demand','Private Demand', 'Public Demand', 'nan','Gross Fixed Capital Formation', 'nan',
                            'GDP, Excluding FISIM', 'Consumption of Households, Excluding FISIM', 'Export, Excluding FISIM' , 'Import, Excluding FISIM',
                            'Trading Gains/Losses', 'GDI', 'Residual']

DATASETS['gaku-mg']['name'] = 'Nominal Gross Domestic Product (original series)'
DATASETS['gaku-mg']['dimension_count'] = 1
DATASETS['gaku-mg']['series_count'] = 25
DATASETS['gaku-mg']['last_update'] = datetime.datetime(2015,12,4)
DATASETS['gaku-mg']['filename'] = 'gaku-mg1532.csv'
DATASETS['gaku-mfy']['name'] = 'Annual Nominal GDP (fiscal year)' 
DATASETS['gaku-mfy']['dimension_count'] = 1
DATASETS['gaku-mfy']['series_count'] = 25
DATASETS['gaku-mfy']['last_update'] = datetime.datetime(2015,12,4)
DATASETS['gaku-mfy']['filename'] = 'gaku-mfy1532.csv'
DATASETS['def-qg']['name'] = 'Deflators (original series)'
DATASETS['def-qg']['dimension_count'] = 1
DATASETS['def-qg']['series_count'] = 25
DATASETS['def-qg']['last_update'] = datetime.datetime(2015,12,4)
DATASETS['def-qg']['filename'] = 'def-qg1532.csv'

def make_url(self):
    import tempfile
    filepath = os.path.abspath(os.path.join('./tests/resources/esri/sna',
                                            "%s1532.csv" % self.dataset_code))
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

def get_simple_japanese_file(filepath):
    with open(filepath,'rb') as fh:
        return fh.read()

def fake_release_date(arg):
    return datetime.datetime(1900,1,1)

def httpretty_setup():
    url = esri.INDEX_URL
    httpretty.register_uri(httpretty.GET, url,
                           body = get_simple_file('./tests/resources/esri/Statistics - Cabinet Office Home Page.html'))
    httpretty.register_uri(httpretty.GET,urljoin(url,"en/sna/sokuhou/sokuhou_top.html"),
                           body = get_simple_file('./tests/resources/esri/Quarterly Estimates of GDP - National Accounts.html'))
    url_base = 'http://www.esri.cao.go.jp/en/sna/data/sokuhou/files/'
    httpretty.register_uri(httpretty.GET, url_base + 'toukei_top.html',
                           body = get_simple_file('./tests/resources/esri/Quarterly Estimates of GDP - Release Archive - National Accounts.html'))
    httpretty.register_uri(httpretty.GET, url_base + '2015/toukei_2015.html',
                           body = get_simple_file('./tests/resources/esri/Quarterly Estimates of GDP - Release Archive - 2015 - National Accounts.html'))
    httpretty.register_uri(httpretty.GET, url_base + '2015/qe153_2/gdemenuea.html',
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
    httpretty.register_uri(httpretty.GET,urljoin(esri.INDEX_URL,"jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-mg1532.csv"),
                           body = get_simple_japanese_file('./tests/resources/esri/sna/gaku-mg1532.csv'))
    httpretty.register_uri(httpretty.GET,urljoin(esri.INDEX_URL,"jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-mfy1532.csv"),
                           body = get_simple_japanese_file('./tests/resources/esri/sna/gaku-mfy1532.csv'))
    for d in dataset_codes:
        httpretty.register_uri(httpretty.GET,urljoin(esri.INDEX_URL,"jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/"+d+"1532.csv"),
                               body = get_simple_japanese_file('./tests/resources/esri/sna/'+d+'1532.csv'))

class FakeDataset():
    def __init__(self,code):
        self.provider_name = 'ESRI'
        self.dataset_code = code
        self.dimension_list = []
        self.attribute_list = []

@mock.patch('requests.get', local_get)
@mock.patch('dlstats.fetchers.esri.Esri.make_url',make_url)
class EsriFixSeriesTestCase(BaseTestCase):
    """ESRI fixing series names
    """

    # nosetests -s -v dlstats.tests.fetchers.test_esri:EsriFixSeriesTestCase

    def test_fix_series_name(self):

        # nosetests -s -v dlstats.tests.fetchers.test_esri:EsriFixSeriesTestCase.test_fix_series_name
        
        for d in dataset_codes:
            if re.match('gaku',d):
                filepath = get_filepath(d)
                self.assertTrue(os.path.exists(filepath))
                
                dataset = FakeDataset(d)
                dataset.last_update = datetime.datetime(2015,12,25)
                self.dataset_code = d
                e = esri.EsriData(dataset,make_url(self),filename = d)
                
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
    @mock.patch('dlstats.fetchers.esri.Esri.make_url', make_url)    
    def _common_tests(self):

        self._collections_is_empty()
        
        self.filepath = get_filepath(self.dataset_code)
        self.assertTrue(os.path.exists(self.filepath))
        
        # provider.update_database
        self.fetcher.provider.update_database()
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": self.fetcher.provider_name})
        self.assertIsNotNone(provider)
        
        dataset = Datasets(provider_name=self.fetcher.provider_name, 
                           dataset_code=self.dataset_code, 
                           name=DATASETS[self.dataset_code]['name'],
                           last_update=DATASETS[self.dataset_code]['last_update'],
                           fetcher=self.fetcher)

        # manual Data for iterator
        fetcher_data = esri.EsriData(dataset,make_url(self),filename=DATASETS[self.dataset_code]['filename']) 
        dataset.series.data_iterator = fetcher_data
        dataset.last_update = DATASETS[self.dataset_code]['last_update']
        dataset.update_database()

        self.dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                            "dataset_code": self.dataset_code})
        
        self.assertIsNotNone(self.dataset)

        dimensions = self.dataset["dimension_list"]
        self.assertEqual(len(dimensions), DATASETS[self.dataset_code]["dimension_count"])
        for c in dimensions['concept']:
            self.assertIn(c[1],DATASETS['series_names'])
        
        series = self.db[constants.COL_SERIES].find({"provider_name": self.fetcher.provider_name, 
                                                     "dataset_code": self.dataset_code})

        self.assertEqual(series.count(), DATASETS[self.dataset_code]['series_count'])
        
        
    @httpretty.activate
    def test_data_tree(self):

        # nosetests -s -v dlstats.tests.fetchers.test_esir:EsriDatasetsDBTestCase.test_data_tree

        httpretty_setup()
        
        self._collections_is_empty()

        # test ESRI site parsing
        site = esri.parse_esri_site()
        self.assertEqual(type(site),list)
        # to be changed with inclusion of Business Statistics
        self.assertEqual(len(site),1)

        # test data_tree()
        self.fetcher.build_data_tree()
        datasets_list = self.fetcher.datasets_list(category_filter='ESRI.SNA.QuarterlyGDP.GDP.Amount')

        self.assertEqual(len(datasets_list), 8)

        datasets = [
            {'dataset_code': 'gaku-jcy',
             'last_update': datetime.datetime(2015, 12, 4),
             'exposed': True,
             'metadata': {'url': 'http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-jcy1532.csv',
                          'doc_href': None},
             'name': 'Annual Real GDP (calendar year)'
            },
            {'dataset_code': 'gaku-jfy',
             'name': 'Annual Real GDP (fiscal year)',
             'last_update': datetime.datetime(2015, 12, 4),
             'exposed': True,
             'metadata': {'url': 'http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-jfy1532.csv',
                          'doc_href': None}
            },
            {'dataset_code': 'gaku-jg',
             'name': 'Real Gross Domestic Product (original series)',
             'last_update': datetime.datetime(2015, 12, 4),
             'exposed': True,
             'metadata': {'url': 'http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-jg1532.csv',
                          'doc_href': None}
            },
            {'dataset_code': 'gaku-jk',
             'name': 'Real Gross Domestic Product (seasonally adjusted series)',
             'last_update': datetime.datetime(2015, 12, 4),
             'exposed': True,
             'metadata': {'url': 'http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-jk1532.csv',
                          'doc_href': None}
            },
            {'dataset_code': 'gaku-mcy',
             'name': 'Annual Nominal GDP (calendar year)',
             'last_update': datetime.datetime(2015, 12, 4),
             'exposed': True,
             'metadata': {'url': 'http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-mcy1532.csv',
                          'doc_href': None}
            },
            {'dataset_code': 'gaku-mfy',
             'name': 'Annual Nominal GDP (fiscal year)',
             'last_update': datetime.datetime(2015, 12, 4),
             'exposed': True,
             'metadata': {'url': 'http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-mfy1532.csv',
                          'doc_href': None}
            },
            {'dataset_code': 'gaku-mg',
             'name': 'Nominal Gross Domestic Product (original series)',
             'last_update': datetime.datetime(2015, 12, 4),
             'exposed': True,
             'metadata': {'url': 'http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-mg1532.csv',
                          'doc_href': None}
            },
            {'dataset_code': 'gaku-mk',
             'name': 'Nominal Gross Domestic Product (seasonally adjusted series)',
             'last_update': datetime.datetime(2015, 12, 4),
             'exposed': True,
             'metadata': {'url': 'http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-mk1532.csv',
                          'doc_href': None}
            }
        ]

        self.assertEqual(datasets_list,datasets)

        self.fetcher.provider.update_database()
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": self.fetcher.provider_name})
        self.assertIsNotNone(provider)
        
        attempted_data_tree = [
            {'category_code': 'ESRI',
             'datasets': [],
             'description': None,
             'doc_href': 'http://www.esri.cao.go.jp/index-e.html',
             'exposed': False,
             'last_update': None,
             'name': 'ESRI'
            },
            {'category_code': 'ESRI.SNA',
             'datasets': [],
             'description': None,
             'doc_href': None,
             'exposed': False,
             'last_update': None,
             'name': 'National accounts of Japan'
            },
            {'category_code': 'ESRI.SNA.QuarterlyGDP',
             'datasets': [],
             'description': None,
             'doc_href': None,
             'exposed': False,
             'last_update': None,
             'name': 'Tables of GDP and its components (1994:I-2015:Ⅲ) (Expenditure approach)'
            },
            {'category_code': 'ESRI.SNA.QuarterlyGDP.GDP',
             'datasets': [],
             'description': None,
             'doc_href': None,
             'exposed': False,
             'last_update': None,
             'name': 'I. GDP (expenditure approach) and its components'
            },
            {'category_code': 'ESRI.SNA.QuarterlyGDP.GDP.Amount',
             'datasets': [
                 {'dataset_code': 'gaku-jcy',
                  'last_update': datetime.datetime(2015, 12, 4),
                  'exposed': True,
                  'metadata': {'url': 'http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-jcy1532.csv',
'doc_href': None},
                  'name': 'Annual Real GDP (calendar year)'
                 },
                 {'dataset_code': 'gaku-jfy',
                  'name': 'Annual Real GDP (fiscal year)',
                  'last_update': datetime.datetime(2015, 12, 4),
                  'exposed': True,
                  'metadata': {'url': 'http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-jfy1532.csv',
                               'doc_href': None}
                 },
                 {'dataset_code': 'gaku-jg',
                  'name': 'Real Gross Domestic Product (original series)',
                  'last_update': datetime.datetime(2015, 12, 4),
                  'exposed': True,
                  'metadata': {'url': 'http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-jg1532.csv',
                               'doc_href': None}
                 },
                 {'dataset_code': 'gaku-jk',
                  'name': 'Real Gross Domestic Product (seasonally adjusted series)',
                  'last_update': datetime.datetime(2015, 12, 4),
                  'exposed': True,
                  'metadata': {'url': 'http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-jk1532.csv',
                               'doc_href': None}
                 },
                 {'dataset_code': 'gaku-mcy',
                  'name': 'Annual Nominal GDP (calendar year)',
                  'last_update': datetime.datetime(2015, 12, 4),
                  'exposed': True,
                  'metadata': {'url': 'http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-mcy1532.csv',
                               'doc_href': None}
                 },
                 {'dataset_code': 'gaku-mfy',
                  'name': 'Annual Nominal GDP (fiscal year)',
                  'last_update': datetime.datetime(2015, 12, 4),
                  'exposed': True,
                  'metadata': {'url': 'http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-mfy1532.csv',
                               'doc_href': None}
                 },
                 {'dataset_code': 'gaku-mg',
                  'name': 'Nominal Gross Domestic Product (original series)',
                  'last_update': datetime.datetime(2015, 12, 4),
                  'exposed': True,
                  'metadata': {'url': 'http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-mg1532.csv',
                               'doc_href': None}
                 },
                 {'dataset_code': 'gaku-mk',
                  'name': 'Nominal Gross Domestic Product (seasonally adjusted series)',
                  'last_update': datetime.datetime(2015, 12, 4),
                  'exposed': True,
                  'metadata': {'url': 'http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2015/qe153_2/__icsFiles/afieldfile/2015/12/04/gaku-mk1532.csv',
                               'doc_href': None}
                 }
             ],
             'description': None,
             'doc_href': None,
             'exposed': True,
             'last_update': None,
             'name': 'Amount'
            }
        ]

        data_tree = provider.get('data_tree')
        codes = 'ESRI.SNA.QuarterlyGDP.GDP.Amount'.split('.')
        self.maxDiff = None
        for i in range(len(codes)):
            code = '.'.join(codes[:i+1])
            category = [c for c in data_tree if c['category_code'] == code]
            self.assertEqual(attempted_data_tree[i],category[0])

    def test_gaku_mg(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_esri:EsriDatasetsDBTestCase.test_gaku_mg
                
        self.dataset_code = 'gaku-mg'
        
        self._common_tests()        

        attempt = DATASETS['gaku-mg']
        
        dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                            "dataset_code": self.dataset_code})

        self.assertEqual(dataset['name'], attempt['name'])
        self.assertEqual(dataset['provider_name'], 'ESRI')
        self.assertEqual(dataset['last_update'], attempt['last_update'])
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

    def test_gaku_mfy(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_esri:EsriDatasetsDBTestCase.test_gaku_mg
                
        self.dataset_code = 'gaku-mfy'
        
        self._common_tests()        

        attempt = DATASETS['gaku-mfy']
        
        dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                            "dataset_code": self.dataset_code})

        self.assertEqual(dataset['name'], attempt['name'])
        self.assertEqual(dataset['provider_name'], 'ESRI')
        self.assertEqual(dataset['last_update'], attempt['last_update'])
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

    def test_def_qg(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_esri:EsriDatasetsDBTestCase.test_gaku_mg
                
        self.dataset_code = 'def-qg'
        
        self._common_tests()        

        attempt = DATASETS['def-qg']
        
        dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                            "dataset_code": self.dataset_code})

        self.assertEqual(dataset['name'], attempt['name'])
        self.assertEqual(dataset['provider_name'], 'ESRI')
        self.assertEqual(dataset['last_update'], attempt['last_update'])
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
        
    @httpretty.activate
    def _common_tests(self):

        self._collections_is_empty()

        
        httpretty_setup()
        self.fetcher.provider.update_database()
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": self.fetcher.provider_name})
        self.assertIsNotNone(provider)

        self.fetcher.build_data_tree()
        
        self.fetcher.upsert_dataset(self.dataset_code)

        self.dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                                 "dataset_code": self.dataset_code})
        self.assertIsNotNone(self.dataset)

        series = self.db[constants.COL_SERIES].find({"provider_name": self.fetcher.provider_name, 
                                                     "dataset_code": self.dataset_code})

        self.assertEqual(series.count(), DATASETS[self.dataset_code]['series_count'])

    def test_gaku_mg(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_esri:LightEsriDatasetsDBTestCase.test_gaku_mg
                
        self.dataset_code = 'gaku-mg'
        
        self._common_tests()        

        attempt = DATASETS['gaku-mg']
        
        dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                            "dataset_code": self.dataset_code})

        self.assertEqual(dataset['name'], attempt['name'])
        self.assertEqual(dataset['provider_name'], 'ESRI')
        self.assertEqual(dataset['last_update'], attempt['last_update'])
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

    def test_gaku_mfy(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_esri:LightEsriDatasetsDBTestCase.test_gaku_mg
                
        self.dataset_code = 'gaku-mfy'
        
        self._common_tests()        

        attempt = DATASETS['gaku-mfy']
        
        dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                            "dataset_code": self.dataset_code})

        self.assertEqual(dataset['name'], attempt['name'])
        self.assertEqual(dataset['provider_name'], 'ESRI')
        self.assertEqual(dataset['last_update'], attempt['last_update'])
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

    @unittest.skipIf(True,'TODO')
    def test_def_qg(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_esri:LightEsriDatasetsDBTestCase.test_gaku_mg
                
        self.dataset_code = 'def-qg'
        
        self._common_tests()        

        attempt = DATASETS['def-qg']
        
        dataset = self.db[constants.COL_DATASETS].find_one({"provider_name": self.fetcher.provider_name, 
                                                            "dataset_code": self.dataset_code})

        self.assertEqual(dataset['name'], attempt['name'])
        self.assertEqual(dataset['provider_name'], 'ESRI')
        self.assertEqual(dataset['last_update'], attempt['last_update'])
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

    def test_gaku_mg(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_esri:FullEsriDatasetsDBTestCase.test_gaku_mg

        self.dataset_code = 'gaku_mg'        

        self._common_tests()
        
        #self.fail("test")

        #TODO: meta_datas tests  

