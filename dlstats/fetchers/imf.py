# -*- coding: utf-8 -*-

import os
import csv
from datetime import datetime
from re import match
import logging

import requests
from lxml import etree

from widukind_common import errors

from dlstats.utils import Downloader, get_ordinal_from_period, clean_datetime
from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats.xml_utils import select_dimension

VERSION = 3

logger = logging.getLogger(__name__)

FREQUENCIES_SUPPORTED = ["A", "Q", "M"]
FREQUENCIES_REJECTED = []

DATASETS = {
    'WEO': { 
        'name': 'World Economic Outlook',
        'doc_href': 'http://www.imf.org/external/ns/cs.aspx?id=28',
        'previous_datasets': [],
    },
    'WEO-GROUPS': { 
        'name': 'World Economic Outlook (groups)',
        'doc_href': 'http://www.imf.org/external/ns/cs.aspx?id=28',
        'previous_datasets': [],
    },
    'BOP': { 
        'name': 'Balance of Payments',
        'doc_href': 'http://data.imf.org/BOP',
        'previous_datasets': [],
    },
    'BOPAGG': {
        'name': 'Balance of Payments, World and Regional Aggregates',
        'doc_href': 'http://data.imf.org/BOP',
        'previous_datasets': [],
    },
    'DOT': { 
        'name': 'Direction of Trade Statistics',
        'doc_href': 'http://data.imf.org/DOT',
        'previous_datasets': [],
    },                         
    'IFS': { 
        'name': 'International Financial Statistics',
        'doc_href': 'http://data.imf.org/IFS',
        'previous_datasets': [],
    },
    'COMMP': { 
        'name': 'Primary Commodity Prices',
        'doc_href': 'http://data.imf.org',
        'previous_datasets': [],
    },
    'COMMPP': { 
        'name': 'Primary Commodity Prices Projections',
        'doc_href': 'http://data.imf.org',
        'previous_datasets': [],
    },
    'GFSR': { 
        'name': 'Government Finance Statistics, Revenue',
        'doc_href': 'http://data.imf.org/COFR',
        'previous_datasets': ['GFSR2015']
    },
    'GFSSSUC': { 
        'name': 'Government Finance Statistics, Statement of Sources and Uses of Cash',
        'doc_href': 'http://data.imf.org/COFR',
        'previous_datasets': ['GFSSSUC2015']
    },
    'GFSCOFOG': { 
        'name': 'Government Finance Statistics, Expenditure by Function of Government',
        'doc_href': 'http://data.imf.org/COFR',
        'previous_datasets': ['GFSCOFOG2015']
    },
    'GFSFALCS': { 
        'name': 'Government Finance Statistics, Financial Assets and Liabilities by Counterpart Sector',
        'doc_href': 'http://data.imf.org/COFR',
        'previous_datasets': ['GFSFALCS2015']
    },
    'GFSIBS': { 
        'name': 'Government Finance Statistics, Integrated Balance Sheet (Stock Positions and Flows in Assets and Liabilities)',
        'doc_href': 'http://data.imf.org/COFR',
        'previous_datasets': ['GFSIBS2015']
    },
    'GFSMAB': { 
        'name': 'Government Finance Statistics, Main Aggregates and Balances',
        'doc_href': 'http://data.imf.org/COFR',
        'previous_datasets': ['GFSMAB2015']
    },
    'GFSE': { 
        'name': 'Government Finance Statistics, Expense',
        'doc_href': 'http://data.imf.org/COFR',
        'previous_datasets': ['GFSE2015']
    },
    'FSI': { 
        'name': 'Financial Soundness Indicators',
        'doc_href': 'http://data.imf.org/FSI',
        'previous_datasets': [],
    },
    #'RT': { 
    #    'name': 'International Reserves Template',
    #    'doc_href': 'http://data.imf.org/RT',
    #    'previous_datasets': [],
    #},
    'FAS': { 
        'name': 'Financial Access Survey',
        'doc_href': 'http://data.imf.org/FAS',
        'previous_datasets': [],
    },
    'COFER': { 
        'name': 'Currency Composition of Official Foreign Exchange Reserves',
        'doc_href': 'http://data.imf.org/COFER',
        'previous_datasets': [],
    },
    'CDIS': { 
        'name': 'Coordinated Direct Investment Survey',
        'doc_href': 'http://data.imf.org/CDIS',
        'previous_datasets': [],
    },
    'CPIS': {                                    # frequency S (semi annual)
        'name': 'Coordinated Portfolio Investment Survey',
        'doc_href': 'http://data.imf.org/CPIS',
        'previous_datasets': [],
    },
    'WoRLD': { 
        'name': 'World Revenue Longitudinal Data',
        'doc_href': 'http://data.imf.org',
        'previous_datasets': [],
    },
    'MCDREO': { 
        'name': 'Middle East and Central Asia Regional Economic Outlook',
        'doc_href': 'http://data.imf.org/MCDREO',
        'previous_datasets': ['MCDREO201410', 'MCDREO201501', 'MCDREO201505', 'MCDREO201510']
    },
    'APDREO': { 
        'name': 'Asia and Pacific Regional Economic Outlook',
        'doc_href': 'http://data.imf.org/APDREO',
        'previous_datasets': ['APDREO201410', 'APDREO201504', 'APDREO201510']
    },
    'AFRREO': { 
        'name': 'Sub-Saharan Africa Regional Economic Outlook',
        'doc_href': 'http://data.imf.org/AFRREO',
        'previous_datasets': ['AFRREO201410', 'AFRREO201504', 'AFRREO201510']
    },
    'WHDREO': {                                   # bug: KeyError: 'NGDP_FY'
        'name': 'Western Hemisphere Regional Economic Outlook',
        'doc_href': 'http://data.imf.org/WHDREO',
        'previous_datasets': ['WHDREO201504', 'WHDREO201510']
    },
    'WCED': {                                     # bug: KeyError: 'OP'
        'name': 'World Commodity Exporters',
        'doc_href': 'http://data.imf.org/WCED',
        'previous_datasets': [],
    },
    'CPI': {
        'name': 'Consumer Price Index',
        'doc_href': 'http://data.imf.org/CPI',
        'previous_datasets': [],
    },
    'COFR': {                                     # Erreur 500
        'name': 'Coverage of Fiscal Reporting',
        'doc_href': 'http://data.imf.org/COFR',
        'previous_datasets': [],
    },
    'ICSD': {                                     # bug: KeyError: 'IGOV'
        'name': 'Investment and Capital Stock',
        'doc_href': 'http://data.imf.org/ICSD',
        'previous_datasets': [],
    },
    'HPDD': {                                     # bug: KeyError: 'GGXWDG'
        'name': 'Historical Public Debt',
        'doc_href': 'http://data.imf.org/HPDD',
        'previous_datasets': [],
    },
    'PGI': { 
        'name': 'Principal Global Indicators',
        'doc_href': 'http://data.imf.org/PGI',
        'previous_datasets': [],
    },
}

"""
FSIRE
Financial Soundness Indicators (FSI), Balance Sheets

FSIBS
Financial Soundness Indicators (FSI), Reporting Entities

FSIREM
Financial Soundness Indicators (FSI), Reporting Entities - Multidimensional

GFS01M
Government Finance Statistics (GFS 2001) - Multidimensional

GFS01
Government Finance Statistics (GFS 2001)

FM (FM201410, FM201504, FM201510)
Fiscal Monitor (FM)

CDISARCHIVE
Coordinated Direct Investment Survey (CDIS) - Archive

IRFCL
International Reserves and Foreign Currency Liquidity (IRFCL)

RAFIT2AGG
RA-FIT Round 2 Aggregates

GFSYR2014
Government Finance Statistics Yearbook (GFSY 2014), Revenue

GFSYSSUC2014
Government Finance Statistics Yearbook (GFSY 2014), Statement of Sources and Uses of Cash

GFSYCOFOG2014
Government Finance Statistics Yearbook (GFSY 2014), Expenditure by Function of Government (COFOG)

GFSYIBS2014
Government Finance Statistics Yearbook (GFSY 2014), Integrated Balance Sheet (Stock Positions and Flows in Assets and Liabilities)

GFSYMAB2014
Government Finance Statistics Yearbook (GFSY 2014), Main Aggregates and Balances

GFSYFALCS2014
Government Finance Statistics Yearbook (GFSY 2014), Financial Assets and Liabilities by Counterpart Sector

GFSYE2014
Government Finance Statistics Yearbook (GFSY 2014), Expense

BOPSDMXUSD
Balance of Payments (BOP), Global SDMX (US Dollars)
"""


CATEGORIES = [
    {
        "provider_name": "IMF",
        "category_code": "BOFS",
        "name": "Balance of Payments Statistics",
        "position": 1,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "BOP",
                "name": DATASETS["BOP"]["name"],
                "last_update": None,
                "metadata": {
                    "doc_href": DATASETS["BOP"]["doc_href"]
                }
            },
            {
                "dataset_code": "BOPAGG",
                "name": DATASETS["BOPAGG"]["name"],
                "last_update": None,
                "metadata": {
                    "doc_href": DATASETS["BOPAGG"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "PCP",
        "name": "Primary Commodity Prices",
        "position": 2,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "COMMP",
                "name": DATASETS["COMMP"]["name"],
                "last_update": None,
                "metadata": {
                    "doc_href": DATASETS["COMMP"]["doc_href"]
                }
            },                     
            {
                "dataset_code": "COMMPP",
                "name": DATASETS["COMMPP"]["name"],
                "last_update": None,
                "metadata": {
                    "doc_href": DATASETS["COMMPP"]["doc_href"]
                }
            },                     
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "GFS",
        "name": "Government Finance Statistics",
        "position": 3,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "GFSCOFOG",
                "name": DATASETS["GFSCOFOG"]["name"],
                "last_update": None,
                "metadata": {
                    "doc_href": DATASETS["GFSCOFOG"]["doc_href"]
                }
            },
            {
                "dataset_code": "GFSE",
                "name": DATASETS["GFSE"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["GFSE"]["doc_href"]
                }
            },
            {
                "dataset_code": "GFSFALCS",
                "name": DATASETS["GFSFALCS"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["GFSFALCS"]["doc_href"]
                }
            },
            {
                "dataset_code": "GFSIBS",
                "name": DATASETS["GFSIBS"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["GFSIBS"]["doc_href"]
                }
            },
            {
                "dataset_code": "GFSMAB",
                "name": DATASETS["GFSMAB"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["GFSMAB"]["doc_href"]
                }
            },
            {
                "dataset_code": "GFSR",
                "name": DATASETS["GFSR"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["GFSR"]["doc_href"]
                }
            },
            {
                "dataset_code": "GFSSSUC",
                "name": DATASETS["GFSSSUC"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["GFSSSUC"]["doc_href"]
                }
            },            
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "CDIS",
        "name": DATASETS["CDIS"]["name"],
        "position": 4,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "CDIS",
                "name": DATASETS["CDIS"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["CDIS"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "CPIS",
        "name": DATASETS["CPIS"]["name"],
        "position": 5,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "CPIS",
                "name": DATASETS["CPIS"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["CPIS"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "COFER",
        "name": DATASETS["COFER"]["name"],
        "position": 6,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "COFER",
                "name": DATASETS["COFER"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["COFER"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "DOT",
        "name": DATASETS["DOT"]["name"],
        "position": 7,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "DOT",
                "name": DATASETS["DOT"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["DOT"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "FAS",
        "name": DATASETS["FAS"]["name"],
        "position": 8,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "FAS",
                "name": DATASETS["FAS"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["FAS"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "FSI",
        "name": DATASETS["FSI"]["name"],
        "position": 9,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "FSI",
                "name": DATASETS["FSI"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["FSI"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "REO",
        "name": "Regional Economic Outlook",
        "position": 10,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "AFRREO",
                "name": DATASETS["AFRREO"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["AFRREO"]["doc_href"]
                }
            },
            {
                "dataset_code": "MCDREO",
                "name": DATASETS["MCDREO"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["MCDREO"]["doc_href"]
                }
            },
            {
                "dataset_code": "APDREO",
                "name": DATASETS["APDREO"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["APDREO"]["doc_href"]
                }
            },
            {
                "dataset_code": "WHDREO",
                "name": DATASETS["WHDREO"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["WHDREO"]["doc_href"]
                }
            },                    
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "IFS",
        "name": DATASETS["IFS"]["name"],
        "position": 11,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "IFS",
                "name": DATASETS["IFS"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["IFS"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
    #{
    #    "provider_name": "IMF",
    #    "category_code": "RT",
    #    "name": DATASETS["RT"]["name"],
    #    "position": 12,
    #    "parent": None,
    #    "all_parents": [],
    #    "doc_href": None,
    #    "datasets": [
    #        {
    #            "dataset_code": "RT",
    #            "name": DATASETS["RT"]["name"], 
    #            "last_update": None,                 
    #            "metadata": {
    #                "doc_href": DATASETS["RT"]["doc_href"]
    #            }
    #        },
    #    ],
    #    "metadata": {}
    #},
    {
        "provider_name": "IMF",
        "category_code": "WoRLD",
        "name": DATASETS["WoRLD"]["name"],
        "position": 13,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "WoRLD",
                "name": DATASETS["WoRLD"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["WoRLD"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "WEO",
        "name": DATASETS["WEO"]["name"],
        "position": 14,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "WEO",
                "name": DATASETS["WEO"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["WEO"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "WEO-GROUPS",
        "name": DATASETS["WEO-GROUPS"]["name"],
        "position": 14,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "WEO-GROUPS",
                "name": DATASETS["WEO-GROUPS"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["WEO-GROUPS"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "PGI",
        "name": DATASETS["PGI"]["name"],
        "position": 15,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "PGI",
                "name": DATASETS["PGI"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["PGI"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "WCED",
        "name": DATASETS["WCED"]["name"],
        "position": 16,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "WCED",
                "name": DATASETS["WCED"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["WCED"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "CPI",
        "name": DATASETS["CPI"]["name"],
        "position": 17,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "CPI",
                "name": DATASETS["CPI"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["CPI"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "COFR",
        "name": DATASETS["COFR"]["name"],
        "position": 18,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "COFR",
                "name": DATASETS["COFR"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["COFR"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "ICSD",
        "name": DATASETS["ICSD"]["name"],
        "position": 19,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "ICSD",
                "name": DATASETS["ICSD"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["ICSD"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
    {
        "provider_name": "IMF",
        "category_code": "HPDD",
        "name": DATASETS["HPDD"]["name"],
        "position": 20,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "HPDD",
                "name": DATASETS["HPDD"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["HPDD"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
]

class IMF(Fetcher):

    def __init__(self, **kwargs):        
        super().__init__(provider_name='IMF', version=VERSION, **kwargs)

        self.provider = Providers(name=self.provider_name, 
                                  long_name="International Monetary Fund",
                                  version=VERSION, 
                                  region='World', 
                                  website='http://www.imf.org/',
                                  terms_of_use='http://www.imf.org/external/terms.htm',
                                  fetcher=self)
        
        self.requests_client = requests.Session()

    def build_data_tree(self):
        
        return CATEGORIES
        
    def upsert_dataset(self, dataset_code):
        
        settings = DATASETS[dataset_code]
        
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code, 
                           name=settings['name'], 
                           doc_href=settings['doc_href'],
                           fetcher=self)

        klass = None
        if dataset_code in DATASETS_KLASS:
            klass = DATASETS_KLASS[dataset_code]
        else:
            klass = DATASETS_KLASS["JSON"]

        dataset.series.data_iterator = klass(dataset)
        
        return dataset.update_database()

import time
import hashlib
import json
from pprint import pprint

def retry(tries=1, sleep_time=2):
    """Retry calling the decorated function
    :param tries: number of times to try
    :type tries: int
    """
    def try_it(func):
        def f(*args, **kwargs):
            attempts = 0
            while True:
                try:
                    return func(*args,**kwargs)
                except Exception as e:
                    logger.warning("retry url[%s]" % args[1])
                    args[0].retry_count += 1
                    attempts += 1
                    if attempts > tries:
                        raise e
                    time.sleep(sleep_time * attempts)
        return f
    return try_it

class IMF_JSON_Data(SeriesIterator):
    
    def __init__(self, dataset=None):
        super().__init__(dataset)

        self.store_path = self.get_store_path()
        
        self.url_base = "http://dataservices.imf.org/REST/SDMX_JSON.svc"
        self.frequency_field = None
        
        self.retry_count = 0
        
        self.current_dataset_code = self.dataset_code
        
        self.previous_datasets = []
        self.previous_last_update = None
        if not self.dataset.last_update and DATASETS[self.dataset_code].get('previous_datasets'):
            self.previous_datasets = DATASETS[self.dataset_code].get('previous_datasets')
            self.previous_datasets.append(self.dataset_code)
            self.rows = self._get_data_by_dimension_multi_datasets()
        else:
            self.load_dsd()
            self.rows = self._get_data_by_dimension()

    @retry(tries=10, sleep_time=2)
    def download_json(self, url, params={}):
        
        if not os.path.exists(self.store_path):
            os.makedirs(self.store_path, exist_ok=True)
        
        filename = "%s.json" % hashlib.sha224(url.encode("utf-8")).hexdigest()
        filepath = os.path.abspath(os.path.join(self.store_path, filename))
        
        if os.path.exists(filepath):
            if os.path.getsize(filepath) == 0:
                os.remove(filepath)
            elif not self.fetcher.use_existing_file:
                os.remove(filepath)
                
        if not os.path.exists(filepath):
            response = self.fetcher.requests_client.get(url, params=params, stream=True, allow_redirects=False)
            #response = requests.get(url, params=params, stream=True, allow_redirects=False)
            
            logger.info("download url[%s] - filepath[%s] - status_code[%s]" %  (response.url, filepath, response.status_code))
            
            if response.status_code >= 400:
                return
    
            response.raise_for_status()
    
            with open(filepath, mode='wb') as f:
                for chunk in response.iter_content():
                    f.write(chunk)
                    
            self.fetcher.for_delete.append(filepath)
        else:
            logger.info("use exist filepath[%s]" % filepath)
                
        with open(filepath) as f:
            return json.load(f)

    def _get_url_dsd(self):
        return "http://dataservices.imf.org/REST/SDMX_JSON.svc/DataStructure/%s" % self.current_dataset_code 

    def _get_url_data(self):
        return "http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/%s" % self.current_dataset_code
    
    def load_dsd(self):
        url = self._get_url_dsd()
        json_dsd = self.download_json(url)
        
        codelists = {}
        concepts = {}
        codelists_list = self._get_list(json_dsd['Structure']['CodeLists']['CodeList'])
        
        for cl in codelists_list:
            cl_key = cl["@id"]
            #cl_name = cl["Name"]["#text"]
            if not cl_key in codelists:
                codelists[cl_key] = {}
            
            code_list = self._get_list(cl["Code"])
            for code in code_list:
                try:
                    codelists[cl_key][code["@value"]] = code["Description"]["#text"]
                except Exception as err:
                    raise

        concepts_list = self._get_list(json_dsd['Structure']['Concepts']['ConceptScheme']['Concept'])
        for concept in concepts_list:
            concepts[concept["@id"]] = concept["Name"]["#text"]
            
        dimensions_list = self._get_list(json_dsd['Structure']['KeyFamilies']['KeyFamily']['Components']['Dimension'])
        for dim in dimensions_list:
            if not dim["@conceptRef"] in self.dataset.dimension_keys:
                self.dataset.dimension_keys.append(dim["@conceptRef"])
            self.dataset.codelists[dim["@conceptRef"]] = codelists[dim["@codelist"]]
            
            if dim.get('@isFrequencyDimension') and dim.get('@isFrequencyDimension') == "true":
                self.frequency_field = dim["@conceptRef"]

        attributes_list = self._get_list(json_dsd['Structure']['KeyFamilies']['KeyFamily']['Components']['Attribute'])
        for attr in attributes_list:
            if not attr["@conceptRef"] in self.dataset.attribute_keys:
                self.dataset.attribute_keys.append(attr["@conceptRef"])
            if "@codelist" in attr:
                self.dataset.codelists[attr["@conceptRef"]] = codelists[attr["@codelist"]]
            else:
                self.dataset.codelists[attr["@conceptRef"]] = {}

        '''last_update process'''
        last_update_str = None
        if self.previous_last_update:
            self.dataset.last_update = self.previous_last_update
        else:
            annotations_list = self._get_list(json_dsd['Structure']['KeyFamilies']['KeyFamily']['Annotations']['Annotation'])
            for annotation in annotations_list:
                if annotation['AnnotationTitle'] == 'Latest Update Date':
                    last_update_str = annotation['AnnotationText']['#text']
                    break
            if last_update_str:
                last_update = clean_datetime(datetime.strptime(last_update_str, '%m/%d/%Y'))
                if not self.dataset.last_update:
                    self.dataset.last_update = last_update
                elif not self.previous_datasets:
                    if self.dataset.last_update >= last_update:
                        comments = "update-date[%s]" % last_update
                        raise errors.RejectUpdatedDataset(provider_name=self.provider_name,
                                                          dataset_code=self.dataset_code,
                                                          comments=comments)
                    else:
                        self.dataset.last_update = last_update
                else:
                    self.dataset.last_update = last_update
            else:
                self.dataset.last_update = clean_datetime()
        
        self.dataset.concepts = concepts
        
        #TODO: datasets.notes from Annotations
        annotations_list = self._get_list(json_dsd['Structure']['KeyFamilies']['KeyFamily']['Annotations']['Annotation'])
        for annotation in annotations_list:
            print("AnnotationTitle : ", annotation['AnnotationTitle'])
        
    def _get_dimensions_from_dsd(self):
        dimensions = {}
        for key in self.dataset.dimension_keys:
            dimensions[key] = self.dataset.codelists[key]
        return self.dataset.dimension_keys, dimensions 

    def _get_data_by_dimension_multi_datasets(self):
        for dataset_code in self.previous_datasets:
            self.current_dataset_code = dataset_code
            
            if self.current_dataset_code != self.dataset_code:
                year = int(self.current_dataset_code.split(self.dataset_code)[1][:4])
                try:
                    month = int(self.current_dataset_code.split(self.dataset_code)[1][-2:])
                except ValueError:
                    month = 1
                if month <= 0 or month > 12:
                    month = 1
                self.previous_last_update = clean_datetime(datetime(year, month, 1))
            else:
                self.previous_last_update = None
            
            self.load_dsd()
            
            for row, err in self._get_data_by_dimension():
                if row or err:
                    yield row, err
                else:
                    continue
            yield None, errors.InterruptBatchSeriesData()
                
    def _get_data_by_dimension(self):
        
        dimension_keys, dimensions = self._get_dimensions_from_dsd()
        
        position, _key, dimension_values = select_dimension(dimension_keys, dimensions, choice="max")
        
        count_dimensions = len(dimension_keys)
        
        for dimension_value in dimension_values:
            '''Pour chaque valeur de la dimension, generer une key d'url'''
            
            local_count = 0
                        
            sdmx_key = []
            for i in range(count_dimensions):
                if i == position:
                    sdmx_key.append(dimension_value)
                else:
                    sdmx_key.append(".")
            key = "".join(sdmx_key)

            url = "%s/%s" % (self._get_url_data(), key)
            json_data = self.download_json(url)
            
            if not json_data:
                logger.warning("no data for dataset[%s] - url[%s]" % (self.dataset_code, url))
                continue

            if not "Series" in json_data["CompactData"]["DataSet"]:
                logger.warning("no series for url[%s]" % url)
                continue

            for row, err in self._json_data_process(json_data):
                if row and len(row["values"]) == 0:
                    #TODO: log
                    continue
                yield row, err
                local_count += 1

            if local_count >= 2999:
                logger.warning('TODO: VRFY - series greater of 2999 for provider[IMF] - dataset[%s] - key[%s]' % (self.dataset_code, url))
            
        logger.info("retries count[%s] for dataset[%s]" % (self.retry_count, self.dataset_code))
        
        yield None, None
        
    def _get_list(self, values):
        if isinstance(values, list):
            return values
        elif isinstance(values, dict):
            return [values]
        else:
            raise TypeError("not list or dict type [%s]" % type(values))
        
    def _json_data_process(self, json_data):

        series_list = self._get_list(json_data["CompactData"]["DataSet"]["Series"])

        for series in series_list:
            
            if not "Obs" in series:
                continue
            
            bson = {
                'provider_name': self.provider_name,
                'dataset_code': self.dataset_code,
                'name': None,
                'key': None,
                'values': [],
                'attributes': None,
                'dimensions': {},
                'last_update': self.dataset.last_update,
                'start_date': None,
                'end_date': None,
                'frequency': None
            }

            tmp_attribute_keys = ["@%s" % key for key in self.dataset.attribute_keys]
            
            obs_list = self._get_list(series["Obs"])
            
            for obs in obs_list:
                if not "@OBS_VALUE" in obs:
                    continue
                item = {
                    "period": obs["@TIME_PERIOD"],
                    "value": obs["@OBS_VALUE"],
                    "attributes": None,
                }
                for key in tmp_attribute_keys:
                    if key in obs:
                        _key = obs[key][1:]
                        if not item["attributes"]:
                            item["attributes"] = {}
                        item["attributes"][_key] = obs[key]
                bson["values"].append(item)
                    
            if len(bson["values"]) == 0:
                #TODO: log
                return 
            
            for dim in self.dataset.dimension_keys:
                key = "@%s" % dim
                bson["dimensions"][dim] = series[key]
            
            if self.dataset.attribute_keys:
                bson["attributes"] = {}
                for attr in self.dataset.attribute_keys:
                    key = "@%s" % attr
                    if key in series:
                        bson["attributes"][attr] = series[key]
                    
            bson["key"] = self.dataset_code + "." + ".".join([bson["dimensions"][key] for key in self.dataset.dimension_keys])
            bson["name"] = " - ".join([self.dataset.codelists[key][bson["dimensions"][key]] for key in self.dataset.dimension_keys])
            
            bson["frequency"] = series["@FREQ"] #series["@%s" % self.frequency_field],
            
            bson["start_date"] = get_ordinal_from_period(bson["values"][0]["period"], freq=bson["frequency"])
            bson["end_date"] = get_ordinal_from_period(bson["values"][-1]["period"], freq=bson["frequency"])
            
            """
            Récupérable dans Annotation du codelist pour l'indicator:
                Topic: External Sector
                Alternate Publication Codes: eLibrary Concept: TX_R, WEO publication: TX_RPCH
                Source Code - Collection: TX_RPCH|PCOPP
                APDREO Name: Volume of total exports of goods and services, US Dollars, percent change
                APDREO Code: TX_RPCH.A
            """
            
            #pprint(bson)
                
            yield bson, None
        
    def build_series(self, bson):
        #bson["last_update"] = self.dataset.last_update
        self.dataset.add_frequency(bson["frequency"])
        return bson
        
        
class WeoData(SeriesIterator):
    
    def __init__(self, dataset):
        super().__init__(dataset)
        
        self.store_path = self.get_store_path()
        self.urls = self.weo_urls()
        
        self.release_date = None

        self.frequency = 'A'
        self.dataset.add_frequency(self.frequency)

        #WEO Country Code    ISO    WEO Subject Code    Country    Subject Descriptor    Subject Notes    Units    Scale    Country/Series-specific Notes
        self.dataset.dimension_keys = ['WEO Subject Code', 'ISO', 'Units']
        self.dataset.attribute_keys = ['WEO Country Code', 'Scale', 'flag']
        concepts = ['ISO', 'WEO Country Code', 'Scale', 'WEO Subject Code', 'Units', 'flag']
        self.dataset.concepts = dict(zip(concepts, concepts))

        #self.attribute_list.update_entry('flag', 'e', 'Estimates Start After')
        self.dataset.codelists["flag"] = {"e": 'Estimates Start After'}
        self.dataset.codelists['WEO Subject Code'] = {}
        self.dataset.codelists['ISO'] = {}
        self.dataset.codelists['Units'] = {}
        self.dataset.codelists['WEO Country Code'] = {}
        self.dataset.codelists['Scale'] = {}
        
        self.rows = self._process()

    def weo_urls(self):
        download = Downloader(url='http://www.imf.org/external/ns/cs.aspx?id=28',
                              filename="weo.html",
                              store_filepath=self.store_path)
        
        filepath = download.get_filepath()
        with open(filepath, 'rb') as fp:
            webpage = fp.read()
        
        self.fetcher.for_delete.append(filepath)
            
        #TODO: replace by beautifoulsoup ?
        html = etree.HTML(webpage)
        hrefs = html.xpath("//div[@id = 'content-main']/h4/a['href']")
        links = [href.values() for href in hrefs]
        
        #The last links of the WEO webpage lead to data we dont want to pull.
        links = links[:-16]
        #These are other links we don't want.
        links.pop(-8)
        links.pop(-10)
        links = [link[0][:-10]+'download.aspx' for link in links]

        output = []
    
        for link in links:
            webpage = requests.get(link)
            html = etree.HTML(webpage.text)
            final_link = html.xpath("//div[@id = 'content']//table//a['href']")
            output.append(link[:-13]+final_link[0].values()[0])
            
        # we need to handle the issue in chronological order
        return sorted(output)
            
    def _process(self):        
        for url in self.urls:
            
            #ex: http://www.imf.org/external/pubs/ft/weo/2006/02/data/WEOSep2006all.xls]
            date_str = match(".*WEO(\w{7})", url).groups()[0] #Sep2006
            self.release_date = datetime.strptime(date_str, "%b%Y") #2006-09-01 00:00:00
            
            if not self._is_updated():
                msg = "upsert dataset[%s] bypass because is updated from release_date[%s]"
                logger.info(msg % (self.dataset_code, self.release_date))
                continue

            self.dataset.last_update = self.release_date        
                
            logger.info("load url[%s]" % url)
            
            download = Downloader(url=url,
                                  store_filepath=self.store_path, 
                                  filename=os.path.basename(url),
                                  use_existing_file=self.fetcher.use_existing_file)        
            
            data_filepath = download.get_filepath()
            self.fetcher.for_delete.append(data_filepath)
            
            with open(data_filepath, encoding='latin-1') as fp:
                
                self.sheet = csv.DictReader(fp, dialect=csv.excel_tab)
                self.years = self.sheet.fieldnames[9:-1]
                self.start_date = get_ordinal_from_period(self.years[0], 
                                                          freq=self.frequency)
                self.end_date = get_ordinal_from_period(self.years[-1], 
                                                        freq=self.frequency)
                
                for row in self.sheet:
                    if not row or not row.get('Country'):
                        break
                    yield row, None

        yield None, None
        
    def _is_updated(self):

        if not self.dataset.last_update:
            return True
        
        if self.release_date > self.dataset.last_update:            
            return True

        return False
        
    def build_series(self, row):
        
        dimensions = {}
        attributes = {}
        
        #'WEO Subject Code': (BCA, Current account balance)
        weo_subject_code = row['WEO Subject Code']

        dimensions['WEO Subject Code'] = weo_subject_code
        if not weo_subject_code in self.dataset.codelists['WEO Subject Code']:
            self.dataset.codelists['WEO Subject Code'][weo_subject_code] = "%s (%s)" % (row['Subject Descriptor'], 
                                                                                        row['Units'])
                                                                          
        #'ISO': (DEU, Germany)
        dimensions['ISO'] = self.dimension_list.update_entry('ISO', 
                                                             row['ISO'], 
                                                             row['Country'])

        if not dimensions['ISO'] in self.dataset.codelists['ISO']:
            self.dataset.codelists['ISO'][dimensions['ISO']] = row['Country']

        #'Units': (2, U.S. dollars)
        dimensions['Units'] = self.dimension_list.update_entry('Units', 
                                                               '', 
                                                               row['Units'])

        if not dimensions['Units'] in self.dataset.codelists['Units']:
            self.dataset.codelists['Units'][dimensions['Units']] = row['Units']

        #'WEO Country Code': (134, Germany)    
        attributes['WEO Country Code'] = self.attribute_list.update_entry('WEO Country Code', 
                                                             row['WEO Country Code'], 
                                                             row['Country'])

        if not attributes['WEO Country Code'] in self.dataset.codelists['WEO Country Code']:
            self.dataset.codelists['WEO Country Code'][attributes['WEO Country Code']] = row['Country']

        
        if row['Scale']:
            attributes['Scale'] = self.attribute_list.update_entry('Scale', 
                                                                   '', #row['Scale'], 
                                                                   row['Scale'])
    
            if not attributes['Scale'] in self.dataset.codelists['Scale']:
                self.dataset.codelists['Scale'][attributes['Scale']] = row['Scale']

        #'BCA.DEU.2'
        # TODO: <Series FREQ="A" WEO Country Code="122" INDICATOR="AIP_IX" SCALE="0" SERIESCODE="122AIP_IX.A" BASE_YEAR="2010" TIME_FORMAT="P1Y" xmlns="http://dataservices.imf.org/compact/IFS">
        series_key = "%s.%s.%s" % (weo_subject_code,
                                   dimensions['ISO'],
                                   dimensions['Units'])
        
        #'Current account balance - Germany - U.S. dollars',
        series_name = "%s - %s - %s" % (row['Subject Descriptor'], 
                                        row['Country'],
                                        row['Units'])


        values = []
        estimation_start = None

        if row['Estimates Start After']:
            estimation_start = int(row['Estimates Start After'])
            
        for period in self.years:
            value = {
                'attributes': None,
                'period': period,
                'value': row[period].replace(',' ,'')
            }
            if estimation_start:
                if int(period) >= estimation_start:
                    value["attributes"] = {'flag': 'e'}
            
            values.append(value)
    
        bson = {
            'provider_name': self.dataset.provider_name,
            'dataset_code': self.dataset.dataset_code,
            'name': series_name,
            'key': series_key,
            'values': values,
            'attributes': attributes,
            'dimensions': dimensions,
            'last_update': self.release_date,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'frequency': self.frequency
        }
            
        notes = []
        
        if row['Subject Notes']:
            notes.append(row['Subject Notes'])
        
        if row['Country/Series-specific Notes']:
            notes.append(row['Country/Series-specific Notes'])
            
        if notes:
            bson["notes"] = "\n".join(notes)

        return bson


class WeoGroupsData(SeriesIterator):
    
    def __init__(self, dataset):
        super().__init__(dataset)
        
        self.store_path = self.get_store_path()
        self.urls = self.weo_urls()
        
        self.release_date = None

        self.frequency = 'A'
        self.dataset.add_frequency(self.frequency)
        self.dataset.dimension_keys = ['WEO Subject Code', 'WEO Country Group Code', 'Units']
        self.dataset.attribute_keys = ['Scale', 'flag']
        concepts = ['WEO Country Group Code', 'Scale', 'WEO Subject Code', 'Units', 'flag']
        self.dataset.concepts = dict(zip(concepts, concepts))

        #self.attribute_list.update_entry('flag', 'e', 'Estimates Start After')
        self.dataset.codelists["flag"] = {"e": 'Estimates Start After'}
        self.dataset.codelists['WEO Subject Code'] = {}
        self.dataset.codelists['Units'] = {}
        self.dataset.codelists['WEO Country Group Code'] = {}
        self.dataset.codelists['Scale'] = {}
        
        self.rows = self._process()

    def weo_urls(self):
        download = Downloader(url='http://www.imf.org/external/ns/cs.aspx?id=28',
                              filename="weo.html",
                              store_filepath=self.store_path)
        
        filepath = download.get_filepath()
        with open(filepath, 'rb') as fp:
            webpage = fp.read()
        
        self.fetcher.for_delete.append(filepath)
            
        #TODO: replace by beautifoulsoup ?
        html = etree.HTML(webpage)
        hrefs = html.xpath("//div[@id = 'content-main']/h4/a['href']")
        links = [href.values() for href in hrefs]
        
        #The last links of the WEO webpage lead to data we dont want to pull.
        links = links[:-16]
        #These are other links we don't want.
        links.pop(-8)
        links.pop(-10)
        links = [link[0][:-10]+'download.aspx' for link in links]

        output = []
    
        for link in links:
            webpage = requests.get(link)
            html = etree.HTML(webpage.text)
            final_link = html.xpath("//div[@id = 'content']//table//a['href']")
            output.append(link[:-13]+final_link[1].values()[0])
    
        # we need to handle the issue in chronological order
        return sorted(output)
            
    def _process(self):        
        for url in self.urls:
            
            #TODO: if not url.endswith("alla.xls"):
            
            #ex: http://www.imf.org/external/pubs/ft/weo/2006/02/data/WEOSep2006all.xls]
            date_str = match(".*WEO(\w{7})", url).groups()[0] #Sep2006
            self.release_date = datetime.strptime(date_str, "%b%Y") #2006-09-01 00:00:00
            
            if not self._is_updated():
                msg = "upsert dataset[%s] bypass because is updated from release_date[%s]"
                logger.info(msg % (self.dataset_code, self.release_date))
                continue

            self.dataset.last_update = self.release_date        
                
            logger.info("load url[%s]" % url)
            
            download = Downloader(url=url,
                                  store_filepath=self.store_path, 
                                  filename=os.path.basename(url),
                                  use_existing_file=self.fetcher.use_existing_file)        
            
            data_filepath = download.get_filepath()
            self.fetcher.for_delete.append(data_filepath)
            
            with open(data_filepath, encoding='latin-1') as fp:
                
                self.sheet = csv.DictReader(fp, dialect=csv.excel_tab)
                self.years = self.sheet.fieldnames[8:-1]
                self.start_date = get_ordinal_from_period(self.years[0], 
                                                          freq=self.frequency)
                self.end_date = get_ordinal_from_period(self.years[-1], 
                                                        freq=self.frequency)
                
                for row in self.sheet:
                    if not row or not row.get('Country Group Name'):
                        break
                    yield row, None

        yield None, None
        
    def _is_updated(self):

        if not self.dataset.last_update:
            return True
        
        if self.release_date > self.dataset.last_update:            
            return True

        return False
        
    def build_series(self, row):

        dimensions = {}
        attributes = {}
        
        #'WEO Subject Code': (BCA, Current account balance)
        weo_subject_code = row['WEO Subject Code']
        country = row['Country Group Name']

        dimensions['WEO Subject Code'] = weo_subject_code
        if not weo_subject_code in self.dataset.codelists['WEO Subject Code']:
            self.dataset.codelists['WEO Subject Code'][weo_subject_code] = "%s (%s)" % (row['Subject Descriptor'], 
                                                                                        row['Units'])
                                                                          
        #'ISO': (DEU, Germany)
        dimensions['WEO Country Group Code'] = row['WEO Country Group Code']

        if not dimensions['WEO Country Group Code'] in self.dataset.codelists['WEO Country Group Code']:
            self.dataset.codelists['WEO Country Group Code'][dimensions['WEO Country Group Code']] = country

        #'Units': (2, U.S. dollars)
        dimensions['Units'] = self.dimension_list.update_entry('Units', 
                                                               '', 
                                                               row['Units'])

        if not dimensions['Units'] in self.dataset.codelists['Units']:
            self.dataset.codelists['Units'][dimensions['Units']] = row['Units']

        if row['Scale']:
            attributes['Scale'] = self.attribute_list.update_entry('Scale', 
                                                                   '', 
                                                                   row['Scale'])
    
            if not attributes['Scale'] in self.dataset.codelists['Scale']:
                self.dataset.codelists['Scale'][attributes['Scale']] = row['Scale']

        series_key = "%s.%s.%s" % (weo_subject_code,
                                   dimensions['WEO Country Group Code'],
                                   dimensions['Units'])
        
        series_name = "%s - %s - %s" % (row['Subject Descriptor'], 
                                        country,
                                        row['Units'])


        values = []
        estimation_start = None

        if row['Estimates Start After']:
            estimation_start = int(row['Estimates Start After'])
            
        for period in self.years:
            value = {
                'attributes': None,
                'period': period,
                'value': row[period].replace(',' ,'') if row[period] else ''
            }
            if estimation_start:
                if int(period) >= estimation_start:
                    value["attributes"] = {'flag': 'e'}
            
            values.append(value)
    
        bson = {
            'provider_name': self.dataset.provider_name,
            'dataset_code': self.dataset.dataset_code,
            'name': series_name,
            'key': series_key,
            'values': values,
            'attributes': attributes,
            'dimensions': dimensions,
            'last_update': self.release_date,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'frequency': self.frequency
        }
            
        notes = []
        
        if row['Subject Notes']:
            notes.append(row['Subject Notes'])
        
        if row['Series-specific Notes']:
            notes.append(row['Series-specific Notes'])
            
        if notes:
            bson["notes"] = "\n".join(notes)

        return bson


DATASETS_KLASS = {
    "WEO": WeoData,
    "WEO-GROUPS": WeoGroupsData,
    "JSON": IMF_JSON_Data
}
        
