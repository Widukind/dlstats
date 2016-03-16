# -*- coding: utf-8 -*-

import os
import csv
from datetime import datetime
from re import match
import logging

import requests
from lxml import etree

from dlstats.utils import Downloader, get_ordinal_from_period, clean_datetime, clean_key, clean_dict
from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats import constants
from dlstats.xml_utils import (XMLStructure_2_0 as XMLStructure, 
                               XMLCompactData_2_0_IMF as XMLData,
                               dataset_converter,
                               select_dimension,
                               get_dimensions_from_dsd)

VERSION = 2

logger = logging.getLogger(__name__)

FREQUENCIES_SUPPORTED = ["A", "Q", "M"]
FREQUENCIES_REJECTED = []

DATASETS = {
    'WEO': { 
        'name': 'World Economic Outlook',
        'doc_href': 'http://www.imf.org/external/ns/cs.aspx?id=28',
    },
    'BOP': { 
        'name': 'Balance of Payments',
        'doc_href': 'http://data.imf.org/BOP',
    },
    'BOPAGG': {
        'name': 'Balance of Payments, World and Regional Aggregates',
        'doc_href': 'http://data.imf.org/BOPAGG',
    },
    'DOT': { 
        'name': 'Direction of Trade Statistics',
        'doc_href': 'http://data.imf.org/DOT',
    },                         
    'IFS': { 
        'name': 'International Financial Statistics',
        'doc_href': 'http://data.imf.org/IFS',
    },
    'COMMP': { 
        'name': 'Primary Commodity Prices',
        'doc_href': 'http://data.imf.org/COMMP',
    },
    'COMMPP': { 
        'name': 'Primary Commodity Prices Projections',
        'doc_href': 'http://data.imf.org/COMMPP',
    },
    'GFSR': { 
        'name': 'Government Finance Statistics, Revenue',
        'doc_href': 'http://data.imf.org/GFSR',
    },
    'GFSSSUC': { 
        'name': 'Government Finance Statistics, Statement of Sources and Uses of Cash',
        'doc_href': 'http://data.imf.org/GFSSSUC',
    },
    'GFSCOFOG': { 
        'name': 'Government Finance Statistics, Expenditure by Function of Government',
        'doc_href': 'http://data.imf.org/GFSCOFOG',
    },
    'GFSFALCS': { 
        'name': 'Government Finance Statistics, Financial Assets and Liabilities by Counterpart Sector',
        'doc_href': 'http://data.imf.org/GFSFALCS',
    },
    'GFSIBS': { 
        'name': 'Government Finance Statistics, Integrated Balance Sheet (Stock Positions and Flows in Assets and Liabilities)',
        'doc_href': 'http://data.imf.org/GFSIBS',
    },
    'GFSMAB': { 
        'name': 'Government Finance Statistics, Main Aggregates and Balances',
        'doc_href': 'http://data.imf.org/GFSMAB',
    },
    'GFSE': { 
        'name': 'Government Finance Statistics, Expense',
        'doc_href': 'http://data.imf.org/GFSE',
    },
    'FSI': { 
        'name': 'Financial Soundness Indicators',
        'doc_href': 'http://data.imf.org/FSI',
    },
    'RT': { 
        'name': 'International Reserves Template',
        'doc_href': 'http://data.imf.org/RT',
    },
    'FAS': { 
        'name': 'Financial Access Survey',
        'doc_href': 'http://data.imf.org/FAS',
    },
    'COFER': { 
        'name': 'Currency Composition of Official Foreign Exchange Reserves',
        'doc_href': 'http://data.imf.org/COFER',
    },
    'CDIS': { 
        'name': 'Coordinated Direct Investment Survey',
        'doc_href': 'http://data.imf.org/CDIS',
    },
    'CPIS': {                                    # frequency S (semi annual)
        'name': 'Coordinated Portfolio Investment Survey',
        'doc_href': 'http://data.imf.org/CPIS',
    },
    'WoRLD': { 
        'name': 'World Revenue Longitudinal Data',
        'doc_href': 'http://data.imf.org/WoRLD',
    },
    'MCDREO': { 
        'name': 'Middle East and Central Asia Regional Economic Outlook',
        'doc_href': 'http://data.imf.org/MCDREO',
    },
    'APDREO': { 
        'name': 'Asia and Pacific Regional Economic Outlook',
        'doc_href': 'http://data.imf.org/APDREO',
    },
    'AFRREO': { 
        'name': 'Sub-Saharan Africa Regional Economic Outlook',
        'doc_href': 'http://data.imf.org/AFRREO',
    },
    'WHDREO': {                                   # bug: KeyError: 'NGDP_FY'
        'name': 'Western Hemisphere Regional Economic Outlook',
        'doc_href': 'http://data.imf.org/WHDREO',
    },
    'WCED': {                                     # bug: KeyError: 'OP'
        'name': 'World Commodity Exporters',
        'doc_href': 'http://data.imf.org/WCED',
    },
    'CPI': {
        'name': 'Consumer Price Index',
        'doc_href': 'http://data.imf.org/CPI',
    },
    'COFR': {                                     # Erreur 500
        'name': 'Coverage of Fiscal Reporting',
        'doc_href': 'http://data.imf.org/COFR',
    },
    'ICSD': {                                     # bug: KeyError: 'IGOV'
        'name': 'Investment and Capital Stock',
        'doc_href': 'http://data.imf.org/ICSD',
    },
    'HPDD': {                                     # bug: KeyError: 'GGXWDG'
        'name': 'Historical Public Debt',
        'doc_href': 'http://data.imf.org/HPDD',
    },
    'PGI': { 
        'name': 'Principal Global Indicators',
        'doc_href': 'http://data.imf.org/PGI',
    },
}


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
    {
        "provider_name": "IMF",
        "category_code": "RT",
        "name": DATASETS["RT"]["name"],
        "position": 12,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "RT",
                "name": DATASETS["RT"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["RT"]["doc_href"]
                }
            },
        ],
        "metadata": {}
    },
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
            klass = DATASETS_KLASS["XML"]

        dataset.series.data_iterator = klass(dataset)
        
        return dataset.update_database()
        
        
class IMF_XML_Data(SeriesIterator):
    
    def __init__(self, dataset=None):
        super().__init__(dataset)

        self.dataset.last_update = clean_datetime()        
        self.store_path = self.get_store_path()
        self.xml_dsd = XMLStructure(provider_name=self.provider_name)        

        self._load_dsd()

        self.xml_data = XMLData(provider_name=self.provider_name,
                                dataset_code=self.dataset_code,
                                xml_dsd=self.xml_dsd,
                                frequencies_supported=FREQUENCIES_SUPPORTED)
        
        #self._load_data()
        self.rows = self._get_data_by_dimension()

    def _get_url_dsd(self):
        return "http://dataservices.imf.org/REST/SDMX_XML.svc/DataStructure/%s" % self.dataset_code 

    def _get_url_data(self):
        return "http://dataservices.imf.org/REST/SDMX_XML.svc/CompactData/%s" % self.dataset_code 
        
    def _load_dsd(self):
        url = self._get_url_dsd()
        download = Downloader(store_filepath=self.store_path,
                              url=url, 
                              filename="dsd-%s.xml" % self.dataset_code,
                              use_existing_file=self.fetcher.use_existing_file,
                              client=self.fetcher.requests_client)
        filepath = download.get_filepath()
        self.fetcher.for_delete.append(filepath)
        
        self.xml_dsd.process(filepath)
        self._set_dataset()

    def _set_dataset(self):

        dataset = dataset_converter(self.xml_dsd, self.dataset_code)
        self.dataset.dimension_keys = dataset["dimension_keys"] 
        self.dataset.attribute_keys = dataset["attribute_keys"] 
        self.dataset.concepts = dataset["concepts"] 
        self.dataset.codelists = dataset["codelists"]
        
    def _get_data_by_dimension(self):
        
        dimension_keys, dimensions = get_dimensions_from_dsd(self.xml_dsd,
                                                                       self.provider_name,
                                                                       self.dataset_code)
        
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
            filename = "data-%s-%s.xml" % (self.dataset_code, key.replace(".", "_"))
            download = Downloader(url=url, 
                                  filename=filename,
                                  store_filepath=self.store_path,
                                  client=self.fetcher.requests_client)            
            filepath, response = download.get_filepath_and_response()

            if filepath:
                self.fetcher.for_delete.append(filepath)
            
            if response.status_code >= 400 and response.status_code < 500:
                continue
            elif response.status_code >= 500:
                raise response.raise_for_status()
            
            for row, err in self.xml_data.process(filepath):
                yield row, err
                local_count += 1
                if local_count > 2500:
                    logger.warning("TODO: VRFY - series > 2500 for provider[IMF] - dataset[%s] - key[%s]" % (self.dataset_code, key))

            #self.dataset.update_database(save_only=True)
        
        yield None, None
        
    def build_series(self, bson):
        bson["last_update"] = self.dataset.last_update
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
        self.dataset.dimension_keys = ['WEO Subject Code', 'ISO', 'WEO Country Code', 'Units']
        self.dataset.attribute_keys = ['Scale', 'flag']
        concepts = ['ISO', 'WEO Country Code', 'Scale', 'WEO Subject Code', 'Units', 'flag']
        self.dataset.concepts = dict(zip(concepts, concepts))

        #self.attribute_list.update_entry('flag', 'e', 'Estimates Start After')
        self.dataset.codelists["flag"] = {"e": 'Estimates Start After'}
        
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
            #final_link = final_link[0].values()
            #['WEOOct2015all.xls']
            output.append(link[:-13]+final_link[0].values()[0])
            
            #['WEOOct2015alla.xls']
            #TODO: output.append(link[:-13]+final_link[1].values()[0])
    
        # we need to handle the issue in chronological order
        return sorted(output)
            
    def _process(self):        
        for url in self.urls:
            
            #TODO: if not url.endswith("alla.xls"):
            
            #ex: http://www.imf.org/external/pubs/ft/weo/2006/02/data/WEOSep2006all.xls]
            date_str = match(".*WEO(\w{7})", url).groups()[0] #Sep2006
            self.release_date = datetime.strptime(date_str, "%b%Y") #2006-09-01 00:00:00
            
            if not self.is_updated():
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

            #self.dataset.update_database(save_only=True)
        
        yield None, None
        
    def is_updated(self):
        
        query = {'provider_name': self.dataset.provider_name, 
                 "dataset_code": self.dataset.dataset_code}
        dataset_doc = self.dataset.fetcher.db[constants.COL_DATASETS].find_one(query)
        
        if not dataset_doc:
            return True

        print("UPDATED : ? ", self.release_date, dataset_doc['last_update'], self.release_date > dataset_doc['last_update'])

        if self.release_date > dataset_doc['last_update']:            
            return True

        return False
        
    def build_series(self, row):
        
        dimensions = {}
        attributes = {}
        
        #'WEO Subject Code': (BCA, Current account balance)
        dimensions['WEO Subject Code'] = self.dimension_list.update_entry('WEO Subject Code', 
                                                                row['WEO Subject Code'], 
                                                                row['Subject Descriptor'])
        if not 'WEO Subject Code' in self.dataset.codelists:
            self.dataset.codelists['WEO Subject Code'] = {}

        if not dimensions['WEO Subject Code'] in self.dataset.codelists['WEO Subject Code']:
            self.dataset.codelists['WEO Subject Code'][dimensions['WEO Subject Code']] = row['Subject Descriptor']
                                                                          
        #'ISO': (DEU, Germany)
        dimensions['ISO'] = self.dimension_list.update_entry('ISO', 
                                                             row['ISO'], 
                                                             row['Country'])

        if not 'ISO' in self.dataset.codelists:
            self.dataset.codelists['ISO'] = {}

        if not dimensions['ISO'] in self.dataset.codelists['ISO']:
            self.dataset.codelists['ISO'][dimensions['ISO']] = row['Country']

        #'WEO Country Code': (134, Germany)    
        dimensions['WEO Country Code'] = self.dimension_list.update_entry('WEO Country Code', 
                                                             row['WEO Country Code'], 
                                                             row['Country'])

        if not 'WEO Country Code' in self.dataset.codelists:
            self.dataset.codelists['WEO Country Code'] = {}

        if not dimensions['WEO Country Code'] in self.dataset.codelists['WEO Country Code']:
            self.dataset.codelists['WEO Country Code'][dimensions['WEO Country Code']] = row['Country']

        #'Units': (2, U.S. dollars)
        dimensions['Units'] = self.dimension_list.update_entry('Units', 
                                                               '', 
                                                               row['Units'])

        if not 'Units' in self.dataset.codelists:
            self.dataset.codelists['Units'] = {}

        if not dimensions['Units'] in self.dataset.codelists['Units']:
            self.dataset.codelists['Units'][dimensions['Units']] = row['Units']

        attributes['Scale'] = self.attribute_list.update_entry('Scale', 
                                                               '', #row['Scale'], 
                                                               row['Scale'])

        if not 'Scale' in self.dataset.codelists:
            self.dataset.codelists['Scale'] = {}

        if not attributes['Scale'] in self.dataset.codelists['Scale']:
            self.dataset.codelists['Scale'][attributes['Scale']] = row['Scale']


        #'BCA.DEU.2'
        # TODO: <Series FREQ="A" WEO Country Code="122" INDICATOR="AIP_IX" SCALE="0" SERIESCODE="122AIP_IX.A" BASE_YEAR="2010" TIME_FORMAT="P1Y" xmlns="http://dataservices.imf.org/compact/IFS">
        series_key = "%s.%s.%s" % (dimensions['WEO Subject Code'],
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
                'release_date': self.release_date,
                'ordinal': get_ordinal_from_period(period, freq=self.frequency),
                'period': period,
                'value': row[period]
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


DATASETS_KLASS = {
    "WEO": WeoData,
    "XML": IMF_XML_Data
}
        
