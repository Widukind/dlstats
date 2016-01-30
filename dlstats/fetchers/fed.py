# -*- coding: utf-8 -*-

import os
from datetime import datetime
from collections import OrderedDict
import time
import logging
import zipfile

from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats.utils import Downloader
from dlstats import errors
from dlstats.xml_utils import (XMLStructure_1_0 as XMLStructure, 
                               XMLData_1_0_FED as XMLData,
                               dataset_converter_v2 as dataset_converter)
from dlstats.tests.resources.xml_samples import filepath

VERSION = 2

logger = logging.getLogger(__name__)

DATASETS = {
    'G19': {
        "name": "G.19 - Consumer Credit",
        "doc_href": 'http://www.federalreserve.gov/releases/G19/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G19&filetype=zip',
    },
    'G17':{
         "name": "G.17 - Industrial Production and Capacity Utilization",
         "doc_href": 'http://www.federalreserve.gov/releases/G17/Current/default.htm',
         'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&filetype=zip',
    },
    'H3':{
         "name": "H.3 - Aggregate Reserves of Depository Institution and the Monetary Base",
         "doc_href": 'http://www.federalreserve.gov/releases/H3/current/default.htm',
         'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H3&filetype=zip',
    },    
    'H8':{
         "name": "H.8 - Assets and Liabilities of Commercial Banks in the U.S.",
         "doc_href": 'http://www.federalreserve.gov/releases/H8/current/',
         'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H8&filetype=zip',
    },
    'E2':{
         "name": "E.2 - Survey of Terms of Business Lending",
         "doc_href": 'http://www.federalreserve.gov/releases/E2/Current/default.htm',
         'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=E2&filetype=zip',
    },
    'G20':{
         "name": "G.20 - Finance Companies",
         "doc_href": 'http://www.federalreserve.gov/releases/G20/current/',
         'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G20&filetype=zip',
    },
    'H10':{
         "name": "G.5 / H.10 - Foreign Exchange Rates",
         "doc_href": 'http://www.federalreserve.gov/releases/H10/current/default.htm',
         'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H10&filetype=zip',
    },
    'Z1': {
        "name": "Z.1 - Financial Accounts of the United States",
        "doc_href": 'http://www.federalreserve.gov/releases/Z1/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=Z1&filetype=zip',
    },    
    'H15':{
         "name": "H.15 - Selected Interest Rates",
         "doc_href": 'http://www.federalreserve.gov/releases/H15/current/default.htm',
         'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H15&filetype=zip',
    },
    'H41':{
         "name": "H.4.1 - Factors Affecting Reserve Balances",
         "doc_href": 'http://www.federalreserve.gov/releases/H41/Current/',
         'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H41&filetype=zip',
    },
    'H6':{
         "name": "H.6 - Money Stock Measures",
         "doc_href": 'http://www.federalreserve.gov/releases/H6/current/default.htm',
         'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H6&filetype=zip',
    },
    'SLOOS':{
         "name": "SLOOS - Senior Loan Officer Opinion Survey on Bank Lending Practices",
         "doc_href": 'http://www.federalreserve.gov/boarddocs/SnLoanSurvey/default.htm',
         'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=SLOOS&filetype=zip',
    },    
    'CP':{
         "name": "CP - Commercial Paper",
         "doc_href": 'http://www.federalreserve.gov/releases/CP/default.htm',
         'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=CP&filetype=zip',
    },    
    'PRATES':{
         "name": "PRATES - Policy Rates",
         "doc_href": 'http://www.federalreserve.gov/monetarypolicy/reqresbalances.htm',
         'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=PRATES&filetype=zip',
    },
    'FOR':{
         "name": "FOR - Household Debt Service and Financial Obligations Ratios",
         "doc_href": 'http://www.federalreserve.gov/releases/housedebt/default.htm',
         'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=FOR&filetype=zip',
    }, 
    'CHGDEL':{
         "name": "CHGDEL - Charge-off and Delinquency Rates",
         "doc_href": 'http://www.federalreserve.gov/releases/chargeoff/',
         'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=CHGDEL&filetype=zip',
    },           
}
CATEGORIES = [
    {
        "category_code": "PEI",
        "name": "Principal Economic Indicators",
        "position": 1,
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "G19",
                "name": DATASETS["G19"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["G19"]["doc_href"]
                }
            },
            {
                "dataset_code": "G17",
                "name": DATASETS["G17"]["name"], 
                "last_update": None,
                "metadata": {
                    "doc_href": DATASETS["G17"]["doc_href"], 
                }
            },
        ]
    },
    {
        "category_code": "BAL",
        "name": "Bank Assets & Liabilities",
        "position": 2,
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "H3",
                "name": DATASETS["H3"]["name"], 
                "last_update": None,
                "metadata": {
                    "doc_href": DATASETS["H3"]["doc_href"], 
                }
            },
            {
                "dataset_code": "H8",
                "name": DATASETS["H8"]["name"], 
                "last_update": None,
                "metadata": {
                    "doc_href": DATASETS["H8"]["doc_href"], 
                }
            },
            {
                "dataset_code": "CHGDEL",
                "name": DATASETS["CHGDEL"]["name"], 
                "last_update": None,
                "metadata": {
                    "doc_href": DATASETS["CHGDEL"]["doc_href"], 
                }
            },              
            {
                "dataset_code": "SLOOS",
                "name": DATASETS["SLOOS"]["name"], 
                "last_update": None,
                "metadata": {
                    "doc_href": DATASETS["SLOOS"]["doc_href"], 
                }
            },  
            {
                "dataset_code": "E2",
                "name": DATASETS["E2"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["E2"]["doc_href"], 
                }
            },          
        ]
    },
    {
        "category_code": "BF",
        "name": "Business Finance",
        "position": 3,
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "CP",
                "name": DATASETS["CP"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["CP"]["doc_href"], 
                }
            },
            {
                "dataset_code": "G20",
                "name": DATASETS["G20"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["G20"]["doc_href"], 
                }
            },            
        ]
    },
    {
        "category_code": "ERID",
        "name": "Exchange Rates and International Data",
        "position": 4,
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "H10",
                "name": DATASETS["H10"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["H10"]["doc_href"], 
                }
            },           
        ]
    },
    {
        "category_code": "FA",
        "name": "Financial Accounts",
        "position": 5,
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "Z1",
                "name": DATASETS["Z1"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["Z1"]["doc_href"], 
                }
            },
        ]
    },    
    {
        "category_code": "HF",
        "name": "Household Finance",
        "position": 6,
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "G19",
                "name": DATASETS["G19"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["G19"]["doc_href"], 
                }
            },  
            {
                "dataset_code": "G20",
                "name": DATASETS["G20"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["G20"]["doc_href"], 
                }
            }, 
            {
                "dataset_code": "FOR",
                "name": DATASETS["FOR"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["FOR"]["doc_href"], 
                }
            },             
        ]
    }, 
    {
        "category_code": "IA",
        "name": "Industrial Activity",
        "position": 7,
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "G17",
                "name": DATASETS["G17"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["G17"]["doc_href"], 
                }
            },           
        ]
    },   
    {
        "category_code": "IR",
        "name": "Interest Rates",
        "position": 8,
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "H15",
                "name": DATASETS["H15"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["H15"]["doc_href"], 
                }
            },  
            {
                "dataset_code": "PRATES",
                "name": DATASETS["PRATES"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["PRATES"]["doc_href"], 
                }
            },             
        ]
    }, 
    {
        "category_code": "MSRB",
        "name": "Money Stock and Reserve Balances",
        "position": 9,
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "H3",
                "name": DATASETS["H3"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["H3"]["doc_href"], 
                }
            },  
            {
                "dataset_code": "H41",
                "name": DATASETS["H41"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["H41"]["doc_href"], 
                }
            },   
            {
                "dataset_code": "H6",
                "name": DATASETS["H6"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["H6"]["doc_href"], 
                }
            },              
        ]
    }      

]

MONGO_DENIED_KEY_CHARS = ["."]

def clean_key(key):
    if not key:
        return key    
    #if not key in MONGO_DENIED_KEY_CHARS:
    #    return key
    for k in MONGO_DENIED_KEY_CHARS:
        key = key.replace(k, "_")
    return key

def clean_dict(dct):
    if not dct:
        return dct
    new_dct = dct.copy()
    for k, v in dct.items():
        new_dct.pop(k)
        key = clean_key(k)
        new_dct[key] = v
    return new_dct

def extract_zip_file(zipfilepath):
    zfile = zipfile.ZipFile(zipfilepath)
    filepaths = {}
    for filename in zfile.namelist():
        if filename.endswith("struct.xml"):
            key = "struct.xml"
        elif filename.endswith("data.xml"):
            key = "data.xml"
        else:
            key = filename

        filepath = zfile.extract(filename, os.path.dirname(zipfilepath))
        filepaths[key] = os.path.abspath(filepath)

    return filepaths

class FED(Fetcher):
    
    def __init__(self, **kwargs):        
        super().__init__(provider_name='FED', max_errors=10, **kwargs)
        
        if not self.provider:
            self.provider = Providers(name=self.provider_name,
                                      long_name='Federal Reserve',
                                      version=VERSION,
                                      region='US',
                                      website='http://www.federalreserve.gov',
                                      fetcher=self)

        if self.provider.version != VERSION:
            self.provider.update_database()
        
    def build_data_tree(self, force_update=False):
        
        return CATEGORIES
        
    def upsert_dataset(self, dataset_code):
        
        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        
        #TODO: control si existe ou update !!!

        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code,
                           name=DATASETS[dataset_code]['name'],
                           doc_href=DATASETS[dataset_code]['doc_href'],
                           last_update=datetime.now(),
                           fetcher=self)
        
        _data = FED_Data(dataset=dataset, 
                         url=DATASETS[dataset_code]['url'])
        dataset.series.data_iterator = _data
        result = dataset.update_database()
        
        _data = None

        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))
        
        return result

    def load_datasets_first(self):
        start = time.time()        
        logger.info("datasets first load. provider[%s] - START" % (self.provider_name))
        
        self.upsert_data_tree()

        for dataset in self.datasets_list():
            dataset_code = dataset["dataset_code"]
            try:
                self.upsert_dataset(dataset_code)
            except Exception as err:
                if isinstance(err, errors.MaxErrors):
                    raise
                logger.fatal("error for dataset[%s]: %s" % (dataset_code, str(err)))

        end = time.time() - start
        logger.info("datasets first load. provider[%s] - END - time[%.3f seconds]" % (self.provider_name, end))

    def load_datasets_update(self):
        #TODO: 
        self.load_datasets_first()

class FED_Data(SeriesIterator):
    
    def __init__(self, dataset=None, url=None):
        """
        :param Datasets dataset: Datasets instance
        """
        super().__init__()
        self.dataset = dataset
        self.url = url
        self.attribute_list = self.dataset.attribute_list
        self.dimension_list = self.dataset.dimension_list
        self.provider_name = self.dataset.provider_name
        self.dataset_code = self.dataset.dataset_code

        self.xml_dsd = XMLStructure(provider_name=self.provider_name) 
        
        self.rows = None
        
        self._load()
        
        
    def _load(self):

        download = Downloader(url=self.url, 
                              filename="data-%s.zip" % self.dataset_code)
        zip_filepath = download.get_filepath()
        self.dataset.for_delete.append(zip_filepath)
        
        filepaths = (extract_zip_file(zip_filepath))
        dsd_fp = filepaths['struct.xml']
        data_fp = filepaths['data.xml']

        for filepath in filepaths.values():
            self.dataset.for_delete.append(filepath)
        
        self.xml_dsd.process(dsd_fp)
        
        self._set_dataset()

        self.xml_data = XMLData(provider_name=self.provider_name,
                                dataset_code=self.dataset_code,
                                dimension_keys=self.xml_dsd.dimension_keys,
                                dimensions=self.xml_dsd.dimensions)
        
        self.rows = self.xml_data.process(data_fp)

    def _set_dataset(self):

        dimensions = OrderedDict()
        for key, item in self.xml_dsd.dimensions.items():
            dimensions[key] = item["enum"]
        self.dimension_list.set_dict(dimensions)
        
        attributes = OrderedDict()
        for key, item in self.xml_dsd.attributes.items():
            attributes[key] = item["enum"]
        self.attribute_list.set_dict(attributes)

        dataset = dataset_converter(self.xml_dsd, self.dataset_code)
        self.dataset.dimension_keys = dataset["dimension_keys"] 
        self.dataset.concepts = dataset["concepts"]
        self.dataset.attribute_keys = dataset["attribute_keys"] 

        units = dataset["codelists"].pop("UNIT", None)
        if units:
            new_units = clean_dict(units)
            dataset["codelists"]["UNIT"] = new_units
        
        units = dataset["codelists"].pop("UNIT_MULT", None)
        if units:
            new_units = clean_dict(units)
            dataset["codelists"]["UNIT_MULT"] = new_units
        
        self.dataset.codelists = dataset["codelists"] 
        
    def build_series(self, bson):
        bson["last_update"] = self.dataset.last_update

        attrs = bson.get('attributes', None)
        if attrs and ("UNIT" in attrs.keys() or "UNIT_MULT" in attrs.keys()):
            new_attributes = {}
            for k, v in attrs.items():
                new_attributes[k] = clean_key(v)
            bson["attributes"] = new_attributes
                            
        return bson
        

