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
        'name': 'Balance of Payments Statistics (BOPS)',
        'doc_href': 'http://data.imf.org/BOP',
    },
    'BOPAGG': { 
        'name': 'BOPAGG',
        'doc_href': 'http://data.imf.org/BOPAGG',
    },
    'DOT': { 
        'name': 'Direction of Trade Statistics (DOTS)',
        'doc_href': 'http://data.imf.org/DOT',
    },                         
    'IFS': { 
        'name': 'International Financial Statistics (IFS)',
        'doc_href': 'http://data.imf.org/IFS',
    },
    'COMMP': { 
        'name': 'COMMP',
        'doc_href': 'http://data.imf.org/COMMP',
    },
    'COMMPP': { 
        'name': 'COMMPP',
        'doc_href': 'http://data.imf.org/COMMPP',
    },
    'GFSR': { 
        'name': 'GFSR',
        'doc_href': 'http://data.imf.org/GFSR',
    },
    'GFSSSUC': { 
        'name': 'GFSSSUC',
        'doc_href': 'http://data.imf.org/GFSSSUC',
    },
    'GFSCOFOG': { 
        'name': 'GFSCOFOG',
        'doc_href': 'http://data.imf.org/GFSCOFOG',
    },
    'GFSFALCS': { 
        'name': 'GFSFALCS',
        'doc_href': 'http://data.imf.org/GFSFALCS',
    },
    'GFSIBS': { 
        'name': 'GFSIBS',
        'doc_href': 'http://data.imf.org/GFSIBS',
    },
    'GFSMAB': { 
        'name': 'GFSMAB',
        'doc_href': 'http://data.imf.org/GFSMAB',
    },
    'GFSE': { 
        'name': 'GFSE',
        'doc_href': 'http://data.imf.org/GFSE',
    },

    'FSI': { 
        'name': 'FSI',
        'doc_href': 'http://data.imf.org/FSI',
    },
    'RT': { 
        'name': 'RT',
        'doc_href': 'http://data.imf.org/RT',
    },
    'FAS': { 
        'name': 'FAS',
        'doc_href': 'http://data.imf.org/FAS',
    },
    'COFER': { 
        'name': 'COFER',
        'doc_href': 'http://data.imf.org/COFER',
    },
    'CDIS': { 
        'name': 'CDIS',
        'doc_href': 'http://data.imf.org/CDIS',
    },
    #'CPIS': {                                    # frequency S (semi annual)
    #    'name': 'CPIS',
    #    'doc_href': 'http://data.imf.org/CPIS',
    #},
    'WoRLD': { 
        'name': 'WoRLD',
        'doc_href': 'http://data.imf.org/WoRLD',
    },
    'MCDREO': { 
        'name': 'MCDREO',
        'doc_href': 'http://data.imf.org/MCDREO',
    },
    'APDREO': { 
        'name': 'APDREO',
        'doc_href': 'http://data.imf.org/APDREO',
    },
    'AFRREO': { 
        'name': 'AFRREO',
        'doc_href': 'http://data.imf.org/AFRREO',
    },
    #'WHDREO': {                                   # bug: KeyError: 'NGDP_FY'
    #    'name': 'WHDREO',
    #    'doc_href': 'http://data.imf.org/WHDREO',
    #},
    #'WCED': {                                     # bug: KeyError: 'OP'
    #    'name': 'WCED',
    #    'doc_href': 'http://data.imf.org/WCED',
    #},
    #'CPI': {                                      # bug OBS_STATUS=n.a
    #    'name': 'CPI',
    #    'doc_href': 'http://data.imf.org/CPI',
    #},
    #'COFR': {                                     # Erreur 500
    #    'name': 'COFR',
    #    'doc_href': 'http://data.imf.org/COFR',
    #},
    #'ICSD': {                                     # bug: KeyError: 'IGOV'
    #    'name': 'ICSD',
    #    'doc_href': 'http://data.imf.org/ICSD',
    #},
    #'HPDD': {                                     # bug: KeyError: 'GGXWDG'
    #    'name': 'HPDD',
    #    'doc_href': 'http://data.imf.org/HPDD',
    #},
    'PGI': { 
        'name': 'PGI',
        'doc_href': 'http://data.imf.org/PGI',
    },
}

"""
CATEGORIES = [
    {
        "category_code": "REO",
        "name": "Regional Economic Outlook",
        "position": 2,
        "doc_href": None,
        "datasets": [
            {
                "dataset_code": "G19-TERMS",
                "name": DATASETS["G19-TERMS"]["name"], 
                "last_update": None,                 
                "metadata": {
                    "doc_href": DATASETS["G19-TERMS"]["doc_href"]
                }
            },
        ]
    }
]
"""



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
        
        #return CATEGORIES
        
        categories = []
        
        for category_code, dataset in DATASETS.items():
            cat = {
                "provider_name": self.provider_name,
                "category_code": category_code,
                "name": dataset["name"],
                "doc_href": dataset["doc_href"],
                "datasets": [{
                    "name": dataset["name"],
                    "dataset_code": category_code,
                    "last_update": None, 
                    "metadata": None
                }],
                "metadata": {}
            }
            categories.append(cat)
        
        return categories

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
        
        if "OBS_STATUS" in self.dataset.codelists:
            self.dataset.codelists["OBS_STATUS"] = clean_dict(self.dataset.codelists["OBS_STATUS"])

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
            download = Downloader(url=url, 
                                  filename="data-%s-%s.xml" % (self.dataset_code, key),
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

            self.dataset.update_database(save_only=True)
        
        yield None, None
        
    def build_series(self, bson):
        bson["last_update"] = self.dataset.last_update
        self.dataset.add_frequency(bson["frequency"])
        
        for value in bson["values"]:
            if value.get("attributes") and "OBS_STATUS" in value.get("attributes"):
                value["attributes"]["OBS_STATUS"] = clean_key(value["attributes"]["OBS_STATUS"])
        
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

            self.dataset.update_database(save_only=True)
        
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
        
