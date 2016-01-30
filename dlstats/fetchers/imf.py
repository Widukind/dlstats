# -*- coding: utf-8 -*-

import csv
from datetime import datetime
from re import match
import time
import logging

import requests
from lxml import etree

from dlstats.utils import Downloader
from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats import constants
from dlstats import errors

VERSION = 1

logger = logging.getLogger(__name__)

def weo_urls():
    download = Downloader(url='http://www.imf.org/external/ns/cs.aspx?id=28',
                          filename="weo.html")
    
    with open(download.get_filepath(), 'rb') as fp:
        webpage = fp.read()
        
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
        final_link = final_link[0].values()
        output.append(link[:-13]+final_link[0])

    # we need to handle the issue in chronological order
    return sorted(output)
    
DATASETS = {
    'WEO': { 
        'name': 'World Economic Outlook',
        'doc_href': 'http://www.imf.org/external/ns/cs.aspx?id=28',
    },
}

class IMF(Fetcher):

    def __init__(self, **kwargs):        
        super().__init__(provider_name='IMF', **kwargs)

        if not self.provider:
            self.provider = Providers(name=self.provider_name, 
                                      long_name="International Monetary Fund",
                                      version=VERSION, 
                                      region='world', 
                                      website='http://www.imf.org/', 
                                      fetcher=self)

        if self.provider.version != VERSION:
            self.provider.update_database()
            
    def build_data_tree(self, force_update=False):
        
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

    def load_datasets_first(self):        
        start = time.time()
        logger.info("first load fetcher[%s] - START" % (self.provider_name))
        
        for dataset in self.datasets_list():
            dataset_code = dataset["dataset_code"]
            try:
                self.upsert_dataset(dataset_code)
            except Exception as err:
                if isinstance(err, errors.MaxErrors):
                    raise
                logger.fatal("error for dataset[%s]: %s" % (dataset_code, str(err)))
        
        end = time.time() - start
        logger.info("first load fetcher[%s] - END - time[%.3f seconds]" % (self.provider_name, end))

    def load_datasets_update(self):
        #TODO: use download_last
        self.load_datasets_first()
        
    def upsert_dataset(self, dataset_code):
        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        
        settings = DATASETS[dataset_code]
        
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code, 
                           name=settings['name'], 
                           doc_href=settings['doc_href'],
                           fetcher=self)

        fetcher_data = DATASETS_KLASS[dataset_code](dataset, fetcher=self)
        dataset.series.data_iterator = fetcher_data
        result = dataset.update_database()

        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))
        return result
        
        
class WeoData(SeriesIterator):
    
    def __init__(self, dataset, fetcher):
        super().__init__()
        
        self.dataset = dataset
        self.fetcher = fetcher
        self.provider_name = dataset.provider_name
        self.dataset_code = dataset.dataset_code
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        
        self.urls = weo_urls()
        
        self.release_date = None

        self.frequency = 'A'

        self.dataset.dimension_keys = ['WEO Subject Code', 'ISO', 'WEO Country Code', 'Units']
        self.dataset.attribute_keys = ['Scale', 'flag']
        concepts = ['WEO Subject Code', 'ISO', 'WEO Country Code', 'Units', 'Scale', 'flag']
        self.dataset.concepts = dict(zip(concepts, concepts))

        self.attribute_list.update_entry('flag', 'e', 'Estimates Start After')        
        
        self.rows = self._process()
        
    def _process(self):        
        for url in self.urls:

            #ex: http://www.imf.org/external/pubs/ft/weo/2006/02/data/WEOSep2006all.xls]
            date_str = match(".*WEO(\w{7})", url).groups()[0] #Sep2006
            self.release_date = datetime.strptime(date_str, "%b%Y") #2006-09-01 00:00:00
            self.dataset.last_update = self.release_date        
            
            #import os
            #print("http \"%s\" > imf/%s" % (url, os.path.basename(url)))
            #continue
            
            if not self.is_updated():
                msg = "upsert dataset[%s] bypass because is updated from release_date[%s]"
                logger.info(msg % (self.dataset_code, self.release_date))
                continue
            
            download = Downloader(url=url, filename="weo-data.csv")        
            data_filepath = download.get_filepath()
            self.dataset.for_delete.append(data_filepath)
            
            with open(data_filepath, encoding='latin-1') as fp:
                
                self.sheet = csv.DictReader(fp, dialect=csv.excel_tab)
                self.years = self.sheet.fieldnames[9:-1]
                self.start_date = self.fetcher.get_ordinal_from_period(self.years[0], 
                                                                       freq=self.frequency)
                self.end_date = self.fetcher.get_ordinal_from_period(self.years[-1], 
                                                                     freq=self.frequency)
                
                for row in self.sheet:
                    yield row, None
        
            for k, dimensions in self.dimension_list.get_dict().items():
                self.dataset.codelists[k] = dimensions

            for k, attributes in self.attribute_list.get_dict().items():
                self.dataset.codelists[k] = attributes
                
    def is_updated(self):
        
        query = {'provider_name': self.dataset.provider_name, 
                 "dataset_code": self.dataset.dataset_code}
        dataset_doc = self.dataset.fetcher.db[constants.COL_DATASETS].find_one(query)
        
        if not dataset_doc:
            return True

        if self.release_date > dataset_doc['last_update']:
            return True

        return False
        
    def build_series(self, row):
        if not row.get('Country'):
            return
        
        dimensions = {}
        attributes = {}

        
        """
        'dimensions': {
            'WEO Subject Code': 'BCA',
            'ISO': 'DEU',
            'Units': '2',
            'WEO Country Code': '134'
        }
        'attributes': {
            'Scale': 'Billions',
        }
        """

        """
        # dimensions keys                                        
        WEO Country Code                    134
        ISO                                 DEU
        WEO Subject Code                    BCA
        Units                               U.S. dollars (2)

        # dimensions text
        Country                             Germany
        Subject Descriptor                  Current account balance

        # series attributes:
        Scale                               Billions
        
        # observation attributes:
        Estimates Start After               e (2005)
        
        # Notes
        Subject Notes                       "Balance of payments data are based upon the methodology of the 5th edition of the International Monetary Fund's Balance of Payments Manual (1993). Data for the world total reflects errors, omissions, and asymmetries in balance of payments statistics on current account, as well as the exclusion of data for international organizations and a limited number of countries. Calculated as the sum of the balance of individual countries."
        Country/Series-specific Notes       Definition: Balance on current transaction excluding exceptional financing Source: Central Bank Latest actual data: 2005 Notes: Data until 1990 refers to German federation only (West Germany). Data from 1991 refer to United Germany. Primary domestic currency:  Euros Data last updated: 08/2006
        """                                        

        #'WEO Subject Code': (BCA, Current account balance)
        dimensions['WEO Subject Code'] = self.dimension_list.update_entry('WEO Subject Code', 
                                                                          row['WEO Subject Code'], 
                                                                          row['Subject Descriptor'])
                                                                          
        #'ISO': (DEU, Germany)
        dimensions['ISO'] = self.dimension_list.update_entry('ISO', 
                                                             row['ISO'], 
                                                             row['Country'])

        #'WEO Country Code': (134, Germany)    
        dimensions['WEO Country Code'] = self.dimension_list.update_entry('WEO Country Code', 
                                                             row['WEO Country Code'], 
                                                             row['Country'])
        #'Units': (2, U.S. dollars)
        dimensions['Units'] = self.dimension_list.update_entry('Units', 
                                                               '', 
                                                               row['Units'])

        attributes['Scale'] = self.attribute_list.update_entry('Scale', 
                                                               '', #row['Scale'], 
                                                               row['Scale'])


        #'BCA.DEU.2'
        series_key = "%s.%s.%s" % (dimensions['WEO Subject Code'],
                                   dimensions['ISO'],
                                   dimensions['Units'])

        #'Current account balance - Germany - U.S. dollars',
        series_name = "%s - %s - %s" % (row['Subject Descriptor'], 
                                        row['Country'],
                                        row['Units'])


        """        
        dimensions['Country'] = self.dimension_list.update_entry('Country', 
                                                                 row['ISO'], 
                                                                 row['Country'])

        dimensions['WEO Country Code'] = self.dimension_list.update_entry('WEO Country Code', 
                                                                          row['WEO Country Code'], 
                                                                          row['Country'])
        
        dimensions['Subject'] = self.dimension_list.update_entry('Subject', 
                                                                 row['WEO Subject Code'], 
                                                                 row['Subject Descriptor'])
        
        dimensions['Units'] = self.dimension_list.update_entry('Units', 
                                                               '', 
                                                               row['Units'])
        
        dimensions['Scale'] = self.dimension_list.update_entry('Scale', 
                                                               row['Scale'], 
                                                               row['Scale'])
        
        series_name = "%s - %s - %s" % (row['Subject Descriptor'],
                                        row['Country'],
                                        row['Units'])

        series_key = "%s.%s.%s" % (row['WEO Subject Code'],
                                   row['ISO'],
                                   dimensions['Units'])
        """
        
        values = []
        estimation_start = None

        if row['Estimates Start After']:
            estimation_start = int(row['Estimates Start After'])
            
        for period in self.years:
            value = {
                'attributes': None,
                'release_date': self.release_date,
                'ordinal': self.fetcher.get_ordinal_from_period(period, freq=self.frequency),
                'period_o': period,
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
}
        
