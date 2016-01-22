# -*- coding: utf-8 -*-

import csv
import os
from datetime import datetime
from re import match
import time
import logging
import tempfile

import requests
from lxml import etree
import pandas

from dlstats.fetchers._commons import Fetcher, Datasets, Providers

VERSION = 1

logger = logging.getLogger(__name__)

DATASETS = {
    'WEO': { 
        'name': 'World Economic Outlook',
        'doc_href': 'http://www.imf.org/external/ns/cs.aspx?id=28',
    },
}

def download(url, filename=None):
    
    filename = filename or os.path.basename(url)
    
    storedir = tempfile.mkdtemp()
    if not os.path.exists(storedir):
        os.makedirs(storedir, exist_ok=True)
    
    filepath = os.path.abspath(os.path.join(storedir, filename))
    
    start = time.time()
    try:
        response = requests.get(url, 
                                #timeout=timeout, 
                                stream=True,
                                allow_redirects=True,
                                verify=False)

        if not response.ok:
            msg = "download url[%s] - status_code[%s] - reason[%s]" % (url, 
                                                                       response.status_code, 
                                                                       response.reason)
            logger.error(msg)
            raise Exception(msg)
        
        with open(filepath,'wb') as f:
            for chunk in response.iter_content():
                f.write(chunk)
            
    except requests.exceptions.ConnectionError as err:
        raise Exception("Connection Error")
    except requests.exceptions.ConnectTimeout as err:
        raise Exception("Connect Timeout")
    except requests.exceptions.ReadTimeout as err:
        raise Exception("Read Timeout")
    except Exception as err:
        raise Exception("Not captured exception : %s" % str(err))            

    end = time.time() - start
    logger.info("download file[%s] - END - time[%.3f seconds]" % (url, end))
    
    return filepath


class IMF(Fetcher):

    def __init__(self, db=None, **kwargs):        
        super().__init__(provider_name='IMF', db=db, **kwargs)
        
        self.provider = Providers(name=self.provider_name, 
                                  long_name="International Monetary Fund",
                                  version=VERSION, 
                                  region='world', 
                                  website='http://www.imf.org/', 
                                  fetcher=self)

    def upsert_all_datasets(self):
        start = time.time()
        logger.info("update fetcher[%s] - START" % (self.provider_name))
        
        for dataset_code in DATASETS.keys():
            self.upsert_dataset(dataset_code) 

        end = time.time() - start
        logger.info("update fetcher[%s] - END - time[%.3f seconds]" % (self.provider_name, end))
        
    def upsert_dataset(self, dataset_code):
        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        
        if dataset_code=='WEO':
            for u in self.weo_urls:
                self.upsert_weo_issue(u, dataset_code)
        else:
            raise Exception("This dataset is unknown" + dataset_code)
        
        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))

    def datasets_list(self):
        return DATASETS.keys()

    def datasets_long_list(self):
        return [(key, dataset['name']) for key, dataset in DATASETS.items()]

    @property
    def weo_urls(self):

        webpage = requests.get('http://www.imf.org/external/ns/cs.aspx?id=28')
        
        #TODO: replace by beautifoulsoup ?
        html = etree.HTML(webpage.text)
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
        return(sorted(output))
        
    def upsert_weo_issue(self, url, dataset_code):
        
        settings = DATASETS[dataset_code]
        
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code, 
                           name=settings['name'], 
                           doc_href=settings['doc_href'], 
                           fetcher=self)
        
        weo_data = WeoData(dataset, url)
        dataset.last_update = weo_data.release_date        
        dataset.attribute_list.update_entry('flags','e','Estimated')
        dataset.series.data_iterator = weo_data
        try:
            dataset.update_database()
            self.update_metas(dataset_code)
        except Exception as err:
            logger.error(str(err))

    def upsert_categories(self):
        data_tree = {'name': 'IMF',
                     'category_code': 'imf_root',
                     'children': [{'name': 'WEO' , 
                                   'category_code': 'WEO',
                                   'exposed': True,
                                   'children': []}]}
        self.provider.add_data_tree(data_tree)
        
class WeoData():
    
    def __init__(self, dataset, url):
        self.dataset = dataset
        self.provider_name = dataset.provider_name
        self.dataset_code = dataset.dataset_code
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        
        data_filepath = download(url)

        #TODO: encoding ? 
        self.fp = open(data_filepath, encoding='latin-1')
        self.sheet = csv.DictReader(self.fp, dialect=csv.excel_tab)             

        self.years = self.sheet.fieldnames[9:-1]
        self.start_date = pandas.Period(self.years[0], freq='A')
        self.end_date = pandas.Period(self.years[-1], freq='A')
    
        #ex: http://www.imf.org/external/pubs/ft/weo/2006/02/data/WEOSep2006all.xls]
        date_str = match(".*WEO(\w{7})", url).groups()[0] #Sep2006
        self.release_date = datetime.strptime(date_str, "%b%Y") #2006-09-01 00:00:00

    def __next__(self):
        row = next(self.sheet) 
        series = self.build_series(row)
        if series is None:
            if self.fp and not self.fp.close():
                self.fp.close()
            raise StopIteration()            
        return(series)
        
    def build_series(self,row):
        if row['Country']:               
            series = {}
            values = []
            dimensions = {}
            
            for year in self.years:
                values.append(row[year])
            
            dimensions['Country'] = self.dimension_list.update_entry('Country', 
                                                                     row['ISO'], 
                                                                     row['Country'])
            dimensions['WEO Country Code'] = self.dimension_list.update_entry('WEO Country Code', 
                                                                              row['WEO Country Code'], 
                                                                              row['Country'])
            dimensions['Subject'] = self.dimension_list.update_entry('Subject', 
                                                                     row['WEO Subject Code'], 
                                                                     row['Subject Descriptor'])
            dimensions['Units'] = self.dimension_list.update_entry('Units', '', row['Units'])
            dimensions['Scale'] = self.dimension_list.update_entry('Scale', row['Scale'], row['Scale'])
            
            series_name = row['Subject Descriptor']+'.'+row['Country']+'.'+row['Units']
            series_key = row['WEO Subject Code']+'.'+row['ISO']+'.'+dimensions['Units']
            
            release_dates = [self.release_date for v in values]
            #print("release_dates : ", release_dates)
            #datetime.datetime(2006, 9, 1, 0, 0)
            series['provider_name'] = self.provider_name
            series['dataset_code'] = self.dataset_code
            series['name'] = series_name
            series['key'] = series_key
            series['values'] = values
            series['attributes'] = {}
            if row['Estimates Start After']:
                estimation_start = int(row['Estimates Start After']);
                series['attributes'] = {'flag': [ '' if int(y) < estimation_start else 'e' for y in self.years]}
            series['dimensions'] = dimensions
            
            #TODO: a verifier
            series['last_update'] = self.release_date
            
            series['release_dates'] = release_dates
            series['start_date'] = self.start_date.ordinal
            series['end_date'] = self.end_date.ordinal
            series['frequency'] = 'A'
            
            if row['Subject Notes']:
                series['notes'] = row['Subject Notes']
            
            if row['Country/Series-specific Notes']:
                row['Country/Series-specific Notes'] += '\n' + row['Country/Series-specific Notes']
            return(series)
        else:
            return None
        
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    import sys
    print("WARNING : run main for testing only", file=sys.stderr)
    try:
        import requests_cache
        cache_filepath = os.path.abspath(os.path.join(tempfile.gettempdir(), 'dlstats_cache'))        
        requests_cache.install_cache(cache_filepath, backend='sqlite', expire_after=None)#=60 * 60) #1H
        print("requests cache in %s" % cache_filepath)
    except ImportError:
        pass
    
    w = IMF()
    w.provider.update_database()
    w.upsert_categories()
    w.upsert_all_datasets()
     
