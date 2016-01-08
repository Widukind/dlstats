# -*- coding: utf-8 -*-

import tempfile
import logging
import os
import zipfile
import datetime
import time

import xlrd
import pandas
import requests

from dlstats.fetchers._commons import Fetcher, Datasets, Providers

VERSION = 1

logger = logging.getLogger(__name__)

DATASETS = {
    'GEM': { 
        'name': 'Global Economic Monitor',
        'doc_href': 'http://data.worldbank.org/data-catalog/global-economic-monitor',
        'url': 'http://siteresources.worldbank.org/INTPROSPECTS/Resources/GemDataEXTR.zip',
        'filename': 'GemDataEXTR.zip',
    },
}

#TODO: is_updated function by datasets or by excel sheet ?

class WorldBank(Fetcher):

    def __init__(self, db=None):
        
        super().__init__(provider_name='WorldBank',  db=db)         
        
        self.provider = Providers(name=self.provider_name,
                                 long_name='World Bank',
                                 version=VERSION,
                                 region='world',
                                 website='http://www.worldbank.org/',
                                 fetcher=self)
       
    def upsert_categories(self):
        data_tree = {'name': 'World Bank',
                     'category_code': 'worldbank_root',
                     'children': [{'name': 'GEM' , 
                                   'category_code': 'GEM',
                                   'exposed': True}]}
        self.fetcher.provider.add_data_tree(data_tree)

    def upsert_dataset(self, dataset_code):
        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        #TODO return the _id field of the corresponding dataset. Update the category accordingly.
        if dataset_code=='GEM':
            self.upsert_gem(dataset_code)
        else:
            raise Exception("This dataset is unknown" + dataCode)                 
        self.update_metas(dataset_code)        
        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))

    def upsert_gem(self, dataset_code):
        d = DATASETS[dataset_code]
        url = d['url']
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code, 
                           name=d['name'], 
                           doc_href=d['doc_href'], 
                           fetcher=self)
        gem_data = GemData(dataset, url)
        dataset.last_update = gem_data.release_date
        dataset.series.data_iterator = gem_data
        dataset.update_database()
        
    def upsert_all_datasets(self):
        start = time.time()
        logger.info("update fetcher[%s] - START" % (self.provider_name))
        self.upsert_dataset('GEM')  
        end = time.time() - start
        logger.info("update fetcher[%s] - END - time[%.3f seconds]" % (self.provider_name, end))

    def datasets_list(self):
        return DATASETS.keys()

    def datasets_long_list(self):
        return [(key, dataset['name']) for key, dataset in DATASETS.items()]

    def download(self, dataset_code=None, url=None):

        filepath_dir = os.path.abspath(os.path.join(tempfile.gettempdir(), 
                                        self.provider_name))
        
        filepath = "%s.zip" % os.path.abspath(os.path.join(filepath_dir, dataset_code))

        if not os.path.exists(filepath_dir):
            os.makedirs(filepath_dir, exist_ok=True)
            
        if os.path.exists(filepath):
            os.remove(filepath)
            
        if logger.isEnabledFor(logging.INFO):
            logger.info("store file to [%s]" % filepath)

        start = time.time()
        try:
            response = requests.get(url, 
                                    #TODO: timeout=self.timeout, 
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

            return response.headers['Last-Modified'], filepath
                
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

class GemData:

    def __init__(self, dataset, url, is_autoload=True):
        self.dataset = dataset
        self.provider_name = dataset.provider_name
        self.dataset_code = dataset.dataset_code
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        self.last_update = []
        self.columns = iter([])
        self.sheets = iter([])
        self.url = url
        self.release_date = None
        self.excel_filenames = []
        self.freq_long_name = {'A': 'Annual', 'Q': 'Quarterly', 'M': 'Monthly', 'D': 'Daily'}
        self.zipfile = None
        
        if is_autoload:
            self.load_datas()
            
    def load_datas(self):
        
        release_date_str, filepath = self.dataset.fetcher.download(dataset_code=self.dataset_code, 
                                                                  url=self.url)
            
        self.release_date = datetime.datetime.strptime(release_date_str, 
                                                      "%a, %d %b %Y %H:%M:%S GMT")
        
        self.zipfile = zipfile.ZipFile(filepath)
        self.excel_filenames = iter(self.zipfile.namelist())
     
    def __iter__(self):
        return self

    def __next__(self):
        return(self.build_series())

    def build_series(self):
        try:
            column = next(self.columns)
        except StopIteration:
            self.update_sheet()
            column = next(self.columns)
        dimensions = {}
        col_header = self.sheet.cell_value(0,column)
        if self.series_name == 'Commodity Prices':
            dimensions['Commodity'] = self.dimension_list.update_entry('Commodity','',col_header)
        else:    
            dimensions['Country'] = self.dimension_list.update_entry('Country','',col_header) 
        values = [str(v) for v in self.sheet.col_values(column,start_rowx=1)]
        #release_dates = [self.last_update for v in values]
        series_key = self.series_name.replace(' ','_').replace(',', '')
        # don't add a period if there is already one
        if series_key[-1] != '.':
            series_key += '.'
        series_key += col_header + '.' + self.frequency
        series = {}
        series['provider_name'] = self.provider_name
        series['dataset_code'] = self.dataset_code
        series['name'] = self.series_name + '; ' + col_header + '; ' + self.freq_long_name[self.frequency]
        series['key'] = series_key
        series['values'] = values
        series['attributes'] = {}
        series['dimensions'] = dimensions
        series['last_update'] = self.last_update
        #series['release_dates'] = release_dates
        series['start_date'] = self.start_date
        series['end_date'] = self.end_date
        series['frequency'] = self.frequency
        return(series)
    
    def update_sheet(self):
        try:
            self.sheet = next(self.sheets)
        except StopIteration:
            self.update_file()
            self.sheet = next(self.sheets)
            
        self.columns = iter(range(1,self.sheet.row_len(0)))
        periods = self.sheet.col_slice(0, start_rowx=2)
        start_period = periods[0].value
        end_period = periods[-1].value
        if self.sheet.name == 'annual':    
            self.frequency = 'A'
            self.start_date = pandas.Period(str(int(start_period)),freq='A').ordinal
            self.end_date = pandas.Period(str(int(end_period)),freq='A').ordinal
        elif self.sheet.name == 'quarterly':    
            self.frequency = 'Q'
            self.start_date = pandas.Period(start_period,freq='Q').ordinal
            self.end_date = pandas.Period(end_period,freq='Q').ordinal
        elif self.sheet.name == 'monthly':    
            self.frequency = 'M'
            self.start_date = pandas.Period(start_period.replace('M','-'),freq='M').ordinal
            self.end_date = pandas.Period(end_period.replace('M','-'),freq='M').ordinal
        elif self.sheet.name == 'daily':    
            self.frequency = 'D'
            self.start_date = self.translate_daily_dates(start_period).ordinal
            self.end_date = self.translate_daily_dates(end_period).ordinal

    def translate_daily_dates(self,value):
            date = xlrd.xldate_as_tuple(value,self.excel_book.datemode)
            return(pandas.Period(year=date[0],month=date[1],day=date[2],freq=self.frequency))
        
    def update_file(self):
        fname = next(self.excel_filenames)
        info = self.zipfile.getinfo(fname)
        while True:
            #bypass directory - not excel file
            if info.file_size > 0 and not info.filename.endswith('/'):
                break
            
            fname = next(self.excel_filenames)
            info = self.zipfile.getinfo(fname)
            
        self.series_name = fname[:-5]
        self.last_update = datetime.datetime(*self.zipfile.getinfo(fname).date_time[0:6])
        self.excel_book = xlrd.open_workbook(file_contents = self.zipfile.read(fname))
        self.sheets = iter([s for s in self.excel_book.sheets()
                            if s.name not in ['Sheet1','Sheet2','Sheet3','Sheet4',
                                              'Feuille1','Feuille2','Feuille3','Feuille4']])
                                              
                                            

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    import sys
    print("WARNING : run main for testing only", file=sys.stderr)
    import tempfile
    import os
    try:
        import requests_cache
        cache_filepath = os.path.abspath(os.path.join(tempfile.gettempdir(), 'dlstats_cache'))        
        requests_cache.install_cache(cache_filepath, backend='sqlite', expire_after=None)#=60 * 60) #1H
        print("requests cache in %s" % cache_filepath)
    except ImportError:
        pass
    
    w = WorldBank()
    w.provider.update_database()
    w.upsert_categories()
    w.upsert_all_datasets()
