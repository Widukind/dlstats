# -*- coding: utf-8 -*-

import os
import io
from zipfile import ZipFile
import zipfile
import csv
import datetime
import tempfile
import time
import logging

import pandas
import requests

from dlstats import constants
from dlstats.fetchers._commons import Fetcher, Categories, Datasets, Providers

__all__ = ['BIS']

logger = logging.getLogger(__name__)

def extract_zip_file(filepath):
    """Extract first file in zip file and return absolute path for the file extracted
    
    :param str filepath: Absolute file path of zip file
    
    Example: 
        file1.zip contains one file: file1.csv
    
    >>> extract_zip_file('/tmp/file1.zip')
    '/tmp/file1.csv'
        
    """
    zfile = zipfile.ZipFile(filepath)
    filename = zfile.namelist()[0]
    return zfile.extract(filename, os.path.dirname(filepath))

def csv_dict(headers, array_line):
    """Convert list1 (keys), list2 (values) to dict()
    """
    return dict(zip(headers, array_line))

def local_read_csv(filepath=None, fileobj=None, 
                   headers_line=4, date_format="%a %b %d %H:%M:%S %Z %Y"):
    """CSV reader for bad CSV format (BIS, ?)
    
    Return:
    
        - rows: _csv.reader for iterations - current line is first data line
        - headers: list of headers (Time Period replaced by KEY)
        - release_date: datetime.datetime() instance
        - dimension_keys: List of dimension keys
        - periods: List of periods

    >>> filepath = '/tmp/full_bis_lbs_diss_csv.csv'
    >>> rows, headers, release_date, dimension_keys, periods = local_read_csv(filepath=filepath)
    >>> line1 = csv_dict(headers, next(rows))
    
    """
    if filepath:
        _file = open(filepath)
    else:
        _file = fileobj
    
    rows = csv.reader(_file)
    release_date_txt = None
    dimension_keys = []
    headers = []
    periods = []        
    
    for i in range(headers_line):
        line = next(rows)
        if line and "Retrieved on" in line:
            release_date_txt = line[1]
        if rows.line_num == headers_line:
            break
    
    release_date = datetime.datetime.strptime(release_date_txt, date_format)
    headers_list = next(rows)
    headers_list_copy = headers_list.copy()
    
    for h in headers_list:
        if h == "Time Period":
            headers_list_copy.pop(0)
            break
        dimension_keys.append(headers_list_copy.pop(0))
        
    periods = headers_list_copy    
    headers = dimension_keys + ["KEY"] + periods
    return rows, headers, release_date, dimension_keys, periods

PROVIDER_NAME = "BIS"

DATASETS = {
    'LBS-DISS': { 
        'name': 'Locational Banking Statistics - disseminated data',
        'doc_href': 'http://www.bis.org/statistics/bankstats.htm',
        'url': 'http://www.bis.org/statistics/full_bis_lbs_diss_csv.zip',
        'filename': 'full_bis_lbs_diss_csv.zip',
        'frequency': 'Q',
        'lines': {
            'release_date': 1,
            'headers': 4
        }
    },
    'CBS': { 
        'name': 'Consolidated banking statistics',
        'doc_href': 'http://www.bis.org/statistics/bankstats.htm',
        'url': 'https://www.bis.org/statistics/full_bis_cbs_csv.zip',
        'filename': 'full_bis_cbs_csv.zip',
        'frequency': 'Q',
        'lines': {
            'release_date': 1,
            'headers': 5
        }
    },
    'DSS': {
        'name': 'Debt securities statistics',
        'doc_href': 'TODO:',
        'url': 'https://www.bis.org/statistics/full_bis_debt_sec2_csv.zip',
        'filename': 'full_bis_debt_sec2_csv.zip',
        'frequency': 'Q',
        'lines': {
            'release_date': 1,
            'headers': 7
        }
    },     
    'CNFS': {
        'name': 'Credit to the non-financial sector',
        'doc_href': 'TODO:',
        'url': 'https://www.bis.org/statistics/full_bis_total_credit_csv.zip',
        'filename': 'full_bis_total_credit_csv.zip',
        'frequency': 'Q',
        'lines': {
            'release_date': 1,
            'headers': 4
        }
    },     
    'DSRP': {
        'name': 'Debt service ratios for the private non-financial sector',
        'doc_href': 'TODO:',
        'url': 'https://www.bis.org/statistics/full_bis_dsr_csv.zip',
        'filename': 'full_bis_dsr_csv.zip',
        'frequency': 'Q',
        'lines': {
            'release_date': 1,
            'headers': 4
        }
    },     
    'PP-SS': {
        'name': 'Property prices - selected series',
        'doc_href': 'TODO:',
        'url': 'https://www.bis.org/statistics/full_bis_selected_pp_csv.zip',
        'filename': 'full_bis_selected_pp_csv.zip',
        'frequency': 'Q',
        'lines': {
            'release_date': 1,
            'headers': 4
        }
    },     
    'PP-LS': {
        'name': 'Property prices - long series',
        'doc_href': 'TODO:',
        'url': 'https://www.bis.org/statistics/full_bis_long_pp_csv.zip',
        'filename': 'full_bis_long_pp_csv.zip',
        'frequency': 'Q',
        'lines': {
            'release_date': 1,
            'headers': 4
        }
    },     
    'EERI': {
        'name': 'Effective exchange rate indices',
        'doc_href': 'TODO:',
        'url': 'https://www.bis.org/statistics/full_bis_eer_csv.zip',
        'filename': 'full_bis_eer_csv.zip',
        'frequency': 'M',
        'lines': {
            'release_date': 1,
            'headers': 4
        }
    },     
}

class Downloader():
    
    headers = {
        'user-agent': 'dlstats - https://github.com/Widukind/dlstats'
    }
    
    def __init__(self, url=None, filename=None, store_filepath=None, 
                 timeout=None, max_retries=0, replace=True):
        self.url = url
        self.filename = filename
        self.store_filepath = store_filepath
        self.timeout = timeout
        self.max_retries = max_retries
        
        if not self.store_filepath:
            self.store_filepath = tempfile.mkdtemp()
        else:
            if not os.path.exists(self.store_filepath):
                os.makedirs(self.store_filepath, exist_ok=True)
        
        self.filepath = os.path.abspath(os.path.join(self.store_filepath, self.filename))
        
        #TODO: force_replace ?
        
        if os.path.exists(self.filepath) and not replace:
            raise Exception("filepath is already exist : %s" % self.filepath)
        
    def _download(self):
        
        #TODO: timeout
        #TODO: max_retries (self.max_retries)
        #TODO: analyse rate limit dans headers
        
        start = time.time()
        try:
            #TODO: Session ?
            response = requests.get(self.url, 
                                    timeout=self.timeout, 
                                    stream=True, 
                                    allow_redirects=True,
                                    verify=False, #ssl
                                    headers=self.headers)

            if not response.ok:
                msg = "download url[%s] - status_code[%s] - reason[%s]" % (self.url, 
                                                                           response.status_code, 
                                                                           response.reason)
                logger.error(msg)
                raise Exception(msg)
            
            with open(self.filepath,'wb') as f:
                for chunk in response.iter_content():
                    f.write(chunk)
                    #TODO: flush ?            
                
            #TODO: response.close() ?
            
        except requests.exceptions.ConnectionError as err:
            raise Exception("Connection Error")
        except requests.exceptions.ConnectTimeout as err:
            raise Exception("Connect Timeout")
        except requests.exceptions.ReadTimeout as err:
            raise Exception("Read Timeout")
        except Exception as err:
            raise Exception("Not captured exception : %s" % str(err))            

        end = time.time() - start
        logger.info("download file[%s] - END - time[%.3f seconds]" % (self.url, end))
    
    def get_filepath(self, force_replace=False):
        
        if os.path.exists(self.filepath) and force_replace:
            os.remove(self.filepath)
        
        if not os.path.exists(self.filepath):
            logger.info("not found file[%s] - download dataset url[%s]" % (self.filepath, self.url))
            self._download()
        else:
            logger.info("use local dataset file [%s]" % self.filepath)
        
        return self.filepath

class BIS(Fetcher):
    
    def __init__(self, db=None, es_client=None):
        
        super().__init__(provider_name='BIS', 
                         db=db, 
                         es_client=es_client)
        
        self.provider = Providers(name=self.provider_name,
                                  long_name='Bank for International Settlements',
                                  region='world',
                                  website='http://www.bis.org', 
                                  fetcher=self)

    def upsert_dataset(self, dataset_code, datas=None):
        
        start = time.time()
        
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        
        if not DATASETS.get(dataset_code):
            raise Exception("This dataset is unknown" + dataset_code)
        
        #TODO: faire un DatasetBis pour inclure le Downloader à l'init comme callback ?
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code, 
                           name=DATASETS[dataset_code]['name'], 
                           doc_href=DATASETS[dataset_code]['doc_href'],
                           fetcher=self)
        
        fetcher_data = BIS_Data(dataset, 
                                url=DATASETS[dataset_code]['url'], 
                                filename=DATASETS[dataset_code]['filename'])
        dataset.series.data_iterator = fetcher_data
        dataset.update_database()

        #TODO: clean datas (file temp)

        end = time.time() - start
        logger.info("upsert dataset[%s] - END-BEFORE-METAS - time[%.3f seconds]" % (dataset_code, end))

        self.update_metas(dataset_code)
        
        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))
        
    def upsert_all_dataset(self):
        
        for dataset_code in DATASETS.keys():
            self.upsert_dataset(dataset_code) 
        
    def upsert_categories(self):
        
        for dataset_code in DATASETS.keys():
            document = Categories(provider=self.provider_name, 
                                  name=DATASETS[dataset_code]['name'], 
                                  categoryCode=dataset_code,
                                  exposed=True,
                                  fetcher=self)
            
            #TODO: attention, plus de retour du result pymongo
            document.update_database()                            

class BIS_Data():
    
    def __init__(self, dataset, url=None, filename=None, store_filepath=None, is_autoload=True):

        self.dataset = dataset
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        
        self.url = url
        self.filename = filename
        self.store_filepath = store_filepath
        
        self.frequency = 'Q'
        
        self.release_date = None
        self.dimension_keys = None
        self.periods = None
        self.start_date = None
        self.end_date = None

        self.rows = None
                
        if is_autoload:
            self._load_datas()
        
    def get_store_path(self):
        return self.store_filepath or os.path.abspath(os.path.join(
                                                                tempfile.gettempdir(), 
                                                                self.dataset.provider_name, 
                                                                self.dataset.dataset_code))
        
    def _load_datas(self, datas=None):
        
        kwargs = {}
        
        if not datas:
            store_filepath = self.get_store_path()
            # TODO: timeout, replace
            download = Downloader(url=self.url, filename=self.filename, store_filepath=store_filepath)
            
            filepath = extract_zip_file(download.get_filepath())
            kwargs['filepath'] = filepath
        else:
            kwargs['fileobj'] = io.StringIO(datas, newline="\n")
        
        kwargs['date_format'] = "%a %b %d %H:%M:%S %Z %Y"
        kwargs['headers_line'] = DATASETS[self.dataset.dataset_code]['lines']['headers']
        self.rows, self.headers, self.release_date, self.dimension_keys, self.periods = local_read_csv(**kwargs)
        
        self.dataset.last_update = self.release_date
            
        self.start_date = pandas.Period(self.periods[0], freq=self.frequency)
        self.end_date = pandas.Period(self.periods[-1], freq=self.frequency)
        
        
    def __next__(self):
        row = csv_dict(self.headers, next(self.rows)) 
        series = self.build_serie(row)
        if series is None:
            #TODO: close self.rows and delete file ?
            raise StopIteration()
        return(series)
    
    def build_serie(self, row):
        """Build one serie
        
        Return instance of :class:`dict`
        """
        series_key = row['KEY']
        
        logger.debug("provider[%s] - dataset[%s] - serie[%s]" % (self.dataset.provider_name,
                                                                 self.dataset.dataset_code,
                                                                 series_key))

        values = [row[period] for period in self.periods]
        dimensions = {}
        
        for d in self.dimension_keys:
            dim_short_id = row[d].split(":")[0]
            dim_long_id = row[d].split(":")[1]
            dimensions[d] = self.dimension_list.update_entry(d, dim_short_id, dim_long_id)
            
        series_name = "-".join([row[d] for d in self.dimension_keys])


        data = {'provider': self.dataset.provider_name,
                'datasetCode': self.dataset.dataset_code,
                'name': series_name,
                'key': series_key,
                'values': values,
                'attributes': {},
                'dimensions': dimensions,
                'lastUpdate': self.release_date,
                'startDate': self.start_date.ordinal,
                'endDate': self.end_date.ordinal,
                'frequency': self.frequency}
        return(data)
        
    
def download_all_sources():
    """Download all datasets files (if not exist) and store local temp directory
    
    Store in /[TMP_DIR]/[PROVIDER_NAME]/[DATASET_CODE]/[FILENAME]
    
    return a dict with key is filename and value is full filepath
    """
    
    filepaths = {}
    
    for dataset_code, dataset in DATASETS.items():
        store_filepath = os.path.abspath(os.path.join(tempfile.gettempdir(), PROVIDER_NAME, dataset_code))
        download = Downloader(url=dataset['url'], filename=dataset['filename'], store_filepath=store_filepath)# TODO:, timeout, replace)
        filepaths[dataset['filename']] = os.path.abspath(os.path.join(store_filepath, dataset['filename']))
        logger.info("Download file[%s]" % download.get_filepath())
        
    return filepaths
        
        
def main():
    #from dlstats.mongo_client import mongo_client
    #db = mongo_client.widukind_test
    #fetcher = BIS(db=db)
    fetcher = BIS()
    fetcher.provider.update_database()
    fetcher.upsert_categories()
    fetcher.upsert_all_dataset()

if __name__ == "__main__":
    main()
