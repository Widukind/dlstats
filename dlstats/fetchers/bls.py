# -*- coding: utf-8 -*-
"""
Created on Wed February 15, 2017

"""

import time
from datetime import datetime, date
from urllib.parse import urljoin
import logging
import re
import csv
import collections
import pandas
import calendar
import copy
import collections

from lxml import etree
import requests

from dlstats.utils import Downloader, make_store_path
from dlstats.fetchers._commons2 import Fetcher, Datasets, Providers, Categories

VERSION = 1

logger = logging.getLogger(__name__)

FREQUENCIES_SUPPORTED = ["A", "Q", "S", "M"]
FREQUENCIES_REJECTED = []

INDEX_URL = "https://www.bls.gov/data/"

def retry(tries=1, sleep_time=2):
    """Retry calling the decorated function
    :param tries: number of times to try
    :type tries: int
    """
    def try_it(func):
        def f(*args,**kwargs):
            attempts = 0
            while True:
                try:
                    return func(*args,**kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts > tries:
                        raise e
                    time.sleep(sleep_time)
        return f
    return try_it

# replaces dlstats.fetchers.utils.get_ordinal_from_period until it is fixed for freq='S'
def get_ordinal_from_year_subperiod(year, subperiod, freq=None):
    
    period_ordinal = None
    if freq == "A":
        period_ordinal = int(year) - 1970
    elif freq == "S":
        period_ordinal = 2*(int(year) - 1970) + int(subperiod) - 1
    elif freq == "Q":
        period_ordinal = 4*(int(year) - 1970) + int(subperiod) - 1
    elif freq == "M":
        period_ordinal = 12*(int(year) - 1970) + int(subperiod) - 1
    else:
        raise NotImplementedError("freq not implemented freq[%s] date[%s]" % (freq, year+subperiod))

    return period_ordinal

    
def get_date(year,subperiod,frequency):
    """Builds date string compatible with get_ordinal_from_period()
    Change frequency for M13 and S03
    Returns a date_string and a frequency character code
    """
    if frequency == 'A':
        date_string = year
    elif frequency == 'S':
        if subperiod == 'S03':
            date_string = year
            frequency = 'A'
        else:
            date_string = year + 'S' + subperiod[-1]
    elif subperiod == 'M13':
        date_string = year
        frequency = 'A'
    else:
        date_string = year + '-' + subperiod
    return (date_string, frequency)
    
def download_page(url):
    
    url = url.strip()
    
    response = requests.get(url)

    if not response.ok:
        msg = "download url[%s] - status_code[%s] - reason[%s]" % (url, 
                                                                   response.status_code, 
                                                                   response.reason)
        logger.error(msg)
        response.raise_for_status()
        #raise Exception(msg)
    
    return response.content

def parse_bls_site():
    page = download_page(INDEX_URL)
    html = etree.HTML(page)
    h1s = html.findall('.//h1')
    site_tree = []
    for i,h1 in enumerate(h1s):
        site_tree.append(parse_h1(h1,str(i+1)))
    return site_tree

def parse_h1(h1,code):
    branch = {
        'name': h1.text,
        'category_code': code,
        'doc_href': None,
        'children': [],
        }
    script = h1.getnext()
    table = script.getnext()
    trs = table.findall('.//tr')
    # skipping table's header
    counter1 = 1
    subcode = None
    subbranch = None
    subbranch_has_dataset = False
    for tr in trs[1:]:
        tds = tr.findall('.//td')
        if len(tds) == 1:
            if len(tds[0]) > 0 and tds[0][0].tag == 'p':
                subbranch['datasets'][-1]['notes'] = tds[0][0].text
                continue
            if subbranch_has_dataset is True:
                branch['children'].append(subbranch)
                subbranch_has_dataset = False
            subcode = str(counter1)
            subbranch = {
                'name': tds[0].text.strip(),
                'category_code': subcode,
                'doc_href': None,
                'datasets': [],
            }
            counter1 += 1
        elif len(tds) == 8 and len(tds[7]) > 0:
            subbranch['datasets'].append(parse_row(tds))
            subbranch_has_dataset = True
    if subbranch_has_dataset is True:
        branch['children'].append(subbranch)
    
    return branch

def parse_row(tds):
    name = tds[0][0].text
    if len(tds[0]) > 1 and tds[0][1].tail is not None:
        name +=  ' ' + tds[0][1].tail.strip()
    if len(tds[1]) > 0:
        doc_href = tds[1][0].get("href")
    else:
        doc_href = None
    href = tds[7][0].get("href")
    dataset_code = href.split('/')[-2]
    dataset = {
        'name': name,
        'dataset_code': dataset_code,
        'url': href,
        'doc_href': doc_href,
        'notes': None,
    }
    return dataset

class Bls(Fetcher):
    
    def __init__(self, **kwargs):
        super().__init__(provider_name='BLS', version=VERSION, **kwargs)
        
        self.provider = Providers(name=self.provider_name,
                                  long_name='Bureau of Labor Statistics',
                                  version=VERSION,
                                  region='United States',
                                  website='https://www.bls.gov/',
                                  fetcher=self)
            
        self.categories_filter = []

    def build_data_tree(self):
        """Build data_tree from BLS site parsing
        """
        categories = []
        def make_node(data, parent_key=None):
            _category = {
                "name": data['name'],
                "category_code": data['category_code'],
                "parent": parent_key,
                "all_parents": [],
                "datasets": []
            }
            if parent_key:
                _category['category_code'] = "%s.%s" % (parent_key, _category['category_code'])
            
            _category_key = _category['category_code']
            
            if 'children' in data:
                for c in data['children']:
                    make_node(c, _category_key)
            
            if 'datasets' in data:
                for d in data['datasets']:
                    _dataset = {
                        "dataset_code": d['dataset_code'],
                        "name": d['name'],
                        "last_update": None,
                        "metadata": {
                            'url': d['url'], 
                            'doc_href': d['doc_href']
                        }
                    }                    
                    _category["datasets"].append(_dataset)
                    
            categories.append(_category)
        
        try:
            for data in parse_bls_site():
                make_node(data)
        except Exception as err:
            logger.error(err)   
            raise
        
        _categories = dict([(doc["category_code"], doc) for doc in categories])
        
        for c in categories:
            parents = Categories.iter_parent(c, _categories)
            c["all_parents"] = parents

        return categories
        

    def upsert_dataset(self, dataset_code):
        """Updates data in Database for selected datasets
        :dset: dataset_code
        :returns: None"""
        self.get_selected_datasets()
        
        self.dataset_settings = self.selected_datasets[dataset_code]        
        
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code, 
                           name=self.dataset_settings['name'], 
                           doc_href=self.dataset_settings['metadata']['doc_href'], 
#                           last_update=self.dataset_settings['last_update'], 
                           fetcher=self)

        url = self.dataset_settings['metadata']['url']
        dataset.series.data_iterator = BlsData(dataset, url)

        results = dataset.update_database()
        
        # reconstruct code_list using codes effectively found in the data
        
        return results

    # TO BE DONE
    def _parse_agenda(self):
        pass
    
class SeriesIterator:
    def __init__(self,url,filename,store_path,use_existing_file):
        self.row_iter = self.iter_row(url,filename,store_path,use_existing_file)
        self.current_row = None
        self.end_of_file = False
        self.footnote_list = []
        
    def iter_row(self,url,filename,store_path,use_existing_file):
        download = Downloader(url=url,
                              filename = filename,
                              store_filepath = store_path,
                              use_existing_file = use_existing_file)
        filepath = download.get_filepath()
        with  open(filepath) as source_file:
            data = csv.reader(source_file,delimiter='\t')
            fields = [f.strip() for f in next(data)]
            #check that data are in the right order
            assert(fields == ['series_id', 'year', 'period', 'value', 'footnote_codes'])
            for row in data:
                yield [elem.strip() for elem in row]

    def __iter__(self):
        return self

    def get_value(self,row,period):
        """Forms one value dictionary
        Returns a dict
        """
        if len(row[4]) > 0:
            attribute = {'footnote': row[4]}
            # several footnotes comma separated
            for f in row[4].split(','):
                if f not in self.footnote_list:
                    self.footnote_list.append(f)
        else:
            attribute = None
        return { 
            'attributes': attribute,
            'period': str(period),
            'value': row[3],
        }

    def fill_series(self,series,previous_period,period):
        """Appends nan values to series
        Returns a list of value dict
        """
        while previous_period + 1 < period:
            series.append({
                'attributes': None,
                'period': str(previous_period + 1),
                'value': 'nan',
            })
            previous_period += 1
        return series
                
    def get_start_ts(self,year,subperiod,frequency):
        year = int(year)
        if frequency == 'A':
            ts = datetime(year,1,1)
        elif frequency == 'S':
            if subperiod == 'S01':
                ts = datetime(year,1,1)
            elif subperiod == 'S02':
                ts = datetime(year,7,1)
            elif subperiod == 'S03':
                ts = datetime(year,1,1)
        elif frequency == 'Q':
            if subperiod == 'Q01':
                ts = datetime(year,1,1)
            elif subperiod == 'Q02':
                ts = datetime(year,4,1)
            elif subperiod == 'Q03':
                ts = datetime(year,7,1)
            elif subperiod == 'Q04':
                ts = datetime(year,10,1)
            elif subperiod == 'Q05':
                ts = datetime(year,1,1)
        elif frequency == 'M':
            if subperiod == 'M13':
                ts = datetime(year,1,1)
            else:
                month = int(subperiod[1:])
                ms = calendar.monthrange(year,month)
                ts = datetime(year,month,ms[1])
        return ts
    
    def get_end_ts(self,year,subperiod,frequency):
        year = int(year)
        if frequency == 'A':
            ts = datetime(year,12,31,23,59)
        elif frequency == 'S':
            if subperiod == 'S01':
                ts = datetime(year,6,30,23,59)
            elif subperiod == 'S02':
                ts = datetime(year,12,31,23,59)
            elif subperiod == 'S03':
                ts = datetime(year,12,31,23,59)
        elif frequency == 'Q':
            if subperiod == 'Q01':
                ts = datetime(year,3,31,23,59)
            elif subperiod == 'Q02':
                ts = datetime(year,6,30,23,59)
            elif subperiod == 'Q03':
                ts = datetime(year,9,30,23,59)
            elif subperiod == 'Q04':
                ts = datetime(year,12,31,23,59)
            elif subperiod == 'Q05':
                ts = datetime(year,12,31,23,59)
        elif frequency == 'M':
            if subperiod == 'M13':
                ts = datetime(year,12,31,23,59)
            else:
                month = int(subperiod[1:])
                ms = calendar.monthrange(year,month)
                ts = datetime(year,month,ms[1],23,59)
        return ts

    def __next__(self):
        """Iterators for basic data for series
        Two series are handled when annual avg are intertwined with monthly
        or semestrial data
        Returns a dict
        """
        if self.end_of_file:
            raise StopIteration
        frequency = None
        values = []
        values_annual = []
        period = None
        period_annual = None
        previous_period = None
        previous_period_annual = None
        start_period = None
        start_period_annual = None
        end_period_annual = None
        start_ts = None
        end_ts = None
        start_ts_annual = None
        end_ts_annual = None
        dates = None
        dates_annual = None
        # fetch first row if it is waiting
        if self.current_row is not None:
            row = self.current_row
            self.current_row = None
        else:
            row = [elem.strip() for elem in next(self.row_iter)]
        series_id = row[0]
        while len(row) > 0 and row[0] == series_id:    
            if row[2] == 'M13' or row[2] == 'Q05' or row[2] == 'S03':
                period_annual = get_ordinal_from_year_subperiod(row[1],None,freq='A')
                if start_period_annual is None:
                    start_period_annual = period_annual
                    start_ts_annual = self.get_start_ts(row[1],None,'A')
                if previous_period_annual is not None and previous_period_annual + 1 < period_annual:
                    values_annual = self.fill_series(values_annual,previous_period_annual,period_annual)
                values_annual.append(self.get_value(row,period_annual))
                previous_period_annual = period_annual
                dates_annual = row[1]
            else:
                frequency = row[2][0]
                period = get_ordinal_from_year_subperiod(row[1],row[2][1:],freq=frequency)
                if start_period is None:
                    start_period = period
                    start_ts = self.get_start_ts(row[1],row[2],frequency)
                if previous_period is not None and previous_period + 1 < period:
                    values = self.fill_series(values,previous_period,period)
                values.append(self.get_value(row,period))
                previous_period = period
                dates = row[1:3]
            try:
                row = [elem.strip() for elem in next(self.row_iter)]
            except:
                self.end_of_file = True
                break
        self.current_row = row
        if dates is not None:
            end_ts = self.get_end_ts(dates[0],dates[1],frequency)
        if dates_annual is not None:
            end_ts_annual = self.get_end_ts(dates_annual,None,'A')
        return({
            'series_id': series_id,
            'frequency': frequency,
            'values': values,
            'values_annual': values_annual,
            'start_period': start_period,
            'end_period': period,
            'start_ts': start_ts,
            'end_ts': end_ts,
            'start_period_annual': start_period_annual,
            'end_period_annual': period_annual,
            'start_ts_annual': start_ts_annual,
            'end_ts_annual': end_ts_annual,
            'footnote_list': self.footnote_list,
        })
    
class BlsData:
    
    def __init__(self, dataset,  url):
        self.dataset = dataset
        self.dataset_url = url
        self.fetcher = self.dataset.fetcher
        
        self.provider_name = self.dataset.provider_name
        self.dataset_code = self.dataset.dataset_code
        
        self.store_path = self.get_store_path()
        self.data_directory = self.get_data_directory()
        self.data_filenames = self.get_data_filenames(self.data_directory)
        self.data_iterators = self.get_data_iterators()
        series_filepath = self.get_series_filepath()
        self.series_fields = self.get_series_fields(series_filepath)
        self.dataset.dimension_keys = self.get_dimension_keys()
        self.dataset.attribute_keys = ['footnote']
        self.dataset.dimension_list = collections.OrderedDict((k,{}) for k in self.dataset.dimension_keys)
        self.dataset.attribute_list = collections.OrderedDict((k,{}) for k in self.dataset.attribute_keys)
        self.dataset.codelists = collections.OrderedDict((k,{}) for k in self.dataset.dimension_keys + self.dataset.attribute_keys)
        self.dataset.concepts = collections.OrderedDict((k,{}) for k in self.dataset.dimension_keys + self.dataset.attribute_keys)
        self.series_iter = self.get_series_iterator(series_filepath)
        self.code_list = self.get_code_list()
        self.available_series = self.available_series_init()
        self.annual_series = None
        self.current_row = None
        self.release_date = self.get_release_date()
        self.dataset.last_update = self.release_date

        self.frequency = None
        self.start_date = float('inf')
        self.end_date = float('-inf')

        self.dataset.add_frequency(self.frequency)
        
    def get_store_path(self):
        return make_store_path(base_path=self.fetcher.store_path,
                               dataset_code=self.dataset_code)

    def _load_datas(self):
        # TODO: timeout, replace
        download = Downloader(url=self.dataset_url, 
                              filename=self.dataset_code,
                              store_filepath=self.store_path,
                              use_existing_file=self.fetcher.use_existing_file)
        filepath = download.get_filepath()
        self.fetcher.for_delete.append(filepath)
        return filepath

    def get_data_directory(self):
        """ Get directory content for one dataset
        Returns a directory dict
        """
        dirname = self.dataset_code
        download = Downloader(url=self.dataset_url, 
                                filename="index.html",
                                store_filepath=self.store_path,
                                use_existing_file=self.fetcher.use_existing_file)
        with open(download.get_filepath()) as f:
            html = etree.HTML(f.read())
        directory = {}
        for br in html.xpath('.//br'):
            text = br.tail
            if not text:
                continue
            entry = text.strip().split()
            filename = br.getnext().text
            splitdate = entry[0].split('/')
            (hour,minute) = entry[1].split(':')
            if entry[2] == 'PM' and int(hour) < 12:
                hour = str(int(hour)+12)
            directory[filename] = {
                'year': int(splitdate[2]),
                'month': int(splitdate[0]),
                'day': int(splitdate[1]),
                'hour': int(hour),
                'minute': int(minute),
            }
        return directory
        
    def get_dimension_keys(self):
        return  [
            f
            for f in self.series_fields
            if f not in ['series_id','series_title', 'series_name', 'footnote','begin_year', 'end_year', 'begin_period', 'end_period']]


    def get_code_list(self):
        """Gets all code lists in a dataset directory
        Returns a dict of dict of dict
        """
        code_list = {}
        for k in self.dataset.dimension_keys + ['footnote']:
            if k != 'seasonal' and k != 'base_period' and k != 'base_date' and k != 'base_year' and k != 'benchmark_year':
                if self.dataset_code == 'wp' and k == 'item':
                    fmt = 2
                elif self.dataset_code == 'cx' and (k == 'item' or k == 'subcategory' or k == 'characteristics'):
                    fmt = 2
                elif (self.dataset_code == 'cs' or self.dataset_code == 'fw' or self.dataset_code == 'ln') and k == 'category':
                    fmt = 2
                elif self.dataset_code == 'is' and k == 'industry':
                    fmt = 2
                elif self.dataset_code == 'la' and k == 'area':
                    fmt = 2
                elif self.dataset_code == 'or' and k == 'occupation':
                    fmt = 3
                elif self.dataset_code == 'oe' and k == 'area':
                    fmt = 4
                else:
                    fmt = 1
                if self.dataset_code == 'ce' and k == 'data_type':
                    filename = 'datatype'
                elif self.dataset_code == 'la' and k == 'srd':
                    filename = 'state_region_division'
                else:
                    filename = k
                if self.dataset_code == 'or' and k == 'occupation':
                    # two codes in one file
                    codes = self.get_dimension_data(self.dataset_code + '.' + filename,fmt)
                    code_list[k] = codes[0]
                    code_list['soc'] = codes[1]
                elif self.dataset_code == 'or' and k == 'soc':
                    continue
                elif self.dataset_code == 'oe' and k == 'area':
                    # two codes in one file
                    codes = self.get_dimension_data(self.dataset_code + '.' + filename,fmt)
                    code_list['state'] = codes[0]
                    code_list[k] = codes[1]
                elif self.dataset_code == 'oe' and k == 'state':
                    continue
                else:
                    code_list[k] = self.get_dimension_data(self.dataset_code + '.' + filename,fmt)[0]
                
        # dimensions that don't have a code file
        if 'seasonal' not in code_list:
            code_list['seasonal'] = {'S': 'Seasonaly adjusted', 'U': 'Unadjusted'}
        if 'base_period' in self.dataset.dimension_keys and 'base_period' not in code_list:
            code_list['base_period'] = {}
        if 'base_date' in self.dataset.dimension_keys and 'base_date' not in code_list:
            code_list['base_date'] = {}
        if 'base_year' in self.dataset.dimension_keys and 'base_year' not in code_list:
            code_list['base_year'] = {}
        if 'benchmark_year' in self.dataset.dimension_keys and 'benchmark_year' not in code_list:
            code_list['benchmark_year'] = {}
        return code_list
        
    def get_dimension_data(self,filename,fmt):
        """Parses code file for one dimension
        Returns a dict
        """
        download = Downloader(url=self.dataset_url + filename,
                              filename = filename,
                              store_filepath = self.store_path,
                              use_existing_file = self.fetcher.use_existing_file)
        filepath = download.get_filepath()
        entries1 = {}
        entries2 = {}
        with open(filepath) as source_file:
            data = csv.reader(source_file,delimiter='\t')
            fields = next(data)
            if fmt == 1:
                for row in data:
                    entries1[row[0]] = row[1]
            elif fmt == 2:
                for row in data:
                    entries1[row[1]] = row[2]
            elif fmt == 3:
                for row in data:
                    entries1[row[0]] = row[2]
                    entries2[row[1]] = row[1]
            elif fmt == 4:
                for row in data:
                    entries1[row[0]] = row[0]
                    entries2[row[1]] = row[3]
            else:
                raise Exception("fmt {} doesn't exist".format(fmt))
        return (entries1, entries2)
    
    def get_data_filenames(self,directory):
        """Determines the list of data files
        Returns a list
        """
        included_files = self.dataset_code + '\.data\.'
#        excluded_files_0 = self.dataset_code + '\.data\.(1|2)\.'
        files =  [d
                  for d in self.data_directory
                  if re.match(included_files,d)] #and (not re.match(excluded_files_0,d))]
        return files

    def get_series_filepath(self):
        """Parse series file for a dataset
        Returns a dict of dict
        """
        filename = self.dataset_code + '.series'
        download = Downloader(url=self.dataset_url + filename,
                              filename = filename,
                              store_filepath = self.store_path,
                              use_existing_file = self.fetcher.use_existing_file)
        return download.get_filepath()

    def get_series_fields(self,filepath):
        with open(filepath) as source_file:
            row_iterator = csv.reader(source_file,delimiter='\t')
            # ip.series has type_code, (comma at the end!)
            return [f.replace('_codes','').replace('_code','').replace(',','') for f in next(row_iterator)]        
            
    def get_series_iterator(self,filepath):
        """Parse series file for a dataset
        Iterates a dict
        """
        with open(filepath) as source_file:
            row_iterator = csv.reader(source_file,delimiter='\t')
            #skip header row
            next(row_iterator)
            for row in row_iterator:
                series = {k.strip(): v.strip() for k,v in zip(self.series_fields,row)}
                yield series
    
    def get_data_iterators(self):
        iterators = []
        for filename in self.get_data_filenames(self.data_directory):
            iterators.append(SeriesIterator(self.dataset_url + filename,
                                            filename,
                                            self.store_path,
                                            self.fetcher.use_existing_file))
        return iterators

    def get_release_date(self):
        """Sets the release date from the date of the datafile
        Return a datetime
        """
        for k,dd in self.data_directory.items():
            if re.match(self.dataset_code + '\.data\.0\.',k):
                break
        return datetime(dd['year'],dd['month'],dd['day'],dd['hour'],dd['minute'])
    
    def available_series_init(self):
        available_series = []
        for i in self.data_iterators:
            try:
                available_series.append(next(i))
            except StopIteration:
                available_series.append(None)
        return available_series
    
    def get_series(self,id,start_period,end_period,case):
        series = None
        for i,a in enumerate(self.available_series):
            if a is not None and a['series_id'] == id:
                OK = False
                if case == 0:
                    if (a['start_period'] <= start_period) and a['end_period'] >= end_period:
                        OK = True
                elif case == 1:
                    if (a['start_period_annual'] <= start_period) and a['end_period'] >= end_period:
                        OK = True
                elif case == 2:
                    if (a['start_period'] <= start_period) and a['end_period_annual'] >= end_period:
                        OK = True
                elif case == 3:
                    if (a['start_period_annual'] <= start_period) and a['end_period_annual'] >= end_period:
                        OK = True
                if OK:
                    series = a
                try:
                    self.available_series[i] = next(self.data_iterators[i])
                except StopIteration:
                    self.available_series[i] = None
        if series is None:
            raise Exception('Series {} not found'.format(id))
        else:
            return series
        
    def update_dimensions(self,dims):
        for f in self.dataset.dimension_keys:
            if f == 'base_period':
                if dims['base_period'] not in self.dataset.dimension_list['base_period']:
                    self.dataset.dimension_list['base_period'][dims[f]] = dims[f]
                    self.dataset.codelists['base_period'][dims[f]] = dims[f]
                    self.dataset.concepts['base_period'][dims[f]] = dims[f]
            elif f == 'base_date':
                if dims['base_date'] not in self.dataset.dimension_list['base_date']:
                    self.dataset.dimension_list['base_date'][dims[f]] = dims[f]
                    self.dataset.codelists['base_date'][dims[f]] = dims[f]
                    self.dataset.concepts['base_date'][dims[f]] = dims[f]
            elif f == 'base_year':
                if dims['base_year'] not in self.dataset.dimension_list['base_year']:
                    self.dataset.dimension_list['base_year'][dims[f]] = dims[f]
                    self.dataset.codelists['base_year'][dims[f]] = dims[f]
                    self.dataset.concepts['base_year'][dims[f]] = dims[f]
            elif f == 'benchmark_year':
                if dims['benchmark_year'] not in self.dataset.dimension_list['benchmark_year']:
                    self.dataset.dimension_list['benchmark_year'][dims[f]] = dims[f]
                    self.dataset.codelists['benchmark_year'][dims[f]] = dims[f]
                    self.dataset.concepts['benchmark_year'][dims[f]] = dims[f]
            else:
                if dims[f] not in self.dataset.dimension_list:
                    if dims[f] == '':
                        self.code_list[f][''] = 'None'                                          
                        self.dataset.dimension_list[f][''] = 'None'
                        self.dataset.codelists[f][''] = 'None'
                        self.dataset.concepts[f][''] = 'None'
                    else:
                        self.dataset.dimension_list[f][dims[f]] = self.code_list[f][dims[f]]
                        self.dataset.codelists[f][dims[f]] = self.code_list[f][dims[f]]
                        self.dataset.concepts[f][dims[f]] = self.code_list[f][dims[f]]
        
    def __next__(self):
        """Sets next series bson
        Returns bson
        """
        # an annual series is waiting to be sent
        if self.annual_series:
            self.dataset.add_frequency('A')
            bson = self.annual_series
            self.annual_series = None
            return bson

        series_dims = next(self.series_iter)
        self.update_dimensions(series_dims)

        frequency = series_dims['begin_period'][0]
        self.dataset.add_frequency(frequency)
        start_date = get_date(series_dims['begin_year'], series_dims['begin_period'], frequency)[0]
        end_date = get_date(series_dims['end_year'], series_dims['end_period'], frequency)[0]
        case = 0
        if series_dims['begin_period'] == 'M13' or series_dims['begin_period'] == 'Q05' or series_dims['begin_period'] == 'S03':
            start_period = get_ordinal_from_year_subperiod(series_dims['begin_year'], None, freq='A')
            case += 1
        else:
            start_period = get_ordinal_from_year_subperiod(series_dims['begin_year'], series_dims['begin_period'][1:], freq=frequency)
        if series_dims['end_period'] == 'M13' or series_dims['end_period'] == 'Q05' or series_dims['end_period'] == 'S03':
            end_period = get_ordinal_from_year_subperiod(series_dims['end_year'], None, freq='A')
            case += 2
        else:
            end_period = get_ordinal_from_year_subperiod(series_dims['end_year'], series_dims['end_period'][1:], freq=frequency)
        s = self.get_series(series_dims['series_id'],start_period,end_period,case)
        # update attribute codes
        for f in s['footnote_list']:
            self.dataset.attribute_list['footnote'][f.upper()] = self.code_list['footnote'][f.upper()]                                                                            
            self.dataset.codelists['footnote'][f.upper()] = self.code_list['footnote'][f.upper()]
            self.dataset.concepts['footnote'][f.upper()] = self.code_list['footnote'][f.upper()]
            
        if 'series_title' in self.series_fields:
            name = series_dims['series_title']
        elif 'series_name' in self.series_fields:
            name = series_dims['series_name']
        else:
            name = '-'.join(
                self.dataset.dimension_list[f][series_dims[f]]
                for f in self.dataset.dimension_keys)
        bson = {} 
        bson['values'] = s['values']                
        bson['provider_name'] = self.provider_name       
        bson['dataset_code'] = self.dataset_code
        bson['name'] = name
        bson['key'] = series_dims['series_id']
        bson['start_date'] = s['start_period']
        bson['end_date'] = s['end_period']
        bson['start_ts'] = s['start_ts']
        bson['end_ts'] = s['end_ts']
        bson['last_update'] = self.release_date
        bson['dimensions'] = {d:series_dims[d] for d in self.dataset.dimension_keys} 
        bson['frequency'] = s['frequency']
        bson['attributes'] = None
        # put series footnote in bson['notes'] if any
        if len(s['footnote_list']) > 0:
            bson['notes'] = '\n'.join(s['footnote_list'])

        if len(s['values_annual']) > 0:
            if s['start_period_annual'] is None:
                   raise Exception('empty start')
            self.dataset.add_frequency('A')
            bson_annual = copy.copy(bson)
            bson_annual['values'] = s['values_annual']                
            bson_annual['name'] = bson['name'] + ' - annual avg.'
            bson_annual['key'] = bson['key'] + 'annual'
            bson_annual['start_date'] = s['start_period_annual']
            bson_annual['end_date'] = s['end_period_annual']
            bson_annual['start_ts'] = s['start_ts_annual']
            bson_annual['end_ts'] = s['end_ts_annual']
            bson_annual['frequency'] = 'A'

        if len(bson['values']) > 0: 
            if start_period < self.start_date:
                self.start_date = start_period
            if end_period > self.end_date:
                self.end_date = end_period
            if len(s['values_annual']) > 0:
                self.annual_series = bson_annual
            return bson
        else:
            return bson_annual

