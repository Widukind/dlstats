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
    elif freq == "M":
        period_ordinal = 12*(int(year) - 1970) + int(subperiod) - 1
    else:
        raise NotImplementedError("freq not implemented freq[%s] date[%s]" % (freq, date_str))

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

    def get_frequency(self,code):
        """Standardize frequency code
        Returns character code
        """
        if code == 'R':
            code = 'M'
        return code

    def get_value(self,row,period):
        """Forms one value dictionary
        Returns a dict
        """
        if len(row[4]) > 0:
            attribute = {'footnote': row[4]}
            if row[4] not in self.footnote_list:
                self.footnote_list.append(row[4])
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
                ts = datetime(year,6,1)
            elif subperiod == 'S03':
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
                ts = datetime(year,12,31,23,59)
            elif subperiod == 'S02':
                ts = datetime(year,6,30,23,59)
            elif subperiod == 'S03':
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
        values = []
        values_annual = []
        period = None
        period_annual = None
        previous_period = None
        previous_period_annual = None
        start_period = None
        start_period_annual = None
        start_ts = None
        start_ts_annual = None
        end_period_annual = None
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
            if row[2] == 'M13' or row[2] == 'S03':
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
            'start_ts_annual': start_ts,
            'end_ts_annual': end_ts,
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
            if entry[2] == 'PM':
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
            if f not in ['series_id','footnote','begin_year', 'end_year', 'begin_period', 'end_period']]


    def get_code_list(self):
        """Gets all code lists in a dataset directory
        Returns a dict of dict of dict
        """
        filenames = self.dataset.dimension_keys + ['footnote']
        if self.dataset_code == 'ce' and 'data_type' in filenames:
            idx = filenames.index('data_type')
            filenames[idx] = 'datatype'
        code_list = {k: self.get_dimension_data(self.dataset_code + '.' + k)
                     for k in filenames
                     if k != 'seasonal' and k != 'base_period'}
        # dimensions that often don't have a code file
        if 'seasonal' not in code_list:
            code_list['seasonal'] = {'S': 'Seasonaly adjusted', 'U': 'Unadjusted'}
        if 'base_period' not in code_list:
            code_list['base_period'] = {}
        return code_list
        
    def get_dimension_data(self,filename):
        """Parses code file for one dimension
        Returns a dict
        """
        download = Downloader(url=self.dataset_url + filename,
                              filename = filename,
                              store_filepath = self.store_path,
                              use_existing_file = self.fetcher.use_existing_file)
        filepath = download.get_filepath()
        with open(filepath) as source_file:
            data = csv.reader(source_file,delimiter='\t')
            fields = next(data)
            entries = {}
            for row in data:
                entries[row[0]] = row[1]
        if filename == 'periodicity':
            entries['A'] = 'Annual'
        return entries
    
    def get_data_filenames(self,directory):
        """Determines the list of data files
        Returns a list
        """
        included_files = self.dataset_code + '.data'
        excluded_files_0 = self.dataset_code + '.data\.(1|2)\.'
        files =  [d
                  for d in self.data_directory
                  if re.match(included_files,d) and (not re.match(excluded_files_0,d))]
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
            return [f.replace('_codes','').replace('_code','') for f in next(row_iterator)]        
            
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

    def get_frequency(self,code):
        """Standardize frequency code
        Returns character code
        """
        if code == 'R':
            code = 'M'
        return code

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
            available_series.append(next(i))
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
            else:
                if dims[f] not in self.dataset.dimension_list:
                    self.dataset.dimension_list[f][dims[f]] = self.code_list[f][dims[f]]
                    self.dataset.codelists[f][dims[f]] = self.code_list[f][dims[f]]
                    self.dataset.concepts[f][dims[f]] = self.code_list[f][dims[f]]
        
    def __next__(self):
        """Sets next series bson
        Returns bson
        """
        # an annual series is waiting to be sent
        if self.annual_series:
            bson = self.annual_series
            self.annual_series = None
            return bson

        series_dims = next(self.series_iter)
        self.update_dimensions(series_dims)
        # put series footnote in bson['notes'] if any
        if len(series_dims['footnote']) > 0:
            bson['notes'] = self.code_list['footnote'][series_dims['footnote']]
                                           
        frequency = self.get_frequency(series_dims['periodicity'])
        self.dataset.add_frequency(self.frequency)
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
            self.dataset.attribute_list['footnote'][f] = self.code_list['footnote'][f]                                                                            
            self.dataset.codelists['footnote'][f] = self.code_list['footnote'][f]
            self.dataset.concepts['footnote'][f] = self.code_list['footnote'][f]
            
        if 'series_title' in self.series_fields:
            name = series_dims['series_title']
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
            self.annual_series = bson_annual
                    
        if start_period < self.start_date:
            self.start_date = start_period
        if end_period > self.end_date:
            self.end_date = end_period

        return bson

