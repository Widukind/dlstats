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
from dlstats.fetchers._commons import Fetcher, Datasets, Providers, Categories

VERSION = 1

logger = logging.getLogger(__name__)

FREQUENCIES_SUPPORTED = ["A", "Q", "S", "M"]
FREQUENCIES_REJECTED = []

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

# TO BE FIXED
selected_datasets = {
    'cu': {
        'name': 'All Urban Consumers (Current Series)',
        'metadata': {
            'doc_href': '',
            'url': "https://download.bls.gov/pub/time.series/cu/",
        },
        'last_update': datetime(2017,2,13),
    }
}    

# replaces dlstats.fetchers.utils.get_ordinal_from_period until it is fixed for freq='S'
def get_ordinal_from_period(date_str, freq=None):
    
    from dlstats.cache import cache
    from dlstats import constants
    from pandas import Period
    from dlstats.utils import get_year
    
    key = "ordinal.%s.%s" % (date_str, freq)

    if cache and freq in constants.CACHE_FREQUENCY:
        period_from_cache = cache.get(key)
        if not period_from_cache is None:
            return period_from_cache
    
    period_ordinal = None
    if freq == "A":
        year = int(get_year(date_str))
        period_ordinal = year - 1970
    elif freq == "S":
        year = int(get_year(date_str))
        if date_str.endswith("S1"):
            semester = 1
        elif date_str.endswith("S2"):
            semester = 2
        else:
            raise NotImplementedError("freq not implemented freq[%s] date[%s]" % (freq, date_str))
        period_ordinal = 2*(year - 1970) + semester - 1
        
    if not period_ordinal:
        period_ordinal = Period(date_str, freq=freq).ordinal
    
    if cache and freq in constants.CACHE_FREQUENCY:
        cache.set(key, period_ordinal)
    
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

    # TO BE DONE
    def build_data_tree(self):
        """Build data_tree from BLS site parsing
        """
        
    def upsert_dataset(self, dataset_code):
        """Updates data in Database for selected datasets
        :dset: dataset_code
        :returns: None"""
#        self.get_selected_datasets()
        
        self.dataset_settings = selected_datasets[dataset_code]        
        
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code, 
                           name=self.dataset_settings['name'], 
                           doc_href=self.dataset_settings['metadata']['doc_href'], 
                           last_update=self.dataset_settings['last_update'], 
                           fetcher=self)

        url = self.dataset_settings['metadata']['url']
        dataset.series.data_iterator = BlsData(dataset, url)

        dataset.dimension_keys = dataset.series.data_iterator.dimension_keys
        # reconstruct code_list using codes effectively found in the data
        dataset.code_list = dataset.dimension_list
        dataset.code_list.update(dataset.attribute_list)
        print(dataset.dimension_list.code_dict)
        return dataset.update_database()

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
        end_period_annual = None
        # fetch first row if it is waiting
        if self.current_row is not None:
            row = self.current_row
            self.current_row = None
        else:
            row = [elem.strip() for elem in next(self.row_iter)]
        series_id = row[0]
        if row[2] == 'M13' or row[2] == 'S03':
            frequency = 'A'
            start_date_annual = get_date(row[1],None,'A')[0]
            period_annual = get_ordinal_from_period(row[1],freq='A')
            start_period_annual = period_annual
        else:
            frequency = row[2][0]
            start_date_annual = get_date(row[1],row[2],frequency)[0]
            period = get_ordinal_from_period(start_date_annual,freq=frequency)
            start_period = period
        while len(row) > 0 and row[0] == series_id:    
            if row[2] == 'M13' or row[2] == 'S03':
                start_date_annual = get_date(row[1],None,'A')[0]
                period_annual = get_ordinal_from_period(row[1],freq='A')
                start_period_annual = period_annual
                if previous_period_annual is not None and previous_period_annual + 1 < period_annual:
                    values_annual = self.fill_series(values_annual,previous_period_annual,period_annual)
                values_annual.append(self.get_value(row,period_annual))
                previous_period_annual = period_annual 
            else:
                frequency = row[2][0]
                start_date = get_date(row[1],row[2],frequency)[0]
                period = get_ordinal_from_period(start_date,freq=frequency)
                if previous_period is not None and previous_period + 1 < period:
                    values = self.fill_series(values,previous_period,period)
                values.append(self.get_value(row,period))
                previous_period = period
            try:
                row = [elem.strip() for elem in next(self.row_iter)]
            except:
                self.end_of_file = True
                break
        self.current_row = row
        return({
            'series_id': series_id,
            'frequency': frequency,
            'values': values,
            'values_annual': values_annual,
            'start_period': start_period,
            'end_period': period,
            'start_period_annual': start_period_annual,
            'end_period_annual': period_annual,
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
        self.dimension_keys = self.get_dimension_keys()
        self.attribute_keys = ['footnote']
        self.dimension_list = collections.OrderedDict((k,{}) for k in self.dimension_keys)
        self.attribute_list = collections.OrderedDict((k,{}) for k in self.attribute_keys)
        self.series_iter = self.get_series_iterator(series_filepath)
        self.code_list = self.get_code_list()
        self.available_series = self.available_series_init()
        self.annual_series = None
        self.current_row = None
        self.release_date = self.get_release_date()
        self.last_update = self.release_date

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
                                filename=self.dataset_code + ".html",
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
            directory[filename] = { 'year': splitdate[2],
                                    'month': splitdate[0],
                                    'day': splitdate[1],
                                    'hour': entry[1] + entry[2],
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
        code_list = {k: self.get_dimension_data(self.dataset_code + '.' + k)
                     for k in self.dimension_keys + ['footnote']
                     if k != 'seasonal' and k != 'base_period'}
        # dimensions that often don't have a code file
        if 'seasonal' not in code_list:
            code_list['seasonal'] = {'S': 'Seasonaly adjusted', 'U': 'Unadjusted'}
        if 'base_period' not in code_list:
            code_list['base_period'] = {}
        self.dataset.codelists = code_list
        self.dataset.concepts = code_list
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
        included_directory = self.dataset_code + '.data'
        excluded_directory_0 = self.dataset_code + '.data\..\.'
        return [d
                for d in self.data_directory
                if re.match(included_directory,d) and (not re.match(excluded_directory_0,d))]

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
                if series['begin_period'] == 'M13' or series['begin_period'] == 'S03':
                    series['begin_period'] = None
                if series['end_period'] == 'M13':
                    series['end_period'] = 'M12'
                if series['end_period'] == 'S03':
                    series['end_period'] = 'S02'
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
        dd = self.data_directory[self.dataset_code + '.data.0.Current']
        time = dd['hour'][:-2].split(':')
        if dd['hour'][-2:] == 'PM':
            time[0] = int(time[0])+12
        return datetime(int(dd['year']), int(dd['month']), int(dd['day']), int(time[0]), int(time[1]))

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

    def available_series_init(self):
        available_series = []
        for i in self.data_iterators:
            available_series.append(next(i))
        return available_series
    
    def get_series(self,id,start_period,end_period):
        series = None
        for i,a in enumerate(self.available_series):
            if a is not None and a['series_id'] == id:
                # the series file may not be up to date
                if (a['start_period'] is None or a['start_period'] <= start_period) and a['end_period'] >= end_period:
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
        for f in self.dimension_keys:
            if f == 'base_period':
                if dims['base_period'] not in self.dimension_list['base_period']:
                    self.dimension_list['base_period'][dims[f]] = dims[f]
            else:
                if dims[f] not in self.dimension_list:
                    self.dimension_list[f][dims[f]] = self.code_list[f][dims[f]]
                
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
        start_ts = self.get_start_ts(series_dims['begin_year'], series_dims['begin_period'], frequency)
        end_ts = self.get_end_ts(series_dims['end_year'],series_dims['end_period'],frequency)
        start_date = get_date(series_dims['begin_year'], series_dims['begin_period'], frequency)[0]
        end_date = get_date(series_dims['end_year'], series_dims['end_period'], frequency)[0]
        start_period = get_ordinal_from_period(start_date,freq=frequency)
        end_period = get_ordinal_from_period(end_date,freq=frequency)
        
        s = self.get_series(series_dims['series_id'],start_period,end_period)
        # update attribute codes
        for f in s['footnote_list']:
            self.attribute_list['footnote'][f] = self.code_list['footnote'][f]                                                                            
        
        if 'series_title' in self.series_fields:
            name = series_dims['series_title']
        else:
            name = '-'.join(
                self.dimension_list[f][series_dims[f]]
                for f in self.dimension_keys)
        bson = {} 
        bson['values'] = s['values']                
        bson['provider_name'] = self.provider_name       
        bson['dataset_code'] = self.dataset_code
        bson['name'] = name
        bson['key'] = series_dims['series_id']
        bson['start_date'] = s['start_period']
        bson['end_date'] = s['end_period']
        bson['start_ts'] = start_ts
        bson['end_ts'] = end_ts
        bson['last_update'] = self.release_date
        bson['dimensions'] = {d:series_dims[d] for d in self.dimension_keys} 
        bson['frequency'] = s['frequency']
        bson['attributes'] = None

        if len(s['values_annual']) > 0:
            self.dataset.add_frequency('A')
            start_ts_annual = self.get_start_ts(series_dims['begin_year'],None, 'A')
            end_ts_annual = self.get_end_ts(series_dims['end_year'],None,'A')
            
            bson_annual = copy.copy(bson)
            bson_annual['values'] = s['values_annual']                
            bson_annual['name'] = bson['name'] + ' - annual avg.'
            bson_annual['key'] = bson['key'] + 'annual'
            bson_annual['start_date'] = s['start_period_annual']
            bson_annual['end_date'] = s['end_period_annual']
            bson_annual['start_ts'] = start_ts_annual
            bson_annual['end_ts'] = end_ts_annual
            bson_annual['frequency'] = 'A'
            self.annual_series = bson_annual
                    
        if start_period < self.start_date:
            self.start_date = start_period
        if end_period > self.end_date:
            self.end_date = end_period
        return bson

