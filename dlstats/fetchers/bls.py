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
        
        return dataset.update_database()

    # TO BE DONE
    def _parse_agenda(self):
        pass
    
class SeriesIterator:
    def __init__(self,url,filename,store_path,use_existing_file):
        self.row_iter = self.iter_row(url,filename,store_path,use_existing_file)
        self.current_row = None
        
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

    def get_date(self,year,subperiod,frequency):
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
    
    def get_value(self,row,period):
        """Forms one value dictionary
        Returns a dict
        """
        if len(row[4]) > 0:
            attribute = {'footnote': row[4]}
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
        values = []
        values_annual = []
        previous_period = None
        previous_period_annual = None
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
            start_date_annual = self.get_date(row[1],None,'A')[0]
            period_annual = get_ordinal_from_period(row[1],freq='A')
            start_period_annual = period_annual
        else:
            frequency = row[2][0]
            start_date_annual = self.get_date(row[1],row[2],frequency)[0]
            period = get_ordinal_from_period(start_date_annual,freq=frequency)
            start_period = period
        while len(row) > 0 and row[0] == series_id:    
            if row[2] == 'M13' or row[2] == 'S03':
                start_date_annual = self.get_date(row[1],None,'A')[0]
                period_annual = get_ordinal_from_period(row[1],freq='A')
                if previous_period_annual is not None and previous_period_annual + 1 < period_annual:
                    annual_values = self.fill_series(values_annual,previous_period_annual,period_annual)
                values_annual.append(self.get_value(row,period))
                previous_period_annual = period 
            else:
                frequency = row[2][0]
                start_date = self.get_date(row[1],row[2],frequency)[0]
                period = get_ordinal_from_period(start_date,freq=frequency)
                if previous_period is not None and previous_period + 1 < period:
                    values = self.fill_series(values,previous_period,period)
                values.append(self.get_value(row,period))
                previous_period = period
            row = [elem.strip() for elem in next(self.row_iter)]
        return({
            'frequency': frequency,
            'values': values,
            'values_annual': values_annual,
            'start_period': start_period,
            'end_period': period,
            'start_period_annual': start_period_annual,
            'end_period_annual': period_annual,
        })
    
class BlsData:
    
    def __init__(self, dataset,  url):
        self.dataset = dataset
        self.dataset_url = url
        self.fetcher = self.dataset.fetcher
        
        self.provider_name = self.dataset.provider_name
        self.dataset_code = self.dataset.dataset_code
        self.dimension_list = self.dataset.dimension_list
        self.attribute_list = ['footnote']
        
        self.store_path = self.get_store_path()
        self.data_directory = self.get_data_directory()
        self.data_filenames = self.get_data_filenames(self.data_directory)
        self.data_iterators = self.get_data_iterators()
        self.code_list = self.get_code_list()
        self.series = self.get_series()
        self.current_row = None
        self.bson_s03 = None
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
        
    def get_code_list(self):
        """Gets all code lists in a dataset directory
        Returns a dict of dict of dict
        """
        code_list = {k.split('.',1)[1]: self.get_dimension_data(k) 
                     for k in self.data_directory if not k.endswith(('.txt','.contacts','.series')) and not '.data.' in k}
        # dimensions that often don't have a code file
        if 'seasonal' not in code_list:
            code_list['seasonal'] = {'S': 'Seasonaly adjusted', 'U': 'Unadjusted'}
        if 'base_period' not in code_list:
            code_list['base_period'] = {}
        self.dataset.codelists = code_list
        self.dataset.concepts = code_list
        self.dataset.dimension_keys = [k for k in code_list.keys()]
        return code_list
        
    def get_dimension_data(self,dimension):
        """Parses code file for one dimension
        Returns a dict
        """
        filename = dimension
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
    
    def get_series(self):
        """Parse series file for a dataset
        Returns a dict of dict
        """
        filename = self.dataset_code + '.series'
        download = Downloader(url=self.dataset_url + filename,
                              filename = filename,
                              store_filepath = self.store_path,
                              use_existing_file = self.fetcher.use_existing_file)
        filepath = download.get_filepath()
        series = {}
        with open(filepath) as source_file:
            data = csv.reader(source_file,delimiter='\t')
            self.series_fields = next(data)
            for row in data:
                series[row[0].strip()] = {k.strip(): v.strip() for k,v in zip(self.series_fields,row)}
        return series
    
    def get_data_iterators(self):
        iterators = {} 
        for filename in self.get_data_filenames(self.data_directory):
            iterators[filename] = SeriesIterator(self.dataset_url + filename,
                                                 filename,
                                                 self.store_path,
                                                 self.fetcher.use_existing_file)
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

    def form_date(self,year,subperiod,frequency):
        if frequency == 'A':
            date_string = year
        elif frequency == 'S':
            if subperiod == 'S03':
                date_string = year
                frequency = 'A'
            else:
                date_string = year + 'S' + subperiod[-1]
        else:
            date_string = year + '-' + subperiod
        return (date_string, frequency)
    
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
        """Sets next series bson
        Returns bson
        """
        print(self.data_iterators)
        for f,it in self.data_iterators.items():
            print(it)
            for row in it:
                print(row)
        # an annual series is waiting to be sent
        if self.bson_s03:
            bson = self.bson_s03
            self.bson_s03 = None
            return bson
        
        while True:
            if self.current_row is not None:
                row = self.current_row
                self.current_row = None
            else:
                row = next(self.data_iter)
            row = [r.strip() for r in row]
            if len(row) == 0:
                break
            if row[2] == 'M13':
                row = next(self.data_iter)
                continue
            last_period = row[1:3]
            series_id = row[0]
            dims = self.series[series_id]
            frequency = self.get_frequency(dims['periodicity_code'])
            self.dataset.add_frequency(frequency)
            start_date = self.form_date(dims['begin_year'], dims['begin_period'], frequency)
            start_ts = self.get_start_ts(dims['begin_year'], dims['begin_period'], frequency)
            effective_start_date = self.form_date(row[1], row[2], frequency)[0]
            period = get_ordinal_from_period(effective_start_date,freq=frequency)
            start_period = period
            values = []
            if frequency == 'S':
                values_s03 = []
                effective_start_date_s03 = self.form_date(row[1], None, 'A')[0]
                start_ts_s03 = self.get_start_ts(dims['begin_year'], None, 'A')
                period_s03 = get_ordinal_from_period(effective_start_date_s03,freq='A')

            while (len(row) > 0) and (row[0] == dims['series_id']):
                if row[2] == 'M13':
                    row = next(self.data_iter)
                    continue
                if row[2] == 'S03':
                    if len(row[4]) > 0:
                        attribute = {'footnote': row[4].strip()}
                    else:
                        attribute = None
                    value = {
                        'attributes': attribute,
                        'period': str(period_s03),
                        'value': row[3],
                    }
                    period_s03 += 1
                    values_s03.append(value)
                    last_period_s03 = row[1:3]
                else:
                    period1 = get_ordinal_from_period(self.form_date(row[1], row[2], frequency)[0],freq=frequency)
                    while period1 < period:
                        values.append({
                            'attributes': None,
                            'period': str(period1),
                            'value': 'nan',
                        })
                        period1 += 1
                    if len(row[4]) > 0:
                        attribute = {'footnote': row[4].strip()}
                    else:
                        attribute = None
                    value = {
                        'attributes': attribute,
                        'period': str(period),
                        'value': row[3],
                    }
                    period += 1
                    values.append(value)
                    last_period = row[1:3]

                row = next(self.data_iter)
            self.current_row = row

            if dims['end_period'] == 'M13':
                end_date = self.form_date(dims['end_year'],'M12',frequency)[0]
                end_ts = self.get_end_ts(dims['end_year'],'M12','M')
            elif dims['end_period'] == 'S03':
                end_date = self.form_date(dims['end_year'],'S02',frequency)[0]
                end_ts = self.get_end_ts(dims['end_year'],'S02','S')
                end_ts_s03 = self.get_end_ts(dims['end_year'],None,'A')
            else:
                end_date = self.form_date(dims['end_year'],dims['end_period'],frequency)[0]
                end_date_ts = self.get_end_ts(dims['end_year'],dims['end_period'],frequency)
            end_period = get_ordinal_from_period(end_date,freq=frequency)
            effective_end_date = self.form_date(last_period[0], last_period[1], frequency)
            if start_date == effective_start_date and end_date == effective_end_date:
                break
        dimension_names = [
            f
            for f in self.series_fields
            if f not in ['series_id','footnote_codes','begin_year', 'end_year', 'begin_period', 'end_period']]

        base_period = dims['base_period']
        if base_period not in self.code_list['base_period']:
            self.code_list['base_period'][base_period] = base_period 
        if 'series_title' in self.series_fields:
            name = dims['series_title']
        else:
            name = '-'.join(
                self.code_list[f.replace('_code','')][dims[f]]
                for f in dimension_names)
        if frequency == 'S':
            if 'series_title' in self.series_fields:
                name_03 = dims['series_title']
            else:
                dims['periodicity_code'] = 'A'
                name_03 = '-'.join(
                    self.code_list[f.replace('_code','')][dims[f]]
                    for f in dimension_names)

            bson_s03 = {} 
        
            bson_s03['values'] = values_s03                
            bson_s03['provider_name'] = self.provider_name       
            bson_s03['dataset_code'] = self.dataset_code
            bson_s03['name'] = name
            # fix frequency in series ID
            bson_s03['key'] = dims['series_id'] + '-YEAR'
            bson_s03['start_date'] = get_ordinal_from_period(effective_start_date_s03,freq='A')
            bson_s03['end_date'] = get_ordinal_from_period(last_period_s03[0],freq='A')
            bson_s03['start_ts'] = start_ts_s03
            bson_s03['end_ts'] = end_ts_s03
            bson_s03['last_update'] = self.release_date
            bson_s03['dimensions'] = {d:dims[d] for d in dimension_names} 
            bson_s03['frequency'] = 'A'
            bson_s03['attributes'] = None
            self.bson_s03 = bson_s03
        bson = {} 
        bson['values'] = values                
        bson['provider_name'] = self.provider_name       
        bson['dataset_code'] = self.dataset_code
        bson['name'] = name
        bson['key'] = dims['series_id']
        bson['start_date'] = start_period
        bson['end_date'] = end_period
        bson['start_ts'] = start_ts
        bson['end_ts'] = end_ts
        bson['last_update'] = self.release_date
        bson['dimensions'] = {d:dims[d] for d in dimension_names} 
        bson['frequency'] = frequency
        bson['attributes'] = None

        if start_period < self.start_date:
            self.start_date = start_period
        if end_period > self.end_date:
            self.end_date = end_period
        return bson

