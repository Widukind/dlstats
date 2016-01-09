# -*- coding: utf-8 -*-

from dlstats.fetchers._commons import (Fetcher, Series, Datasets, Providers)
import urllib
import xlrd
import csv
import codecs
from datetime import datetime
import pandas
from collections import OrderedDict
from re import match, sub
import time
import sdmx
import requests
from dlstats import constants
from lxml.etree import XMLSyntaxError
import logging

VERSION = 1

logger = logging.getLogger(__name__)

class ECB(Fetcher):
    def __init__(self, db=None):
        super().__init__(provider_name='ECB', db=db)
        self.provider_name = 'ECB'
        self.requests_client = requests.Session()
        sdmx.ecb.requests_client = self.requests_client
        self.provider = Providers(name=self.provider_name, 
                                  long_name='European Central Bank',
                                  version=VERSION,
                                  region='Europe',
                                  website='http://www.ecb.europa.eu/',
                                  fetcher=self)
        self.selected_codes = ['ecb_root']
        self.selected_datasets = {}
        
    def get_categories(self):
        return sdmx.ecb.categories

    def build_data_tree(self):
        def walk_category(category):
            in_base_category = {}
            if 'flowrefs' in category:
                children_ = []
                for flowref in category['flowrefs']:
                    dataflow_info = sdmx.ecb.dataflows(flowref)
                    key_family = list(dataflow_info.keys())[0]
                    name = dataflow_info[key_family][2]['en']
                    in_base_category_ = {
                        'name': name,
                        'category_code': flowref,
                        'children': [],
                        'doc_href': None,
                        'last_update': None,
                        'exposed': False}
                    children_.append(in_base_category_)
                in_base_category = {
                    'name': category['name'],
                    'category_code': category['name'],
                    'children': children_,
                    'doc_href': None,
                    'last_update': None,
                    'exposed': False}
            if 'subcategories' in category:
                children_ = []
                for subcategory in category['subcategories']:
                    child = walk_category(subcategory)
                    if child:
                        children_.append(child)
                in_base_category = {
                    'name': category['name'],
                    'category_code': category['name'],
                    'children': children_,
                    'doc_href': None,
                    'last_update': None,
                    'exposed': False}
            return in_base_category

        data_tree_ = walk_category(self.get_categories())
        data_tree = {'name': 'ECB',
                     'doc_href': None,
                     'children': data_tree_['children'],
                     'category_code': 'ecb_root',
                     'exposed': False,
                     'last_update': None}
        return data_tree
    
    def upsert_categories(self):
        data_tree = self.build_data_tree()
        self.provider.add_data_tree(data_tree)
        
    def get_selected_datasets(self):
        """Collects the datasets
        :returns: dict of datasets"""

        def walktree1(node,selected):
            if selected or (node['category_code'] in self.selected_codes):
                selected = True
                if len(node['children']) == 0:
                    # this is a leaf
                    node['exposed'] = True
                    self.selected_datasets.update({node['category_code']: node})

            for child in node['children']:
                walktree1(child,selected)

        provider = self.db[constants.COL_PROVIDERS].find_one({'name': self.provider_name},{'data_tree': 1})
        if provider is None:
            self.upsert_categories()
            provider = self.db[constants.COL_PROVIDERS].find_one({'name': self.provider_name},{'data_tree': 1})

        walktree1(provider['data_tree'],True)
        
    def upsert_selected_datasets(self):
        if self.selected_datasets is None:
            self.get_selected_datasets()
        for d in self.selected_datasets:
            self.upsert_dataset(d)

    def datasets_list(self):
        datasets = self.selected_datasets
        if not datasets:
            self.get_selected_datasets()
        return [d for d in self.selected_datasets]

    def datasets_long_list(self):
        datasets = self.selected_datasets
        if not datasets:
            self.get_selected_datasets()
        return [(d[0],d[1]['name']) for d in self.selected_datasets.items()]

    def upsert_dataset(self, dataset_code):
        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        if not self.selected_datasets:
            self.get_selected_datasets()
        cat = self.selected_datasets[dataset_code]
        dataset = Datasets(self.provider_name,
                           dataset_code,
                           fetcher=self,
                           last_update=datetime.now(),
                           doc_href=cat['doc_href'], name=cat['name'])
        ecb_data = ECBData(dataset)
        dataset.series.data_iterator = ecb_data
        dataset.update_database()
        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))

    def datasets_list(self):
        dataset_codes = self.db[constants.COL_CATEGORIES].find({'provider_name': 'ECB', 'children': None},{'category_code':True, '_id': False})
        return [dataset_code['category_code'] for dataset_code in dataset_codes]

    def datasets_long_list(self):
        dataset_codes = self.db[constants.COL_CATEGORIES].find({'provider_name': 'ECB', 'children': None},{'category_code':True, 'name': True, '_id': False})
        return [(dataset_code['category_code'], dataset_code['name']) for dataset_code in dataset_codes]

    def upsert_all_datasets(self):
        start = time.time()
        logger.info("update fetcher[%s] - START" % (self.provider_name))
        dataset_codes = self.db[constants.COL_CATEGORIES].find({'provider_name': 'ECB', 'children': None},{'category_code':True, '_id': False})
        dataset_codes = [dataset_code['category_code'] for dataset_code in dataset_codes]
        for dataset_code in dataset_codes:
            self.upsert_dataset(dataset_code)
        end = time.time() - start
        logger.info("update fetcher[%s] - END - time[%.3f seconds]" % (self.provider_name, end))


class ECBData(object):
    def __init__(self, dataset):
        self.provider_name = 'ECB'
        self.dataset = dataset
        self.dataset_code = self.dataset.dataset_code
        self.key_family = list(sdmx.ecb.dataflows(self.dataset_code).keys())[0]
        self.key_family = sub('ECB_', '', self.key_family)
        self.codes = sdmx.ecb.codes(self.key_family)
        self.dimension_list = self.dataset.dimension_list
        self.dimension_list.set_dict(self.codes)
        self.largest_dimension = self._largest_dimension()
        self.codes_to_process = list(self.codes[self.largest_dimension[0]].keys())
        self.keys_to_process = []

    def _largest_dimension(self):
        counter = ('',0)
        for key in self.codes.keys():
            size_of_code = len(self.codes[key])
            if size_of_code > counter[1]:
                counter = (key,size_of_code)
        return counter

    def __iter__(self):
        return self

    def __next__(self):
        if self.keys_to_process == []:
            if self.codes_to_process == []:
                raise StopIteration()
            else:
                self.current_raw_data = None
                attempts = 0
                code = self.codes_to_process.pop()
                while attempts < 3 and self.current_raw_data == None:
                    try:
                        #time.sleep(600*attempts+10)
                        attempts += 1
                        self.current_raw_data = sdmx.ecb.raw_data(self.dataset_code, {self.largest_dimension[0]:code})
                    except XMLSyntaxError as e:
                        exception = e
                if self.current_raw_data == None:
                    raise e
                self.keys_to_process = list(self.current_raw_data[0].keys())
                if self.keys_to_process == []:
                    self.__next__()
        current_key = self.keys_to_process.pop()
        series = dict()
        series['provider_name'] = self.provider_name
        series['dataset_code'] = self.dataset_code
        series['key'] = current_key
        series['name'] = "-".join([self.current_raw_data[3][current_key][key]
                                  for key in self.current_raw_data[3][current_key]])
        series['values'] = self.current_raw_data[0][current_key]
        series['frequency'] = self.current_raw_data[3][current_key]['FREQ']
        if series['frequency'] == 'W':
            start_date = self.current_raw_data[1][current_key][0].split('-W')
            end_date = self.current_raw_data[1][current_key][-1].split('-W')
            series['start_date'] = pandas.Period(
                year=int(start_date[0]),
                freq=series['frequency']
            ).ordinal + int(start_date[1])
            series['end_date'] = pandas.Period(
                year=int(end_date[0]),
                freq=series['frequency']
            ).ordinal + int(end_date[1])
        else:
            series['start_date'] = pandas.Period(
                self.current_raw_data[1][current_key][0],
                freq=series['frequency']
            ).ordinal
            series['end_date'] = pandas.Period(
                self.current_raw_data[1][current_key][-1],
                freq=series['frequency']
            ).ordinal
        series['attributes'] = {}
        series['dimensions'] = dict(self.current_raw_data[3][current_key])
        return(series)

if __name__ == '__main__':
    ecb = ECB()
    ecb.upsert_categories()
    ecb.upsert_dataset('2034476')
    #ecb.upsert_all_datasets()
