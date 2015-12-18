# -*- coding: utf-8 -*-

from dlstats.fetchers._commons import (Fetcher, Categories,
                                       Series, Datasets, Providers)
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

logger = logging.getLogger(__name__)


class ECB(Fetcher):
    def __init__(self, db=None):
        super().__init__(provider_name='ECB', db=db)
        self.provider_name = 'ECB'
        self.requests_client = requests.Session()
        sdmx.ecb.requests_client = self.requests_client
        self.provider = Providers(name=self.provider_name, long_name='European Central Bank',
                                  region='Europe',
                                  website='http://www.ecb.europa.eu/',
                                  fetcher=self)

    def get_categories(self):
        return sdmx.ecb.categories

    def upsert_categories(self):
        def walk_category(category):
            children_ids = []
            if 'flowrefs' in category:
                children_ids_ = []
                for flowref in category['flowrefs']:
                    dataflow_info = sdmx.ecb.dataflows(flowref)
                    key_family = list(dataflow_info.keys())[0]
                    name = dataflow_info[key_family][2]['en']
                    in_base_category_ = Categories(
                        provider=self.provider_name,
                        name=name,
                        categoryCode=flowref,
                        children=None,
                        docHref=None,
                        lastUpdate=datetime.now(),
                        exposed=True,
                        fetcher=self)
                    children_ids_.append(in_base_category_.update_database())
                in_base_category = Categories(
                    provider=self.provider_name,
                    name=category['name'],
                    categoryCode=category['name'],
                    children=children_ids_,
                    docHref=None,
                    lastUpdate=datetime.now(),
                    exposed=True,
                    fetcher=self)
            if 'subcategories' in category:
                for subcategory in category['subcategories']:
                    id = walk_category(subcategory)
                    if id is not None:
                        children_ids.append(id)
                in_base_category = Categories(
                    provider=self.provider_name,
                    name=category['name'],
                    categoryCode=category['name'],
                    children=children_ids,
                    docHref=None,
                    lastUpdate=datetime.now(),
                    exposed=True,
                    fetcher=self)
            try:
                return in_base_category.update_database()
            except NameError:
                pass
        walk_category(self.get_categories())

    def upsert_dataset(self, dataset_code):
        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        cat = self.db[constants.COL_CATEGORIES].find_one({'provider':self.provider_name, 'categoryCode': dataset_code})
        dataset = Datasets(self.provider_name,
                           dataset_code,
                           fetcher=self,
                           last_update=datetime.now(),
                           doc_href=cat['docHref'], name=cat['name'])
        ecb_data = ECBData(dataset)
        dataset.series.data_iterator = ecb_data
        dataset.update_database()
        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))

    def datasets_list(self):
        dataset_codes = self.db[constants.COL_CATEGORIES].find({'provider': 'ECB', 'children': None},{'categoryCode':True, '_id': False})
        return [dataset_code['categoryCode'] for dataset_code in dataset_codes]

    def datasets_long_list(self):
        dataset_codes = self.db[constants.COL_CATEGORIES].find({'provider': 'ECB', 'children': None},{'categoryCode':True, 'name': True, '_id': False})
        return [(dataset_code['categoryCode'], dataset_code['name']) for dataset_code in dataset_codes]

    def upsert_all_datasets(self):
        start = time.time()
        logger.info("update fetcher[%s] - START" % (self.provider_name))
        dataset_codes = self.db[constants.COL_CATEGORIES].find({'provider': 'ECB', 'children': None},{'categoryCode':True, '_id': False})
        dataset_codes = [dataset_code['categoryCode'] for dataset_code in dataset_codes]
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
        series['provider'] = self.provider_name
        series['datasetCode'] = self.dataset_code
        series['key'] = current_key
        series['name'] = "-".join([self.current_raw_data[3][current_key][key]
                                  for key in self.current_raw_data[3][current_key]])
        series['values'] = self.current_raw_data[0][current_key]
        series['frequency'] = self.current_raw_data[3][current_key]['FREQ']
        if series['frequency'] == 'W':
            start_date = self.current_raw_data[1][current_key][0].split('-W')
            end_date = self.current_raw_data[1][current_key][-1].split('-W')
            series['startDate'] = pandas.Period(
                year=int(start_date[0]),
                freq=series['frequency']
            ).ordinal + int(start_date[1])
            series['endDate'] = pandas.Period(
                year=int(end_date[0]),
                freq=series['frequency']
            ).ordinal + int(end_date[1])
        else:
            series['startDate'] = pandas.Period(
                self.current_raw_data[1][current_key][0],
                freq=series['frequency']
            ).ordinal
            series['endDate'] = pandas.Period(
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
