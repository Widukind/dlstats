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
from time import sleep
import sdmx
import requests
from dlstats import constants


class ECB(Fetcher):
    def __init__(self, db=None, es_client=None):
        super().__init__(provider_name='ECB', db=db, es_client=es_client)
        self.provider_name = 'ECB'
        self.requests_client = requests.Session()
        sdmx.ecb.requests_client = self.requests_client
        self.provider = Providers(name=self.provider_name,
                                  long_name='European Central Bank',
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
        cat = self.db[constants.COL_CATEGORIES].find_one({'provider':self.provider_name, 'categoryCode': dataset_code})
        dataset = Datasets(self.provider_name,
                           dataset_code,
                           fetcher=self,
                           last_update=datetime.now(),
                           doc_href=cat['docHref'], name=cat['name'])
        ecb_data = ECBData(dataset)
        dataset.series.data_iterator = ecb_data
        dataset.update_database()

    def datasets_list(self):
        dataset_codes = self.db[constants.COL_CATEGORIES].find({'provider': 'ECB', 'children': None},{'categoryCode':True, '_id': False})
        return [dataset_code['categoryCode'] for dataset_code in dataset_codes]

    def datasets_long_list(self):
        dataset_codes = self.db[constants.COL_CATEGORIES].find({'provider': 'ECB', 'children': None},{'categoryCode':True, 'name': True, '_id': False})
        return [(dataset_code['categoryCode'], dataset_code['name']) for dataset_code in dataset_codes]

    def upsert_all_datasets(self):
        dataset_codes = self.db[constants.COL_CATEGORIES].find({'provider': 'ECB', 'children': None},{'categoryCode':True, '_id': False})
        dataset_codes = [dataset_code['categoryCode'] for dataset_code in dataset_codes]
        for dataset_code in dataset_codes:
            self.upsert_dataset(dataset_code)


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
        self.raw_datas = []
        for code in self.codes['FREQ']:
            raw_data = sdmx.ecb.raw_data(
                self.dataset_code, {'FREQ': code})
            self.raw_datas.append(raw_data)
            sleep(9)
        self._keys_to_process = -1

    def __iter__(self):
        return self

    def __next__(self):
        if self._keys_to_process == -1:
            if self.raw_datas == []:
                raise StopIteration()
            else:
                self.current_raw_data = self.raw_datas.pop()
                self._keys_to_process = len(self.current_raw_data[0].keys())-1
                if self._keys_to_process == -1:
                    return self.__next__()
        self._keys_to_process -= 1
        current_key = list(self.current_raw_data[2].keys())[self._keys_to_process]
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
        # This is wrong. We should be able to do:
        # series['attributes'] = current_raw_data[2][current_key]
        # It is currently not possible in dlstats.
        series['attributes'] = {}
        series['dimensions'] = dict(self.current_raw_data[3][current_key])
        return(series)

if __name__ == '__main__':
    ecb = ECB()
    ecb.upsert_categories()
    #ecb.upsert_dataset('2034476')
    ecb.upsert_all_datasets()
