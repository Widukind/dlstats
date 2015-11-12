# -*- coding: utf-8 -*-

from dlstats.fetchers._commons import Fetcher, Categories, Series, Datasets, Providers
import urllib
import xlrd
import csv
import codecs
from datetime import datetime
import pandas
import pprint
from collections import OrderedDict
from re import match, sub
from time import sleep
import sdmx
import logging

lgr = logging.getLogger('sdmx')
fh = logging.FileHandler('titi.log')
fh.setLevel(logging.DEBUG)
lgr.addHandler(fh)


class ECB(Fetcher):
    def __init__(self, db=None, es_client=None):
        super().__init__(provider_name='ECB', db=db, es_client=es_client ) 
        self.provider_name = 'ECB'
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
                    in_base_category_ = Categories(provider=self.provider_name,name=name,
                                                categoryCode=key_family,
                                                children=None,
                                                docHref=None,
                                                lastUpdate=datetime(2014,12,2),
                                                exposed=True,
                                                fetcher=self)
                    children_ids_.append(in_base_category_.update_database())
                in_base_category = Categories(provider=self.provider_name,name=category['name'],
                                            categoryCode=category['name'],
                                            children=children_ids_,
                                            docHref=None,
                                            lastUpdate=datetime(2014,12,2),
                                            exposed=True,
                                            fetcher=self)
            if 'subcategories' in category:
                for subcategory in category['subcategories']:
                    id = walk_category(subcategory)
                    if id is not None:
                        children_ids.append(id)
                in_base_category = Categories(provider=self.provider_name,name=category['name'],
                                            categoryCode=category['name'],
                                            children=children_ids,
                                            docHref=None,
                                            lastUpdate=datetime(2014,12,2),
                                            exposed=True,fetcher=self)
            try:
                return in_base_category.update_database()
            except NameError:
                pass
        walk_category(self.get_categories())

    def upsert_dataset(self, dataset_code):
        cat = self.db.categories.find_one({'categoryCode': dataset_code})
        ecb_data = ECBData(dataset_code)
        dataset = Datasets(self.provider_name,dataset_code,last_update=cat['lastUpdate'],
                          doc_href=cat['docHref'],name=cat['name'])
        dataset.series.data_iterator = ecb_data
        dataset.update_database()


class ECBData(object):
    def __init__(self,dataset_code):
        self.provider_name = 'ECB'
        self.dataset_code = dataset_code
        self.key_family = list(sdmx.ecb.dataflows(dataset_code).keys())[0]
        self.key_family = sub('ECB_','',self.key_family)
        self.codes = sdmx.ecb.codes(self.key_family)
        self.largest_dimension = self._largest_dimension()
        self.raw_datas = []
        for code in self.codes[self.largest_dimension[0]]:
            raw_data = sdmx.ecb.raw_data(
                self.dataset_code,{self.largest_dimension[0]:code})
            self.raw_datas.append(raw_data)
            sleep(9)
        self._codes_to_process = -1
        self._keys_to_process = -1

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
        if self.codes_to_process == -1:
            self._codes_to_process = len(self.raw_datas)
        if self._keys_to_process == -1:
            self._keys_to_process = len(self.raw_datas)
        current_key = self._keys_to_process
        self._keys_to_process -= 1
        current_code = self._codes_to_process
        if self._keys_to_process == -1:
            self._codes_to_process -= 1
        raw_data = self.rawdatas[current_code]
        series['provider'] = self.provider_name
        series['datasetCode'] = self.dataset_code
        series['key'] = current_key
        series['name'] = "-".join([self.raw_data[3][current_key][key]
                                  for key in self.raw_data[3]])
        series['values'] = self.raw_data[0][current_key]
        series['frequency'] = self.raw_data[3]['FREQ']
        series['startDate'] = pandas.Period(self.raw_data[1][current_key][0],freq=series['frequency']).ordinal
        series['endDate'] = pandas.Period(self.raw_data[1][current_key][-1],freq=series['frequency']).ordinal
        return(series)

if __name__ == '__main__':
    ecb = ECB()
    #ecb.upsert_categories()
    ecb.upsert_dataset('2034468')
