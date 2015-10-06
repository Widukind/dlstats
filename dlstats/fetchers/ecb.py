# -*- coding: utf-8 -*-

from ._commons import Fetcher, Category, Series, Dataset, Provider, CodeDict
from .make_elastic_index import ElasticIndex
import urllib
import xlrd
import csv
import codecs
from datetime import datetime
import pandas
import pprint
from collections import OrderedDict
from re import match
from time import sleep
import sdmx


class ECB(Fetcher):
    def __init__(self):
        super().__init__(provider_name='ECB') 
        self.provider_name = 'ECB'
        self.provider = Provider(name=self.provider_name,website='http://www.imf.org/')
        
    def upsert_categories(self):
        categories = sdmx.ecb.categories
        def walk_category(category):
            children_ids = []
            if 'flowrefs' in category:
                children_ids_ = []
                for flowref in category['flowrefs']:
                    dataflow_info = sdmx.ecb.dataflows(flowref)
                    key_family = list(dataflow_info.keys())[0]
                    name = dataflow_info[key_family][2]['en']
                    in_base_category_ = Category(provider='ECB',name=name,
                                                categoryCode=key_family,
                                                children=None,
                                                docHref=None,
                                                lastUpdate=datetime(2014,12,2),
                                                exposed=True)
                    children_ids_.append(in_base_category_.update_database())
                in_base_category = Category(provider='ECB',name=category['name'],
                                            categoryCode=category['name'],
                                            children=children_ids_,
                                            docHref=None,
                                            lastUpdate=datetime(2014,12,2),
                                            exposed=True)
            if 'subcategories' in category:
                for subcategory in category['subcategories']:
                    children_ids.append(walk_category(subcategory))
                in_base_category = Category(provider='ECB',name=category['name'],
                                            categoryCode=category['name'],
                                            children=children_ids,
                                            docHref=None,
                                            lastUpdate=datetime(2014,12,2),
                                            exposed=True)
            try:
                return in_base_category.update_database()
            except NameError:
                pass
        walk_category(categories)

    def upsert_dataset(self, dataset_code):
        dataset = Dataset(self.provider_name,dataset_code)
        cat = self.db.categories.find_one({'categoryCode': dataset_code})
        ecb_data = ECBData(dataset_code)
        dataset.name = cat['name']
        dataset.doc_href = cat['docHref']
        dataset.last_update = cat['lastUpdate']
        dataset.series.data_iterator = ecb_data
        dataset.update_database()


class ECBData(object):
    def __init__(self,dataset_code):
        self.provider_name = 'ECB'
        self.dataset_code = dataset_code
        self.key_family = list(sdmx.ecb.dataflows(dataset_code).keys())[0]
        self.codes = sdmx.ecb.codes(self.key_family)
        self.raw_data = sdmx.ecb.raw_data(self.dataset_code,{})
        self._keys_to_process = list(self.raw_data[0].keys())
        
    def __iter__(self):
        return self

    def __next__(self):
        current_key = self._keys_to_process.pop()
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
    ecb.upsert_categories()
    #ecb.upsert_dataset('2034468')
