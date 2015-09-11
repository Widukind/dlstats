# -*- coding: utf-8 -*-

from dlstats.fetchers._skeleton import Skeleton, Category, Series, Dataset, Provider, CodeDict
from dlstats.fetchers.make_elastic_index import ElasticIndex
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


class ECB(Skeleton):
    def __init__(self):
        super().__init__(provider_name='ECB') 
        self.provider_name = 'ECB'
        self.provider = Provider(name=self.provider_name,website='http://www.imf.org/')
        
    def upsert_categories(self, id=None):
        categories = sdmx.ecb.categories
        def walk_category(category):
            children_ids = []
            if 'flowrefs' in category:
                in_base_category = Category(provider='ECB',name=category['name'],
                                            categoryCode=category['name'],
                                            children=category['flowrefs'],
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
            return in_base_category.update_database()
        walk_category(categories)

    def upsert_dataset(self, dataset_code):
        dataset = Dataset(self.provider_name,dataset_code)
        cat = self.db.categories.find_one({'categoryCode': dataset_code})
        ecb_data = ECB(dataset,url)
        dataset.name = cat['name']
        dataset.doc_href = cat['docHref']
        dataset.last_update = cat['lastUpdate']
        dataset.series.data_iterator = ecb_data
        dataset.update_database()


class ECBData(object):
    def __init__(self,dataset_code):
        self.provider_name = 'ECB'
        self.dataset_code = dataset_code
        #The slice [4:] removes the agencyID ECB_ in the string
        self.key_familiy = list(sdmx.ecb.dataflows(dataset_code).keys())[0][4:]
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
    ecb.upsert_dataset('2034468')
