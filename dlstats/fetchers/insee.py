# -*- coding: utf-8 -*-

from dlstats.fetchers._commons import Fetcher, Category, Dataset, Provider, CodeDict
import urllib
import xlrd
import csv
import codecs
from datetime import datetime
import pandas
from collections import OrderedDict
import sdmx
import bson

class INSEE(Fetcher):
    def __init__(self, db=None, es_client=None):
        super().__init__(provider_name='INSEE',
                         db=db, 
                         es_client=es_client)
        self.provider_name = 'INSEE'
        self.provider = Provider(name=self.provider_name,
                                 long_name='National Institute of Statistics and Economic Studies',
                                 region='France',
                                 website='http://www.insee.fr',
                                 fetcher=self)

    @property
    def categories(self):
        return sdmx.insee.categories

    @property
    def categorisation(self):
        return sdmx.insee.categorisation

    @property
    def dataflows(self):
        return sdmx.insee.dataflows()
        
    def upsert_categories(self):
        
        def walk_category(category,categorisation,dataflows,name=None,categoryCode=None):
            if name is None:
                name = category['name']
            if categoryCode is None:
                categoryCode = category['id']
            children_ids = []
            if 'subcategories' in category:
                for subcategory in category['subcategories']:
                    children_ids.append(walk_category(subcategory,categorisation,dataflows))
                in_base_category = Category(provider=self.provider_name,
                                            name=name,
                                            categoryCode=categoryCode,
                                            children=children_ids,
                                            docHref=None,
                                            lastUpdate=datetime.now(),
                                            exposed=False,
                                            fetcher = self)
                res = in_base_category.update_database()
                return bson.objectid.ObjectId(res.upserted_id)
                try:
                    return bson.objectid.ObjectId(res.upserted_id)
                except NameError:
                    pass
            else:
                for df_id in categorisation[category['id']]:
                    in_base_category = Category(provider=self.provider_name,
                                                name=dataflows[df_id][2]['en'],
                                                categoryCode=category['id'],
                                                children=children_ids,
                                                docHref=None,
                                                lastUpdate=datetime.now(),
                                                exposed=False,
                                                fetcher = self)
                    res = in_base_category.update_database()
                    return bson.objectid.ObjectId(res.upserted_id)
                    try:
                        return bson.objectid.ObjectId(res.upserted_id)
                    except NameError:
                        pass
        walk_category(self.categories,self.categorisation,self.dataflows,name='root',categoryCode='INSEE_root')

    def upsert_dataset(self, dataset_code):
        dataset = Dataset(self.provider_name,dataset_code,fetcher=self)
        cat = self.db.categories.find_one({'categoryCode': dataset_code})
        ecb_data = ECBData(dataset_code)
        dataset.name = cat['name']
        dataset.doc_href = cat['docHref']
        dataset.last_update = cat['lastUpdate']
        dataset.series.data_iterator = ecb_data
        dataset.update_database()


class INSEE_Data(object):
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
    insee = INSEE()
    insee.upsert_categories()
    #ecb.upsert_dataset('2034468')
