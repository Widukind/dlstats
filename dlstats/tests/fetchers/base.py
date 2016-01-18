# -*- coding: utf-8 -*-

from pprint import pprint
from dlstats import constants
import httpretty

from dlstats.tests.base import BaseDBTestCase

#TODO: use tests.utils
def body_generator(filepath):
    '''body for large file'''
    with open(filepath, 'rb') as fp:
        for line in fp:
            yield line        

class BaseFetcherTestCase(BaseDBTestCase):
    
    FETCHER_KLASS = None
    DATASETS = {}
    
    def setUp(self):
        super().setUp()
        self.fetcher = self.FETCHER_KLASS(db=self.db)
        
    def register_url(self, url, filepath, **settings):
        
        default_cfg = dict(status=200, streaming=True, content_type='application/xml')
        
        for it in default_cfg.items():
            settings.setdefault(*it)
    
        httpretty.register_uri(httpretty.GET, 
                               url,
                               body=body_generator(filepath),
                               **settings)

    #TODO:        
    def assertDatasetOK(self, dataset_code):

        query = {
            'provider_name': self.fetcher.provider_name,
            "dataset_code": dataset_code
        }

        dataset = self.db[constants.COL_DATASETS].find_one(query)
        self.assertIsNotNone(dataset)

        ds_settings = self.DATASETS[dataset_code]
        
        #TODO: tests datasets (dimensions, attributes, ...)

    def assertSeriesOK(self, dataset_code):

        query = {
            'provider_name': self.fetcher.provider_name,
            "dataset_code": dataset_code
        }
        
        ds_settings = self.DATASETS[dataset_code]
        
        count = self.db[constants.COL_SERIES].count(query)
        self.assertEqual(ds_settings["series_count"], count)

        series = list(self.db[constants.COL_SERIES].find(query))
        first_series = series[0]
        last_series = series[-1]
        
        #pprint(first_series)
        
        series_list = [(ds_settings["first_series"], first_series), 
                       ((ds_settings["last_series"], last_series))]
        
        for original, target in series_list:
            
            self.assertEqual(original["key"], 
                             target["key"])
    
            self.assertEqual(original["name"], 
                             target["name"])
            
            self.assertEqual(original["frequency"], 
                             target["frequency"])
            
            self.assertEqual(original["first_value"], 
                             target["values"][0])
            
            self.assertEqual(original["last_value"], 
                             target["values"][-1])
            
            """
            TODO: first_date, last_date
            TODO: last_series
            TODO: dimensions, attributes        
            """
        
        
        
