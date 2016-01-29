# -*- coding: utf-8 -*-

import logging
from pprint import pprint
from dlstats import constants
import httpretty

from dlstats.fetchers._commons import Categories
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
    DEBUG_MODE = False
    DATASET_FIRST = None
    DATASET_LAST = None
    
    def setUp(self):
        super().setUp()
        self.fetcher = self.FETCHER_KLASS(db=self.db)
        self.fetcher.cache_settings = None
        self.is_debug = self.DEBUG_MODE
        if self.is_debug:
            logger = logging.getLogger("dlstats")
            logger.setLevel(logging.DEBUG) 
        
    def register_url(self, url, filepath, **settings):
        
        default_cfg = dict(status=200, 
                           match_querystring=True, 
                           streaming=True, 
                           content_type='application/xml')
        
        for it in default_cfg.items():
            settings.setdefault(*it)
    
        httpretty.register_uri(httpretty.GET, 
                               url,
                               body=body_generator(filepath),
                               **settings)

    def _debug_dataset_concepts(self, dataset):
        print()
        print("-------------- CONCEPTS -------------------------")
        print(list(dataset["concepts"].keys()))
        print("-------------------------------------------------")
    
    def _debug_dataset_codelists(self, dataset):
        print()
        print("-------------- CODELISTS ------------------------")
        print(list(dataset["codelists"].keys()))
        for key in dataset["codelists"].keys():
            if key in dataset["codelists"]:
                print('"%s": %s,' % (key, len(dataset["codelists"][key])))
            else:
                print('"%s": %s,' % (key, "NOT !!!"))
        print("-------------------------------------------------")

    def _debug_dataset_dimension(self, dataset):
        print()
        print("-------------- DIMENSIONS -----------------------")
        print(dataset["dimension_keys"])
        #print(list(dataset["dimension_list"].keys()))
        for key in dataset["dimension_keys"]:
            #print('"%s": %s,' % (key, len(dataset["dimension_list"][key])))
            print('"%s": %s,' % (key, len(dataset["codelists"][key])))
        print("-------------------------------------------------")

    def _debug_dataset_attribute(self, dataset):
        print()
        print("-------------- ATTRIBUTES ------------------------")
        print(dataset["attribute_keys"])
        print(list(dataset["attribute_list"].keys()))
        for key in dataset["attribute_keys"]:
            #print('"%s": %s,' % (key, len(dataset["attribute_list"][key])))
            print('"%s": %s,' % (key, len(dataset["codelists"][key])))
        print("-------------------------------------------------")
    
    def assertProvider(self):
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": self.fetcher.provider_name})
        self.assertIsNotNone(provider)
        
    def assertDataTree(self, dataset_code):

        settings = self.DATASETS[dataset_code]
        dsd = settings["DSD"]
        
        results = self.fetcher.upsert_data_tree()
        
        datasets = self.fetcher.datasets_list()
        
        #if self.is_debug:
        #    print("------DATASET LIST--------")
        #    pprint(datasets)
        
        self.assertEqual(datasets[0]["dataset_code"], self.DATASET_FIRST)
        self.assertEqual(datasets[-1]["dataset_code"], self.DATASET_LAST)
        
        category = Categories.search_category_for_dataset(self.fetcher.provider_name,
                                                          dataset_code, 
                                                          db=self.db)
        self.assertIsNotNone(category)
        self.assertEqual(category["category_code"], dsd["categories_key"])
        
        query = {"provider_name": self.fetcher.provider_name,
                 "datasets.dataset_code": dataset_code}
        
        dataset_category = self.db[constants.COL_CATEGORIES].find_one(query)
        self.assertIsNotNone(dataset_category)
        
        self.assertEqual(dataset_category["all_parents"], 
                         dsd["categories_parents"]) 
        
        roots = Categories.root_categories(self.fetcher.provider_name,
                                           db=self.db)
        
        root_codes = [r["category_code"] for r in roots]
        
        if self.is_debug:
            print("ROOTS : ", root_codes)
        
        self.assertEqual(root_codes,
                         dsd["categories_root"])
        
    def assertDataset(self, dataset_code):

        result = self.fetcher.upsert_dataset(dataset_code)
        self.assertIsNotNone(result)
        
        query = {
            'provider_name': self.fetcher.provider_name,
            "dataset_code": dataset_code
        }

        dataset = self.db[constants.COL_DATASETS].find_one(query)
        self.assertIsNotNone(dataset)
        
        if self.is_debug:
            self._debug_dataset_concepts(dataset)
            self._debug_dataset_codelists(dataset)
            self._debug_dataset_dimension(dataset)
            self._debug_dataset_attribute(dataset)

        dsd = self.DATASETS[dataset_code]["DSD"]
        
        self.assertEqual(len(list(dataset["concepts"].keys())), len(dsd["concept_keys"]))
        self.assertEqual(len(list(dataset["codelists"].keys())), len(dsd["codelist_keys"]))

        if dsd["dimension_keys"]:

            self.assertEqual(dataset["dimension_keys"], dsd["dimension_keys"])
            
            for key in dataset["dimension_keys"]:
                self.assertEqual(len(dataset["codelists"][key]), dsd["dimension_count"][key])
                self.assertTrue(key in dataset["concepts"])

        if dsd["attribute_keys"]:
            
            self.assertEqual(dataset["attribute_keys"], dsd["attribute_keys"])

            for key in dataset["attribute_keys"]:
                self.assertEqual(len(dataset["codelists"][key]), dsd["attribute_count"][key])
                self.assertTrue(key in dataset["concepts"])

    def _debug_series(self, series):
        print()
        print("------------------------------------------------")
        pprint(series)
        print("------------------------------------------------")        

    def assertSeries(self, dataset_code):

        query = {
            'provider_name': self.fetcher.provider_name,
            "dataset_code": dataset_code
        }

        settings = self.DATASETS[dataset_code]

        series_list = list(self.db[constants.COL_SERIES].find(query))
        self.assertEqual(settings["series_accept"], len(series_list))

        series_sample = settings["series_sample"]
        series_db = series_list[0]
        if self.is_debug:
            self._debug_series(series_db)

        count_values = 0
        for s in series_list:
            count_values += len(s["values"])
        
        self.assertEqual(settings["series_all_values"], count_values)
        self.assertEqual(settings["series_key_first"], series_list[0]["key"])
        self.assertEqual(settings["series_key_last"], series_list[-1]["key"])

        self.assertEqual(series_db["provider_name"], series_sample["provider_name"])
        self.assertEqual(series_db["dataset_code"], series_sample["dataset_code"])
        
        self.assertEqual(series_db["key"], series_sample["key"])
        self.assertEqual(series_db["name"], series_sample["name"])
        self.assertEqual(series_db["frequency"], series_sample["frequency"])
        
        self.assertEqual(series_db["start_date"], series_sample["first_value"]["ordinal"])
        self.assertEqual(series_db["end_date"], series_sample["last_value"]["ordinal"])
        self.assertTrue(series_db["end_date"] >= series_db["start_date"])
        
        self.assertEqual(series_db["dimensions"], series_sample["dimensions"])

        dsd = settings["DSD"]

        first_sample = series_sample["first_value"]
        last_sample = series_sample["last_value"]
        first_value = series_db["values"][0]
        last_value = series_db["values"][-1]
        
        for source, target in [(first_value, first_sample), (last_value, last_sample)]:
            self.assertEqual(source["value"], target["value"])
            self.assertEqual(source["ordinal"], target["ordinal"])
            self.assertEqual(source["period"], target["period"])
            self.assertEqual(source["period_o"], target["period_o"])
            self.assertEqual(source["attributes"], target["attributes"])

        if dsd["is_completed"]:
            
            for key in series_db["dimensions"].keys():
                self.assertTrue(key in dsd["dimension_keys"])
            
            if series_db["attributes"]:
                for key in series_db["attributes"].keys():
                    self.assertTrue(key in dsd["attribute_keys"])

