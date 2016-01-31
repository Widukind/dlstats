# -*- coding: utf-8 -*-

from datetime import datetime

from bson import ObjectId
from voluptuous import MultipleInvalid
from pymongo.errors import DuplicateKeyError

from dlstats import constants
from dlstats import errors
from dlstats.fetchers._commons import (Fetcher, 
                                       CodeDict, 
                                       DlstatsCollection, 
                                       Providers,
                                       Categories,
                                       Datasets, 
                                       Series,
                                       SeriesIterator)

import unittest

from dlstats.tests.base import BaseTestCase, BaseDBTestCase

class FakeSeriesIterator(SeriesIterator):
    
    def __init__(self, dataset, series_list):
        super().__init__()
        self.dataset = dataset
        self.series_list = series_list
        self.rows = self._process()
        
    def _process(self):
        for row in self.series_list:
            yield row, None
        
    def build_series(self, bson):
        for key, dim in bson["dimensions"].items():
            self.dataset.dimension_list.update_entry(key, dim, key)
        return bson

SERIES1 = {
    'provider_name': 'p1',
    'dataset_code': 'd1',
    'name': 'series1',
    'key': 'key1',
    'values': [
        {
            'release_date': datetime(2015, 1, 1, 0, 0, 0),
            'ordinal': 25,
            'period_o': '1995',
            'period': '1995',
            'value': '1.0',
            'attributes': {
                'OBS_STATUS': 'a'
            },
        },
        {
            'release_date': datetime(2015, 1, 1, 0, 0, 0),
            'ordinal': 44,
            'period_o': '2014',
            'period': '2014',
            'value': '1.5',
            'attributes': None
        }
    ],
    'attributes': None,
    'dimensions': {
        'Country': 'AFG',
        'Scale': 'Billions',
    },
    'last_update': None,
    'start_date': 25, #1995
    'end_date': 44, #2014
    'frequency': 'A'           
}

class FetcherTestCase(BaseTestCase):

    def test_constructor(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test__commons:FetcherTestCase.test_constructor

        with self.assertRaises(ValueError):
            Fetcher()

        f = Fetcher(provider_name="test", is_indexes=False)
        self.assertIsNotNone(f.provider_name)        
        self.assertIsNotNone(f.db) 
        self.assertIsNone(f.provider) 

    def test_not_implemented_methods(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test__commons:FetcherTestCase.test_not_implemented_methods
        
        f = Fetcher(provider_name="test", is_indexes=False)
        
        with self.assertRaises(NotImplementedError):
            f.upsert_dataset(None)

#TODO: CodeDictTestCase
class CodeDictTestCase(BaseTestCase):
    pass

class DlstatsCollectionTestCase(BaseTestCase):

    def test_constructor(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test__commons:DlstatsCollectionTestCase.test_constructor

        with self.assertRaises(ValueError):
            DlstatsCollection()

        with self.assertRaises(TypeError):
            DlstatsCollection(fetcher="abc")

        f = Fetcher(provider_name="test", is_indexes=False)
        DlstatsCollection(fetcher=f)

class ProviderTestCase(BaseTestCase):

    def test_constructor(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:ProviderTestCase.test_constructor

        with self.assertRaises(ValueError):
            Providers()
            
        f = Fetcher(provider_name="p1", is_indexes=False)

        with self.assertRaises(MultipleInvalid):
            Providers(fetcher=f)
                
        p = Providers(name="p1",
                      long_name="Provider One",
                      version=1,
                      region="Dreamland",
                      website="http://www.example.com", 
                      fetcher=f)
        
        bson = p.bson
        self.assertEqual(bson["name"], "p1")
        self.assertEqual(bson["long_name"], "Provider One")
        self.assertEqual(bson["region"], "Dreamland")
        self.assertEqual(bson["website"], "http://www.example.com")
        self.assertEqual(bson["slug"], "p1")

class DatasetTestCase(BaseTestCase):
    
    #TODO: test_load_previous_version()
    
    def test_constructor(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DatasetTestCase.test_constructor
        
        with self.assertRaises(ValueError):
            Datasets(is_load_previous_version=False)
            
        f = Fetcher(provider_name="p1", is_indexes=False)
                
        d = Datasets(provider_name="p1", 
                    dataset_code="d1",
                    name="d1 Name",
                    doc_href="http://www.example.com",
                    fetcher=f, 
                    is_load_previous_version=False)
        d.dimension_list.update_entry("country", "country", "country")

        self.assertTrue(isinstance(d.series, Series))
        self.assertTrue(isinstance(d.dimension_list, CodeDict))
        self.assertTrue(isinstance(d.attribute_list, CodeDict))
        
        bson = d.bson
        self.assertEqual(bson['provider_name'], "p1")
        self.assertEqual(bson["dataset_code"], "d1")
        self.assertEqual(bson["name"], "d1 Name")
        self.assertEqual(bson["doc_href"], "http://www.example.com")
        self.assertTrue(isinstance(bson["dimension_list"], dict))
        self.assertTrue(isinstance(bson["attribute_list"], dict))
        self.assertIsNone(bson["last_update"])
        self.assertEqual(bson["slug"], "p1-d1")

        #TODO: last_update        
        d.last_update = datetime.now()
                
        #print(d.schema(d.bson))

class SeriesTestCase(BaseTestCase):
    
    def test_constructor(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:SeriesTestCase.test_constructor

        with self.assertRaises(ValueError):
            Series()
            
        f = Fetcher(provider_name="p1")
            
        s = Series(provider_name="p1", 
                   dataset_code="d1", 
                   last_update=None, 
                   bulk_size=1, 
                   fetcher=f)
        
        self.assertFalse(hasattr(s, "data_iterator"))
    
class DBProviderTestCase(BaseDBTestCase):

    #TODO: test indexes keys and properties
    @unittest.skipIf(True, "TODO")    
    def test_indexes(self):
        pass
    
    def test_unique_constraint(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBProviderTestCase.test_unique_constraint

        self._collections_is_empty()

        f = Fetcher(provider_name="p1", 
                    db=self.db)

        p = Providers(name="p1", 
                      long_name="Provider One",
                      version=1,
                      region="Dreamland",
                      website="http://www.example.com", 
                      fetcher=f)
        f.provider = p

        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 1)
        
        existing_provider = dict(name="p1")
        
        with self.assertRaises(DuplicateKeyError):
            self.db[constants.COL_PROVIDERS].insert(existing_provider)

        p = Providers(name="p2", 
                      long_name="Provider One",
                      version=1,                      
                      region="Dreamland",
                      website="http://www.example.com",
                      fetcher=f)
        p.update_database()

        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 2)

    def test_version_field(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBProviderTestCase.test_version_field

        self._collections_is_empty()

        f = Fetcher(provider_name="p1", 
                    db=self.db)

        with self.assertRaises(MultipleInvalid):
            Providers(name="p1", 
                      long_name="Provider One",
                      region="Dreamland",
                      website="http://www.example.com", 
                      fetcher=f)

        p = Providers(name="p1", 
                      long_name="Provider One",
                      version=1,
                      region="Dreamland",
                      website="http://www.example.com", 
                      fetcher=f)
        p.update_database()        

        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 1)

    def test_update_database(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBProviderTestCase.test_update_database

        self._collections_is_empty()

        f = Fetcher(provider_name="p1", 
                    db=self.db)

        p = Providers(name="p1", 
                      long_name="Provider One",
                      version=1,
                      region="Dreamland",
                      website="http://www.example.com", 
                      fetcher=f)
        id = p.update_database()
        self.assertIsNotNone(id)
        self.assertIsInstance(id, ObjectId)
        self.db[constants.COL_PROVIDERS].find_one({'_id': ObjectId(id)})
        
        bson = self.db[constants.COL_PROVIDERS].find_one({"name": "p1"})
        self.assertIsNotNone(bson)
        
        self.assertEqual(bson["name"], "p1")
        self.assertEqual(bson["website"], "http://www.example.com")

class DBCategoriesTestCase(BaseDBTestCase):

    #TODO: test indexes keys and properties
    @unittest.skipIf(True, "TODO")    
    def test_indexes(self):
        pass
    
    def test_unique_constraint(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBCategoriesTestCase.test_unique_constraint
    
        self._collections_is_empty()
        
        f = Fetcher(provider_name="p1", 
                    db=self.db)
        
        cat = Categories(provider_name=f.provider_name, 
                         category_code="c1", 
                         name="C1", 
                         fetcher=f)

        result = cat.update_database()
        self.assertIsNotNone(result)
        
        self.assertEqual(self.db[constants.COL_CATEGORIES].count(), 1)
                        
        with self.assertRaises(DuplicateKeyError):
            existing_dataset = dict(provider_name=cat.provider_name, 
                                    category_code=cat.category_code)
            self.db[constants.COL_CATEGORIES].insert(existing_dataset)

    def test_add_categories(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBCategoriesTestCase.test_add_categories

        f = Fetcher(provider_name="p1", is_indexes=False)

        p = Providers(name="p1",
                      long_name="Provider One",
                      version=1,
                      region="Dreamland",
                      website="http://www.example.com", 
                      fetcher=f)        
        p.update_database()
        
        minimal_category = { 'category_code': "c0", 'name': "p1"}
        f.upsert_data_tree([minimal_category])
        
        data_tree = [
             {'category_code': 'p1',
              'datasets': [],
              #'description': None,
              'doc_href': 'http://www.example.com',
              #'exposed': False,
              'last_update': None,
              'name': 'p1'},
             {'category_code': 'p1.c0',
              'datasets': [],
              #'description': None,
              'doc_href': None,
              #'exposed': False,
              'last_update': None,
              'name': 'p1'}
        ]        
        
        #self.assertEqual(p.data_tree, data_tree)


class DBDatasetTestCase(BaseDBTestCase):

    #TODO: test indexes keys and properties
    @unittest.skipIf(True, "TODO")    
    def test_indexes(self):
        pass
    
    def test_unique_constraint(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBDatasetTestCase.test_unique_constraint
    
        self._collections_is_empty()
        
        f = Fetcher(provider_name="p1", 
                    db=self.db)

        d = Datasets(provider_name="p1", dataset_code="d1", name="d1 Name",
                    last_update=datetime.now(), fetcher=f, 
                    is_load_previous_version=False)
        
        series_list = [SERIES1.copy()]
        datas = FakeSeriesIterator(d, series_list)
        d.series.data_iterator = datas

        result = d.update_database()
        self.assertIsNotNone(result)
        
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 1)
                        
        with self.assertRaises(DuplicateKeyError):
            existing_dataset = dict(provider_name="p1", dataset_code="d1")
            self.db[constants.COL_DATASETS].insert(existing_dataset)


    def test_update_database(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBDatasetTestCase.test_update_database

        self._collections_is_empty()

        f = Fetcher(provider_name="p1", 
                    db=self.db)

        d = Datasets(provider_name="p1", 
                    dataset_code="d1",
                    name="d1 Name",
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=f, 
                    is_load_previous_version=False)

        series_list = [SERIES1.copy()]
        datas = FakeSeriesIterator(d, series_list)
        d.series.data_iterator = datas

        _id = d.update_database()
        self.assertIsNotNone(_id)
        self.assertIsInstance(_id, ObjectId)
        self.db[constants.COL_DATASETS].find_one({'_id': _id})
        
        bson = self.db[constants.COL_DATASETS].find_one({'provider_name': "p1", 
                                                         "dataset_code": "d1"})
        self.assertIsNotNone(bson)
    
        self.assertEqual(bson['provider_name'], "p1")
        self.assertEqual(bson["dataset_code"], "d1")
        self.assertEqual(bson["name"], "d1 Name")
        self.assertEqual(bson["doc_href"], "http://www.example.com")
        self.assertTrue(isinstance(bson["dimension_list"], dict))
        self.assertTrue(isinstance(bson["attribute_list"], dict))

        count = self.db[constants.COL_SERIES].count({'provider_name': f.provider_name, 
                                                     "dataset_code": d.dataset_code})
        self.assertEqual(count, 1)

    def test_not_recordable_dataset(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBDatasetTestCase.test_not_recordable_dataset

        self._collections_is_empty()

        f = Fetcher(provider_name="p1",
                    max_errors=1, 
                    db=self.db)

        d = Datasets(provider_name="p1", 
                    dataset_code="d1",
                    name="d1 Name",
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=f, 
                    is_load_previous_version=False)
        d.dimension_list.update_entry("Scale", "Billions", "Billions")
        d.dimension_list.update_entry("country", "AFG", "AFG")
        
        class EmptySeriesIterator():
            def __next__(self):
                raise StopIteration            

        datas = EmptySeriesIterator()
        d.series.data_iterator = datas

        _id = d.update_database()
        self.assertIsNotNone(_id)
        
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 1)
        
        doc = self.db[constants.COL_DATASETS].find_one({"_id": _id})
        self.assertIsNotNone(doc)
        self.assertEqual(doc["enable"], False)
                
        
class DBSeriesTestCase(BaseDBTestCase):

    #TODO: test indexes keys and properties
    @unittest.skipIf(True, "TODO")    
    def test_indexes(self):
        pass
    
    def test_reject_series(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBSeriesTestCase.test_reject_series

        self._collections_is_empty()
        
        class MyFetcher_Data(SeriesIterator):
            
            def rows_generator(self):
                yield None, errors.RejectEmptySeries()
                yield None, errors.RejectUpdatedSeries(key="series-updated")
                yield None, errors.RejectFrequency(frequency="S")
                yield SERIES1.copy(), None
                yield {}, None
            
            def __init__(self):
                super().__init__()
                self.rows = self.rows_generator()
                
            def build_series(self, bson):
                bson['last_update'] = datetime.now()
                return bson
        
        fetcher = Fetcher(provider_name="test", db=self.db)
        
        series = Series(provider_name="test", dataset_code="d1",
                        bulk_size=1, fetcher=fetcher)
        
        data = MyFetcher_Data()
        series.data_iterator = data
        series.process_series_data()
        
        """
        TODO: capturer logs
        dlstats.fetchers._commons: WARNING: Reject empty series for dataset[d1]
        dlstats.fetchers._commons: DEBUG: Reject series updated for dataset[d1] - key[series-updated]
        dlstats.fetchers._commons: WARNING: Reject frequency for dataset[d1] - frequency[S]        
        """
        self.assertEqual(self.db[constants.COL_SERIES].count(), 1) 
                

    def test_process_series_data(self):        

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBSeriesTestCase.test_process_series_data

        self._collections_is_empty()
    
        provider_name = "p1"
        dataset_code = "d1"
        dataset_name = "d1 name"
    
        f = Fetcher(provider_name=provider_name, 
                    db=self.db)

        d = Datasets(provider_name=provider_name, 
                    dataset_code=dataset_code,
                    name=dataset_name,
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=f, 
                    is_load_previous_version=False)
        
        s = Series(provider_name=f.provider_name, 
                   dataset_code=dataset_code, 
                   last_update=datetime(2013,10,28), 
                   bulk_size=1, 
                   fetcher=f)

        series_list = [SERIES1.copy()]
        datas = FakeSeriesIterator(d, series_list)
        s.data_iterator = datas
        
        d.series = s
        d.update_database()        
        
        '''Count All series'''
        self.assertEqual(self.db[constants.COL_SERIES].count(), len(series_list))

        '''Count series for this provider and dataset'''
        series = self.db[constants.COL_SERIES].find({'provider_name': f.provider_name, 
                                                     "dataset_code": dataset_code})
        self.assertEqual(series.count(), len(series_list))

        '''Count series for this provider and dataset and in keys[]'''
        keys = [s['key'] for s in series_list]
        series = self.db[constants.COL_SERIES].find({'provider_name': f.provider_name, 
                                                     "dataset_code": dataset_code,
                                                     "key": {"$in": keys}})
        
        self.assertEqual(series.count(), len(series_list))

    def test_revisions(self):        

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBSeriesTestCase.test_revisions

        self._collections_is_empty()
    
        provider_name = "p1"
        dataset_code = "d1"
        dataset_name = "d1 name"
    
        f = Fetcher(provider_name=provider_name, 
                    db=self.db)

        d = Datasets(provider_name=provider_name, 
                    dataset_code=dataset_code,
                    name=dataset_name,
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=f, 
                    is_load_previous_version=False)
        
        s1 = Series(provider_name=f.provider_name, 
                    dataset_code=dataset_code, 
                    last_update=datetime(2013,4,1), 
                    bulk_size=1, 
                    fetcher=f)

        series_list = [SERIES1.copy()]
        datas1 = FakeSeriesIterator(d, series_list)
        s1.data_iterator = datas1
        d.series = s1
        d.update_database()

        test_key = SERIES1['key']

        SERIES1.pop("_id", None)
        old_value = SERIES1["values"][0]["value"]
        old_release_date = SERIES1["values"][0]["release_date"]

        SERIES2 = SERIES1.copy()
        SERIES2["values"][0]["value"] = "10"

        s1.last_update = datetime(old_release_date.year+1, 
                                  old_release_date.month, 
                                  old_release_date.day)

        old_bson = self.db[constants.COL_SERIES].find_one({'key': test_key})
        #pprint(old_bson)
        try:
            s1.update_series(SERIES2, old_bson=old_bson, is_bulk=False)
        except Exception as err:
            #print(err.path) #['values', 0, 'release_date']
            self.fail(str(err))        
        
        bson = self.db[constants.COL_SERIES].find_one({'key': test_key})
        #pprint(bson)
        
        self.assertEqual(bson["values"][0]["value"], "10")
        self.assertEqual(bson["values"][0]["release_date"], datetime(2016, 1, 1, 0, 0))
        
        self.assertTrue("revisions" in bson["values"][0])
        self.assertEqual(len(bson["values"][0]["revisions"]), 1)        
        self.assertEqual(bson["values"][0]["revisions"][0]["value"], old_value)
        self.assertEqual(bson["values"][0]["revisions"][0]["revision_date"], old_release_date)
        
