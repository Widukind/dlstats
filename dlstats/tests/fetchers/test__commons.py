# -*- coding: utf-8 -*-

import os
import uuid
from datetime import datetime
from random import choice, randint

from voluptuous import MultipleInvalid

from pymongo.errors import DuplicateKeyError

from dlstats import constants
from dlstats.fetchers._commons import (Fetcher, 
                                       CodeDict, 
                                       DlstatsCollection, 
                                       Providers,
                                       Categories, 
                                       Datasets, 
                                       Series)

import unittest

from dlstats.tests.base import BaseTest, BaseDBTest, RESOURCES_DIR

class FakeDataset(Datasets):
    
    def download(self, urls):
        """Download all sources for this Dataset
        
        :param dict urls: URLS dict - key = final filename
        
        :return: :class:`dict`
        
        >>> fetcher = Fetcher(provider_name="test")
        >>> dataset = Datasets(fetcher=fetcher)
        >>> urls = ['http://localhost/file1.zip', http://localhost/file2.zip']
        >>> dataset.download(urls)
        {
            'file1.zip': '/tmp/dfkr56ert98/file1.zip',
            'file2.zip': '/tmp/dfkr56ert98/file2.zip',
        }        
        
        >>> from urllib.parse import urlparse
        >>> url = 'http://www.bea.gov//national/nipaweb/GetCSV.asp?GetWhat=SS_Data/SectionAll_xls.zip&Section=11'
        >>> u = urlparse(url)
        >>> u
        ParseResult(scheme='http', netloc='www.bea.gov', path='//national/nipaweb/GetCSV.asp', params='', query='GetWhat=SS_Data/SectionAll_xls.zip&Section=11', fragment='')
        >>> u.path
        '//national/nipaweb/GetCSV.asp'        
        """
        #urllib.request.url2pathname()
        files = {}
        for url in urls:
            filename = os.path.basename(url)
            files[filename] = url        
        return files
        
class FakeDatas():
    """Fake data for series


    - use:
    
        f = Fetcher(provider_name="p1", 
                    db=self.db, es_client=self.es)
        
        d = Datasets(provider_name="p1", 
                    dataset_code="d1",
                    name="d1 Name",
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=f, 
                    is_load_previous_version=False)
        
        d.dimension_list.update_entry("country", "country", "country")
        
        datas = FakeDatas(provider_name="p1", dataset_code="d1")
        d.series.data_iterator = datas
        
        result = d.update_database()
    """
    
    def __init__(self, provider_name=None, dataset_code=None, max_record=10, fetcher=None):
        
        self.provider_name = provider_name
        self.dataset_code = dataset_code
        self.max_record = max_record
        self.fetcher = fetcher
        
        self.rows = []
        self.keys = []
        self._create_fixtures()
        self._rows_iter = iter(self.rows)
        
    def _create_fixtures(self):
        for i in range(0, self.max_record):
            
            key = str(uuid.uuid4())
            self.keys.append(key)

            '''Mongo format attribute names'''
            n = 9
            frequency = choice(['A','Q']) 
            start_date = randint(10,100)
            end_date = start_date + n - 1
            data = {'provider': self.provider_name, 
                    'datasetCode': self.dataset_code,
                    'key': key, 
                    'name': key,
                    'frequency': frequency,
                    'startDate': start_date,
                    'endDate': end_date,
                    'values': [str(randint(i+1, 100)) for i in range(n)],
                    'releaseDates': [ datetime(2013,11,28) for i in range(n)],
                    'attributes': {},
                    'revisions': {},
                    'dimensions': {
                        'Country': 'AFG', 
                        'Scale': 'Billions'
                    }}
            """
            data = {'provider': self.provider_name, 
                    'datasetCode': self.dataset_code,
                    'key': key, 
                    'name': key,
                    'frequency': frequency,
                    'startDate': start_date,
                    'endDate': end_date,
                    'values': [str(randint(i+1, 100)) for i in range(n)],
                    'releaseDates': [ datetime(2013,11,28) for i in range(n)],
                    'attributes': {},
                    'revisions': {},
                    'dimensions': {
                        'Country': 'AFG', 
                        'Scale': 'Billions'
                    }}
            """
            self.rows.append(data)
    
    def __next__(self):
        row = next(self._rows_iter) 
        if row is None:
            raise StopIteration()
        return(row)
    


class FetcherTestCase(BaseTest):

    def test_constructor(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test__commons:FetcherTestCase.test_constructor

        with self.assertRaises(ValueError):
            Fetcher()

        f = Fetcher(provider_name="test")
        self.assertIsNotNone(f.provider_name)        
        self.assertIsNotNone(f.db) 
        self.assertIsNotNone(f.es_client)
        self.assertIsNone(f.provider) 

    def test_not_implemented_methods(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test__commons:FetcherTestCase.test_not_implemented_methods
        
        f = Fetcher(provider_name="test")
        
        with self.assertRaises(NotImplementedError):
            f.upsert_categories()

        with self.assertRaises(NotImplementedError):
            f.upsert_series()

        with self.assertRaises(NotImplementedError):
            f.upsert_a_series(None)

        with self.assertRaises(NotImplementedError):
            f.upsert_dataset(None)

        with self.assertRaises(AttributeError):
            f.insert_provider()

#TODO: CodeDictTestCase
class CodeDictTestCase(BaseTest):
    pass

class DlstatsCollectionTestCase(BaseTest):

    def test_constructor(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test__commons:DlstatsCollectionTestCase.test_constructor

        with self.assertRaises(ValueError):
            DlstatsCollection()

        with self.assertRaises(TypeError):
            DlstatsCollection(fetcher="abc")

        f = Fetcher(provider_name="test")
        DlstatsCollection(fetcher=f)

class CategoryTestCase(BaseTest):
    
    def test_constructor(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:CategoryTestCase.test_constructor
        
        with self.assertRaises(ValueError):
            Categories()
                    
        f = Fetcher(provider_name="p1")

        with self.assertRaises(MultipleInvalid):
            Categories(fetcher=f)
        
        c = Categories(provider="p1", 
                     name="cat1", 
                     categoryCode="c1",
                     docHref='http://www.example.com',
                     fetcher=f)
        
        bson = c.bson
        self.assertEqual(bson["categoryCode"], "c1")
        self.assertEqual(bson["name"], "cat1")
        self.assertEqual(bson["provider"], "p1")
        self.assertEqual(bson["docHref"], "http://www.example.com")
        self.assertEqual(bson["children"], [None])
        self.assertIsNone(bson["lastUpdate"])
        self.assertFalse(bson["exposed"])
    
        #print(c.schema(c.bson))
    
class ProviderTestCase(BaseTest):

    def test_constructor(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:ProviderTestCase.test_constructor

        with self.assertRaises(ValueError):
            Providers()
            
        f = Fetcher(provider_name="p1")            

        with self.assertRaises(MultipleInvalid):
            Providers(fetcher=f)
                
        p = Providers(name="p1", 
                     website="http://www.example.com", 
                     fetcher=f)
        
        bson = p.bson
        self.assertEqual(bson["name"], "p1")
        self.assertEqual(bson["website"], "http://www.example.com")


class DatasetTestCase(BaseTest):
    
    #TODO: test_load_previous_version()
    
    def test_constructor(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DatasetTestCase.test_constructor
        
        with self.assertRaises(ValueError):
            Datasets(is_load_previous_version=False)
            
        f = Fetcher(provider_name="p1")
                
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
        self.assertEqual(bson["provider"], "p1")
        self.assertEqual(bson["datasetCode"], "d1")
        self.assertEqual(bson["name"], "d1 Name")
        self.assertEqual(bson["docHref"], "http://www.example.com")
        self.assertTrue(isinstance(bson["dimensionList"], dict))
        self.assertTrue(isinstance(bson["attributeList"], dict))
        self.assertIsNone(bson["lastUpdate"])

        #TODO: last_update        
        d.last_update = datetime.now()
                
        #print(d.schema(d.bson))

class SeriesTestCase(BaseTest):
    
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
            
class DBCategoryTestCase(BaseDBTest):
    
    #TODO: test indexes keys and properties
    def test_indexes(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBCategoryTestCase.test_unique_constraint

        indexes = self.db[constants.COL_CATEGORIES].index_information()
        
        self.assertEqual(len(indexes), 2)
        
        
        """
        >>> pp(c.widukind.providers.index_information())

        {'_id_': {'key': [('_id', 1)], 'ns': 'widukind.providers', 'v': 1},
         'name_idx': {'key': [('name', 1)],
                      'ns': 'widukind.providers',
                      'unique': True,
                      'v': 1}}        
        
        >>> for i in list(c.widukind.datasets.index_information().items()): print(i)

        ('lastUpdate_idx', {'key': [('lastUpdate', -1)], 'v': 1, 'ns': 'widukind.datasets'})
        ('provider_datasetCode_idx', {'key': [('provider', 1), ('datasetCode', 1)], 'v': 1, 'ns': 'widukind.datasets', 'unique': True})
        ('name_idx', {'key': [('name', 1)], 'v': 1, 'ns': 'widukind.datasets'})
        ('_id_', {'key': [('_id', 1)], 'v': 1, 'ns': 'widukind.datasets'})                      
        """
    
    def test_unique_constraint(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBCategoryTestCase.test_unique_constraint
    
        self._collections_is_empty()
    
        f = Fetcher(provider_name="p1", db=self.db, es_client=self.es)
        
        c = Categories(provider="p1", 
                     name="cat1", 
                     categoryCode="c1",
                     docHref='http://www.example.com',
                     fetcher=f)
        result = c.update_database()
        self.assertIsNotNone(result)

        self.assertEqual(self.db[constants.COL_CATEGORIES].count(), 1)
        
        with self.assertRaises(DuplicateKeyError):
            existing_category = dict(provider="p1", categoryCode="c1")
            self.db[constants.COL_CATEGORIES].insert(existing_category)

        c = Categories(provider="p1", 
                     name="cat2", 
                     categoryCode="c2",
                     fetcher=f)
        result = c.update_database()
        self.assertIsNotNone(result)

        self.assertEqual(self.db[constants.COL_CATEGORIES].count(), 2)
    
    def test_update_database(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBCategoryTestCase.test_update_database
    
        self._collections_is_empty()
        
        from dlstats.fetchers._commons import create_or_update_indexes
        create_or_update_indexes(self.db)
    
        f = Fetcher(provider_name="p1", db=self.db, es_client=self.es)
        
        c = Categories(provider="p1", 
                     name="cat1", 
                     categoryCode="c1",
                     docHref='http://www.example.com',
                     fetcher=f)
        result = c.update_database()
        self.assertIsNotNone(result)
        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, None) #TODO: MongoDB 3.0 - return 0
        self.assertIsNotNone(result.upserted_id)

        bson = self.db[constants.COL_CATEGORIES].find_one({"provider": "p1", "categoryCode": "c1"})
        self.assertIsNotNone(bson)
        
        self.assertEqual(bson["categoryCode"], "c1")
        self.assertEqual(bson["name"], "cat1")
        self.assertEqual(bson["provider"], "p1")
        self.assertEqual(bson["docHref"], "http://www.example.com")
        self.assertEqual(bson["children"], [None])
        self.assertIsNone(bson["lastUpdate"])
        self.assertFalse(bson["exposed"])
    
        #print(c.schema(c.bson))
    
class DBProviderTestCase(BaseDBTest):

    #TODO: test indexes keys and properties
    def test_indexes(self):
        pass
    
    def test_unique_constraint(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBProviderTestCase.test_unique_constraint

        self._collections_is_empty()

        f = Fetcher(provider_name="p1", 
                    db=self.db, es_client=self.es)

        p = Providers(name="p1", 
                     website="http://www.example.com", 
                     fetcher=f)
        p.update_database()

        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 1)
        
        existing_provider = dict(name="p1")
        
        with self.assertRaises(DuplicateKeyError):
            self.db[constants.COL_PROVIDERS].insert(existing_provider)

        p = Providers(name="p2", 
                     website="http://www.example.com",
                     fetcher=f)
        p.update_database()

        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 2)

    def test_update_database(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBProviderTestCase.test_update_database

        self._collections_is_empty()

        f = Fetcher(provider_name="p1", 
                    db=self.db, es_client=self.es)

        p = Providers(name="p1", 
                     website="http://www.example.com", 
                     fetcher=f)
        result = p.update_database()
        self.assertIsNotNone(result)

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, None) #TODO: MongoDB 3.0 - return 0
        self.assertIsNotNone(result.upserted_id)

        bson = self.db[constants.COL_PROVIDERS].find_one({"name": "p1"})
        self.assertIsNotNone(bson)
        
        self.assertEqual(bson["name"], "p1")
        self.assertEqual(bson["website"], "http://www.example.com")

class DBDatasetTestCase(BaseDBTest):

    #TODO: test indexes keys and properties
    def test_indexes(self):
        pass
    
    def test_unique_constraint(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBDatasetTestCase.test_unique_constraint
    
        self._collections_is_empty()
        
        f = Fetcher(provider_name="p1", 
                    db=self.db, es_client=self.es)

        d = Datasets(provider_name="p1", 
                    dataset_code="d1",
                    name="d1 Name",
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=f, 
                    is_load_previous_version=False)
        d.dimension_list.update_entry("country", "country", "country")

        datas = FakeDatas(provider_name="p1", 
                          dataset_code="d1",
                          fetcher=f)
        d.series.data_iterator = datas

        result = d.update_database()
        self.assertIsNotNone(result)
        
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 1)
                        
        with self.assertRaises(DuplicateKeyError):
            existing_dataset = dict(provider="p1", datasetCode="d1")
            self.db[constants.COL_DATASETS].insert(existing_dataset)


    def test_update_database(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBDatasetTestCase.test_update_database

        self._collections_is_empty()

        f = Fetcher(provider_name="p1", 
                    db=self.db, es_client=self.es)

        d = Datasets(provider_name="p1", 
                    dataset_code="d1",
                    name="d1 Name",
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=f, 
                    is_load_previous_version=False)
        d.dimension_list.update_entry("country", "country", "country")

        datas = FakeDatas(provider_name="p1", 
                          dataset_code="d1",
                          fetcher=f)
        d.series.data_iterator = datas

        result = d.update_database()
        self.assertIsNotNone(result)
        
        #print(result.raw)

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, None) #TODO: MongoDB 3.0 - return 0
        self.assertIsNotNone(result.upserted_id)

        bson = self.db[constants.COL_DATASETS].find_one({"provider": "p1", "datasetCode": "d1"})
        self.assertIsNotNone(bson)
    
        self.assertEqual(bson["provider"], "p1")
        self.assertEqual(bson["datasetCode"], "d1")
        self.assertEqual(bson["name"], "d1 Name")
        self.assertEqual(bson["docHref"], "http://www.example.com")
        self.assertTrue(isinstance(bson["dimensionList"], dict))
        self.assertTrue(isinstance(bson["attributeList"], dict))

        series = self.db[constants.COL_SERIES].find({"provider": f.provider_name, 
                                                     "datasetCode": d.dataset_code})
        self.assertEqual(series.count(), datas.max_record)

class DBSeriesTestCase(BaseDBTest):
    
    #TODO: test indexes keys and properties
    def test_indexes(self):
        pass

    def test_process_series_data(self):        

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBSeriesTestCase.test_process_series_data

        self._collections_is_empty()
    
        provider_name = "p1"
        dataset_code = "d1"
    
        f = Fetcher(provider_name=provider_name, 
                    db=self.db, es_client=self.es)
        
        s = Series(provider_name=f.provider_name, 
                   dataset_code=dataset_code, 
                   last_update=None, 
                   bulk_size=1, 
                   fetcher=f)

        datas = FakeDatas(provider_name=provider_name, 
                          dataset_code=dataset_code,
                          fetcher=f)
        s.data_iterator = datas
        s.process_series_data()
        
        '''Count All series'''
        self.assertEqual( self.db[constants.COL_SERIES].count(), datas.max_record)

        '''Count series for this provider and dataset'''
        series = self.db[constants.COL_SERIES].find({"provider": f.provider_name, 
                                                     "datasetCode": dataset_code})
        self.assertEqual(series.count(), datas.max_record)

        '''Count series for this provider and dataset and in keys[]'''
        series = self.db[constants.COL_SERIES].find({"provider": f.provider_name, 
                                                     "datasetCode": dataset_code,
                                                     "key": {"$in": datas.keys}})
        
        self.assertEqual(series.count(), datas.max_record)
        
            
if __name__ == '__main__':
    unittest.main()
