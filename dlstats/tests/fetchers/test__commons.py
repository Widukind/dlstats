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
                                       Provider,
                                       Category, 
                                       Dataset, 
                                       Series,
                                       SerieEntry)

import unittest

from dlstats.tests.base import BaseTest, BaseDBTest, RESOURCES_DIR

class FakeDataset(Dataset):
    
    def download(self, urls):
        """Download all sources for this Dataset
        
        :param dict urls: URLS dict - key = final filename
        
        :return: :class:`dict`
        
        >>> fetcher = Fetcher(provider_name="test")
        >>> dataset = Dataset(fetcher=fetcher)
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
        
        d = Dataset(provider_name="p1", 
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
    
    def __init__(self, provider_name=None, dataset_code=None, max_record=10):
        
        self.provider_name = provider_name
        self.dataset_code = dataset_code
        self.max_record = max_record
        
        self.rows = []
        self.keys = []
        self._create_fixtures()
        self._rows_iter = iter(self.rows)
        
    def _create_fixtures(self):
        for i in range(0, self.max_record):
            
            key = str(uuid.uuid4())
            self.keys.append(key)

            '''Mongo format attribute names'''
            bson = dict(provider=self.provider_name, 
                        datasetCode=self.dataset_code,
                        key=key, 
                        name=key,
                        frequency=choice(['A', 'Q']),
                        startDate=randint(10, 100),
                        endDate=randint(10, 100),                    
                        values=[str(randint(i, 100)) for i in range(1, 10)],
                        releaseDates=[
                            datetime(2013,11,28),
                            datetime(2014,12,28),
                            datetime(2015,1,28),
                            datetime(2015,2,28)
                        ],
                        attributes={},
                        revisions={},                  
                        dimensions={
                            'Country': 'AFG', 
                            'Scale': 'Billions'
                        })
            """
            bson = dict(provider_name=self.provider_name, 
                        dataset_code=self.dataset_code,
                        key=key, 
                        name=key,
                        frequency=choice(['A', 'Q']),
                        start_date=randint(10, 100),
                        end_date=randint(10, 100),                    
                        values=[str(randint(i, 100)) for i in range(1, 10)],
                        release_dates=[
                            datetime(2013,11,28),
                            datetime(2014,12,28),
                            datetime(2015,1,28),
                            datetime(2015,2,28)
                        ],
                        attributes={},
                        revisions={},                  
                        dimensions={
                            'Country': 'AFG', 
                            'Scale': 'Billions'
                        })
            """
            self.rows.append(bson)
    
    def __next__(self):
        row = next(self._rows_iter) 
        series = self.build_series(row)
        if series is None:
            raise StopIteration()
        return(series)
    
    def build_series(self, row):
        return row


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
            Category()
                    
        f = Fetcher(provider_name="p1")

        with self.assertRaises(MultipleInvalid):
            Category(fetcher=f)
        
        c = Category(provider="p1", 
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
            Provider()
            
        f = Fetcher(provider_name="p1")            

        with self.assertRaises(MultipleInvalid):
            Provider(fetcher=f)
                
        p = Provider(name="p1", 
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
            Dataset(is_load_previous_version=False)
            
        f = Fetcher(provider_name="p1")
                
        d = Dataset(provider_name="p1", 
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

        
class SerieEntryTestCase(BaseTest):
    
    def test_constructor(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:SerieEntryTestCase.test_constructor

        with self.assertRaises(ValueError):
            SerieEntry()

        f = Fetcher(provider_name="p1")
            
        '''SerieEntry Instance populate from init'''        
        s = SerieEntry(provider_name="p1", 
                       dataset_code="d1", 
                       #last_update=datetime.now(), 
                       key='GDP_FR', 
                       name='GDP in France',
                       frequency='Q',
                       dimensions={'Country': 'FR'}, 
                       fetcher=f)
        s.schema(s.bson)

        '''SerieEntry Instance populate from bson datas'''        
        s = SerieEntry(fetcher=f)
        
        '''Mongo attribute names'''
        bson = dict(provider="p1", 
                    datasetCode="d1", 
                    key='GDP_FR', 
                    name='GDP in France',
                    frequency='Q',
                    dimensions={'Country': 'FR'} )
        s.populate(bson)
        s.schema(s.bson)        
        
        '''Same test with more datas'''
        '''Mongo attribute names'''
        bson = dict(provider="p1", 
                    datasetCode="d1",
                    key='GDP_FR', 
                    name='GDP in France',
                    frequency='A',
                    startDate=10,
                    endDate=50,                    
                    values=[
                        'n/a',
                        '2700', 
                        '2720', 
                        '2740', 
                        '2760'
                    ],
                    releaseDates=[
                        datetime(2013,11,28),
                        datetime(2014,12,28),
                        datetime(2015,1,28),
                        datetime(2015,2,28)
                    ],
                    attributes={
                        'flag': ["", "", "e", "e"],  
                    },
                    revisions={
                        '33': [{'releaseDates': datetime(2014, 10, 1, 0, 0), 'value': '1,148.113'}],
                        '34': [{'releaseDates': datetime(2014, 10, 1, 0, 0), 'value': '1,248.663'}],
                    },                  
                    dimensions={
                        'Country': 'AFG', 
                        'Scale': 'Billions'
                    })
        s = SerieEntry(fetcher=f)
        s.populate(bson)
        s.schema(s.bson)
            
class DBCategoryTestCase(BaseDBTest):
    
    #TODO: test indexes keys and properties
    def test_indexes(self):
        pass
    
    def test_unique_constraint(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBCategoryTestCase.test_unique_constraint
    
        self._collections_is_empty()
    
        f = Fetcher(provider_name="p1", db=self.db, es_client=self.es)
        
        c = Category(provider="p1", 
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

        c = Category(provider="p1", 
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
        
        c = Category(provider="p1", 
                     name="cat1", 
                     categoryCode="c1",
                     docHref='http://www.example.com',
                     fetcher=f)
        result = c.update_database()
        """
        print(result.matched_count, result.modified_count, result.upserted_id, result.raw_result)
        > created:
        0 0 561e4b96f3ceb180160a3db0 
        {'electionId': ObjectId('56169d19f3ceb180160a3d25'), 'nModified': 0, 'updatedExisting': False, 'ok': 1, 'lastOp': Timestamp(1444826006, 3), 'n': 1, 'upserted': ObjectId('561e4b96f3ceb180160a3db0')}                

        > updated:
        1 1 None 
        {'ok': 1, 'electionId': ObjectId('56169d19f3ceb180160a3d25'), 'updatedExisting': True, 'n': 1, 'nModified': 1, 'lastOp': Timestamp(1444825463, 1)}
        """
        self.assertIsNotNone(result)
        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)
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

        self._collections_is_empty()

        f = Fetcher(provider_name="p1", 
                    db=self.db, es_client=self.es)

        p = Provider(name="p1", 
                     website="http://www.example.com", 
                     fetcher=f)
        p.update_database()

        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 1)
        
        existing_provider = dict(name="p1")
        
        with self.assertRaises(DuplicateKeyError):
            self.db[constants.COL_PROVIDERS].insert(existing_provider)

        p = Provider(name="p2", 
                     website="http://www.example.com",
                     fetcher=f)
        p.update_database()

        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 2)

    def test_update_database(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBProviderTestCase.test_update_database

        self._collections_is_empty()

        f = Fetcher(provider_name="p1", 
                    db=self.db, es_client=self.es)

        p = Provider(name="p1", 
                     website="http://www.example.com", 
                     fetcher=f)
        result = p.update_database()
        self.assertIsNotNone(result)

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)
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

        d = Dataset(provider_name="p1", 
                    dataset_code="d1",
                    name="d1 Name",
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=f, 
                    is_load_previous_version=False)
        d.dimension_list.update_entry("country", "country", "country")

        datas = FakeDatas(provider_name="p1", 
                          dataset_code="d1")
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

        d = Dataset(provider_name="p1", 
                    dataset_code="d1",
                    name="d1 Name",
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=f, 
                    is_load_previous_version=False)
        d.dimension_list.update_entry("country", "country", "country")

        datas = FakeDatas(provider_name="p1", 
                          dataset_code="d1")
        d.series.data_iterator = datas

        result = d.update_database()
        self.assertIsNotNone(result)
        
        #print(result.raw)

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)
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

    def test_unique_constraint(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBSeriesTestCase.test_unique_constraint
    
        self._collections_is_empty()

        f = Fetcher(provider_name="p1", 
                    db=self.db, es_client=self.es)
        
        s = SerieEntry(provider_name=f.provider_name, 
                       dataset_code="d1", 
                       key='GDP_FR', 
                       name='GDP in France',
                       frequency='Q',
                       dimensions={'Country': 'FR'}, 
                       fetcher=f)
        id = s.update_serie()
        self.assertIsNotNone(id)
        
        self.assertEqual(self.db[constants.COL_SERIES].count(), 1)
                        
        with self.assertRaises(DuplicateKeyError):
            existing_serie = dict(provider="p1", datasetCode="d1", key="GDP_FR")
            self.db[constants.COL_SERIES].insert(existing_serie)
    
    def test_process_series(self):        

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBSeriesTestCase.test_process_series

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
                          dataset_code=dataset_code)
        s.data_iterator = datas
        s.process_series()
        
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