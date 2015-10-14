# -*- coding: utf-8 -*-

import uuid
from datetime import datetime
from random import choice, randint

from voluptuous import MultipleInvalid

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

from ..base import BaseTest, BaseDBTest, RESOURCES_DIR

class FakeDatas():
    
    def __init__(self, provider=None, datasetCode=None, max_record=10):
        
        self.provider = provider
        self.datasetCode = datasetCode
        self.max_record = max_record
        
        self.rows = []
        self.keys = []
        self._create_fixtures()
        self._rows_iter = iter(self.rows)
        
    def _create_fixtures(self):
        for i in range(0, self.max_record):
            
            key = str(uuid.uuid4())
            self.keys.append(key)
            
            bson = dict(provider=self.provider, 
                        datasetCode=self.datasetCode,
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
    
    #TODO: test_update_database()
    
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

    #TODO: test_update_database()

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
            
        s = Series(provider="p1", 
                   datasetCode="d1", 
                   lastUpdate=None, 
                   bulk_size=1, 
                   fetcher=f)
        
        self.assertFalse(hasattr(s, "data_iterator"))

        
class SerieEntryTestCase(BaseTest):
    
    #TODO: test_update_database()
    
    def test_constructor(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:SerieEntryTestCase.test_constructor

        with self.assertRaises(ValueError):
            SerieEntry()

        f = Fetcher(provider_name="p1")
            
        '''SerieEntry Instance populate from init'''        
        s = SerieEntry(provider="p1", 
                       datasetCode="d1", 
                       #lastUpdate=datetime.now(), 
                       key='GDP_FR', 
                       name='GDP in France',
                       frequency='Q',
                       dimensions={'Country': 'FR'}, 
                       fetcher=f)
        s.schema(s.bson)

        '''SerieEntry Instance populate from bson datas'''        
        s = SerieEntry(fetcher=f)
        
        bson = dict(provider="p1", 
                    datasetCode="d1", 
                    key='GDP_FR', 
                    name='GDP in France',
                    frequency='Q',
                    dimensions={'Country': 'FR'} )
        s.populate(bson)
        s.schema(s.bson)        
        
        '''Same test with more datas'''
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
    
    def test_update_database(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBCategoryTestCase.test_update_database
    
        self._collections_is_empty()
    
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

    def test_update_database(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBProviderTestCase.test_update_database

        self._collections_is_empty()

        f = Fetcher(provider_name="p1", 
                    db=self.db, es_client=self.es)

        p = Provider(name="p1", 
                     website="http://www.example.com", 
                     fetcher=f)
        result = p.update_database()

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)
        self.assertIsNotNone(result.upserted_id)

        bson = self.db[constants.COL_PROVIDERS].find_one({"name": "p1"})
        self.assertIsNotNone(bson)
        
        self.assertEqual(bson["name"], "p1")
        self.assertEqual(bson["website"], "http://www.example.com")


class DBDatasetTestCase(BaseDBTest):

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

        datas = FakeDatas(provider="p1", 
                          datasetCode="d1")
        d.series.data_iterator = datas

        result = d.update_database()
        
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
    
    def test_build_series(self):        

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBSeriesTestCase.test_build_series

        self._collections_is_empty()
    
        provider_name = "p1"
        datasetCode = "d1"
    
        f = Fetcher(provider_name=provider_name, 
                    db=self.db, es_client=self.es)
        
        s = Series(provider=f.provider_name, 
                   datasetCode=datasetCode, 
                   lastUpdate=None, 
                   bulk_size=1, 
                   fetcher=f)
        

        datas = FakeDatas(provider=provider_name, 
                          datasetCode=datasetCode)
        s.data_iterator = datas
        s.process_series()
        
        '''Count All series'''
        self.assertEqual( self.db[constants.COL_SERIES].count(), datas.max_record)

        '''Count series for this provider and dataset'''
        series = self.db[constants.COL_SERIES].find({"provider": f.provider_name, 
                                                     "datasetCode": datasetCode})
        self.assertEqual(series.count(), datas.max_record)

        '''Count series for this provider and dataset and in keys[]'''
        series = self.db[constants.COL_SERIES].find({"provider": f.provider_name, 
                                                     "datasetCode": datasetCode,
                                                     "key": {"$in": datas.keys}})
        
        self.assertEqual(series.count(), datas.max_record)
        
            
if __name__ == '__main__':
    unittest.main()