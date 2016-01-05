# -*- coding: utf-8 -*-

import time
import os
import uuid
from datetime import datetime
from random import choice, randint
from copy import deepcopy
from bson import ObjectId

from voluptuous import MultipleInvalid

from pymongo.errors import DuplicateKeyError

from widukind_common import tags

from dlstats import constants
from dlstats.fetchers._commons import (Fetcher, 
                                       CodeDict, 
                                       DlstatsCollection, 
                                       Providers,
                                       Datasets, 
                                       Series)

import unittest

from dlstats.tests.base import BaseTestCase, BaseDBTestCase, RESOURCES_DIR

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
                    db=self.db)
        
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
                    'dataset_code': self.dataset_code,
                    'key': key, 
                    'name': key,
                    'frequency': frequency,
                    'start_date': start_date,
                    'end_date': end_date,
                    'values': [str(randint(i+1, 100)) for i in range(n)],
                    'attributes': {},
                    'revisions': {},
                    'dimensions': {
                        'Country': 'AFG', 
                        'Scale': 'Billions'
                    }}
            """
            data = {'provider': self.provider_name, 
                    'dataset_code': self.dataset_code,
                    'key': key, 
                    'name': key,
                    'frequency': frequency,
                    'start_date': start_date,
                    'end_date': end_date,
                    'values': [str(randint(i+1, 100)) for i in range(n)],
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
        self.assertEqual(bson["provider"], "p1")
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
    def test_indexes(self):
        pass
    
    def test_unique_constraint(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBProviderTestCase.test_unique_constraint

        self._collections_is_empty()

        f = Fetcher(provider_name="p1", 
                    db=self.db)

        p = Providers(name="p1", 
                      long_name="Provider One",
                      region="Dreamland",
                      website="http://www.example.com", 
                      fetcher=f)
        p.update_database()

        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 1)
        
        existing_provider = dict(name="p1")
        
        with self.assertRaises(DuplicateKeyError):
            self.db[constants.COL_PROVIDERS].insert(existing_provider)

        p = Providers(name="p2", 
                      long_name="Provider One",
                      region="Dreamland",
                      website="http://www.example.com",
                      fetcher=f)
        p.update_database()

        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 2)

    def test_update_database(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBProviderTestCase.test_update_database

        self._collections_is_empty()

        f = Fetcher(provider_name="p1", 
                    db=self.db)

        p = Providers(name="p1", 
                      long_name="Provider One",
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

    def test_add_data_tree(self):
        # nosetests -s -v dlstats.tests.fetchers.test__commons:ProviderTestCase.test_constructor

        f = Fetcher(provider_name="p1", is_indexes=False)

        p = Providers(name="p1",
                      long_name="Provider One",
                      region="Dreamland",
                      website="http://www.example.com", 
                      fetcher=f)
        p.update_database()
        
        data_tree = {'provider': "p1",
                     'name': "p1_root",
                     'category_code': "c0",
                     'doc_href': 'http://www.example.com',
                     'children': [
                         {'provider': "p1",
                          'name': "cat1", 
                          'category_code': "c1",
                          'last_update': datetime(2010,1,5),
                          'exposed': False,
                          'children': None}]
                     }

        res = p.add_data_tree(data_tree)
        
        bson = res['data_tree']
        self.assertEqual(bson["category_code"], "c0")
        self.assertEqual(bson["name"], "p1_root")
        self.assertEqual(bson["provider"], "p1")
        self.assertEqual(bson["doc_href"], "http://www.example.com")

        bson1 = res['data_tree']['children'][0]
        self.assertEqual(bson1["category_code"], "c1")
        self.assertEqual(bson1["name"], "cat1")
        self.assertEqual(bson1["provider"], "p1")
        self.assertEqual(bson1["last_update"],datetime(2010,1,5))
        self.assertFalse(bson1["exposed"])

class DBDatasetTestCase(BaseDBTestCase):

    #TODO: test indexes keys and properties
    def test_indexes(self):
        pass
    
    def test_unique_constraint(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DBDatasetTestCase.test_unique_constraint
    
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
        d.dimension_list.update_entry("Country", "AFG", "AFG")
        d.dimension_list.update_entry("Scale", "Billions", "Billions")

        datas = FakeDatas(provider_name="p1", 
                          dataset_code="d1",
                          fetcher=f)
        d.series.data_iterator = datas

        result = d.update_database()
        self.assertIsNotNone(result)
        
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 1)
                        
        with self.assertRaises(DuplicateKeyError):
            existing_dataset = dict(provider="p1", dataset_code="d1")
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
        d.dimension_list.update_entry("Scale", "Billions", "Billions")
        d.dimension_list.update_entry("country", "AFG", "AFG")

        datas = FakeDatas(provider_name="p1", 
                          dataset_code="d1",
                          fetcher=f)
        d.series.data_iterator = datas

        id = d.update_database()
        self.assertIsNotNone(id)
        self.assertIsInstance(id, ObjectId)
        self.db[constants.COL_DATASETS].find_one({'_id': ObjectId(id)})
        
        #print(result.raw)

        bson = self.db[constants.COL_DATASETS].find_one({"provider": "p1", "dataset_code": "d1"})
        self.assertIsNotNone(bson)
    
        self.assertEqual(bson["provider"], "p1")
        self.assertEqual(bson["dataset_code"], "d1")
        self.assertEqual(bson["name"], "d1 Name")
        self.assertEqual(bson["doc_href"], "http://www.example.com")
        self.assertTrue(isinstance(bson["dimension_list"], dict))
        self.assertTrue(isinstance(bson["attribute_list"], dict))

        series = self.db[constants.COL_SERIES].find({"provider": f.provider_name, 
                                                     "dataset_code": d.dataset_code})
        self.assertEqual(series.count(), datas.max_record)
        
        
class DBSeriesTestCase(BaseDBTestCase):
    
    #TODO: test indexes keys and properties
    def test_indexes(self):
        pass

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
        d.dimension_list.update_entry("Scale", "Billions", "Billions")
        d.dimension_list.update_entry("Country", "AFG", "AFG")
        
        s = Series(provider_name=f.provider_name, 
                   dataset_code=dataset_code, 
                   last_update=datetime(2013,10,28), 
                   bulk_size=1, 
                   fetcher=f)
        
        datas = FakeDatas(provider_name=provider_name, 
                          dataset_code=dataset_code,
                          fetcher=f)
        s.data_iterator = datas
        
        d.series = s
        d.update_database()        
        
        '''Count All series'''
        self.assertEqual(self.db[constants.COL_SERIES].count(), datas.max_record)

        '''Count series for this provider and dataset'''
        series = self.db[constants.COL_SERIES].find({"provider": f.provider_name, 
                                                     "dataset_code": dataset_code})
        self.assertEqual(series.count(), datas.max_record)

        tags.update_tags(self.db, 
                    provider_name=f.provider_name, dataset_code=dataset_code,  
                    col_name=constants.COL_SERIES)        

        '''Count series for this provider and dataset and in keys[]'''
        series = self.db[constants.COL_SERIES].find({"provider": f.provider_name, 
                                                     "dataset_code": dataset_code,
                                                     "key": {"$in": datas.keys}})
        
        self.assertEqual(series.count(), datas.max_record)
        

        for doc in series:
            self.assertTrue("tags" in doc)
            self.assertTrue(len(doc['tags']) > 0)
        
        
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
        d.dimension_list.update_entry("Scale", "Billions", "Billions")
        d.dimension_list.update_entry("Country", "AFG", "AFG")
        
        s1 = Series(provider_name=f.provider_name, 
                    dataset_code=dataset_code, 
                    last_update=datetime(2013,4,1), 
                    bulk_size=1, 
                    fetcher=f)
        datas1 = FakeDatas(provider_name=provider_name, 
                           dataset_code=dataset_code,
                           fetcher=f)
        s1.data_iterator = datas1

        d.series = s1
        d.update_database()        

        # A. modifying existing values
        test_key = datas1.rows[0]['key']
        first_series = self.db[constants.COL_SERIES].find_one({'key': test_key})

        s2 = Series(provider_name=f.provider_name, 
                    dataset_code=dataset_code, 
                    last_update=datetime(2014,4,1), 
                    bulk_size=1, 
                    fetcher=f)
        
        datas2 = FakeDatas(provider_name=provider_name, 
                           dataset_code=dataset_code,
                           fetcher=f)
        
        datas2.keys = datas1.keys
        
        for i,r in enumerate(datas2.rows):
            r['key'] = datas2.keys[i]
            r['frequency'] = datas1.rows[i]['frequency']
            r['start_date'] = datas1.rows[i]['start_date']
            r['end_date'] = datas1.rows[i]['end_date']
        datas2.rows[0]['values'] = deepcopy(datas1.rows[0]['values'])
        datas2.rows[0]['values'][1] = str(float(datas2.rows[0]['values'][1]) + 1.5)
        datas2.rows[0]['values'][8] = str(float(datas2.rows[0]['values'][8]) - 0.9)
        s2.data_iterator = datas2
        
        d.series = s2
        d.update_database()        

        self.assertEqual(self.db[constants.COL_SERIES].count(),datas1.max_record)
        test_key = datas2.keys[0]
        test_series = self.db[constants.COL_SERIES].find_one({'key': test_key})
        self.assertEqual(len(test_series['revisions']),2)
        self.assertEqual(test_series['revisions']['1'],[{'value': datas1.rows[0]['values'][1],'release_date':s1.last_update}])
        self.assertEqual(test_series['revisions']['8'],[{'value': datas1.rows[0]['values'][8],'release_date':s1.last_update}])
        self.assertEqual(test_series['release_dates'][1],datetime(2014,4,1))
        self.assertEqual(test_series['release_dates'][8],datetime(2014,4,1))
        self.assertEqual(test_series['release_dates'][0],datetime(2013,4,1))
        self.assertEqual(test_series['release_dates'][2:8],[datetime(2013,4,1) for i in range(6)])
        self.assertEqual(test_series['start_date'],datas1.rows[0]['start_date'])
        self.assertEqual(test_series['end_date'],datas1.rows[0]['end_date'])

        # B. adding observations at the beginning of the series
        s3 = Series(provider_name=f.provider_name, 
                    dataset_code=dataset_code, 
                    last_update=datetime(2014,4,1), 
                    bulk_size=1, 
                    fetcher=f)
        
        datas3 = FakeDatas(provider_name=provider_name, 
                           dataset_code=dataset_code,
                           fetcher=f)
        
        datas3.keys = datas1.keys
        
        for i,r in enumerate(datas3.rows):
            r['key'] = datas3.keys[i]
            r['frequency'] = datas1.rows[i]['frequency']
            r['start_date'] = datas1.rows[i]['start_date']
            r['end_date'] = datas1.rows[i]['end_date']
        
        datas3.rows[0]['start_date'] = datas1.rows[0]['start_date'] - 2;    
        datas3.rows[0]['values'] = [ '10', '10'] + datas1.rows[0]['values']
        datas3.rows[0]['values'][3] = str(float(datas3.rows[0]['values'][3]) + 1.5)
        datas3.rows[0]['values'][10] = str(float(datas3.rows[0]['values'][10]) - 0.9)
        s3.data_iterator = datas3
        
        d.series = s3
        d.update_database()        

        self.assertEqual(self.db[constants.COL_SERIES].count(),datas1.max_record)
        test_key = datas3.keys[0]
        test_series = self.db[constants.COL_SERIES].find_one({'key': test_key})
        self.assertEqual(len(test_series['revisions']),2)
        self.assertEqual(test_series['revisions']['3'],[{'value': datas1.rows[0]['values'][1],'release_date':s1.last_update}])
        self.assertEqual(test_series['revisions']['10'],[{'value': datas1.rows[0]['values'][8],'release_date':s1.last_update}])
        self.assertEqual(len(test_series['release_dates']),len(test_series['values']))
        self.assertEqual(test_series['release_dates'][3],datetime(2014,4,1))
        self.assertEqual(test_series['release_dates'][10],datetime(2014,4,1))
        self.assertEqual(test_series['release_dates'][0:2],[datetime(2014,4,1) for i in range(2)])
        self.assertEqual(test_series['release_dates'][2],datetime(2013,4,1))
        self.assertEqual(test_series['release_dates'][4:10],[datetime(2013,4,1) for i in range(6)])
        self.assertEqual(len(test_series['values']),11)
        self.assertEqual(len(test_series['release_dates']),11)
        self.assertEqual(test_series['start_date'],datas2.rows[0]['start_date']-2)
        self.assertEqual(test_series['end_date'],datas2.rows[0]['end_date'])
            
        # C. adding observations at the end of the series
        s4 = Series(provider_name=f.provider_name, 
                    dataset_code=dataset_code, 
                    last_update=datetime(2014,5,1), 
                    bulk_size=1, 
                    fetcher=f)
        
        datas4 = FakeDatas(provider_name=provider_name, 
                           dataset_code=dataset_code,
                           fetcher=f)

        datas4.keys = datas1.keys
        
        for i,r in enumerate(datas4.rows):
            r['key'] = datas4.keys[i]
            r['frequency'] = datas1.rows[i]['frequency']
            r['start_date'] = datas3.rows[i]['start_date']
            r['end_date'] = datas3.rows[i]['end_date']
        
        datas4.rows[0]['end_date'] = datas3.rows[0]['end_date'] + 1;    
        datas4.rows[0]['values'] = datas3.rows[0]['values'] + ['1.0']
        s4.data_iterator = datas4
        
        d.series = s4
        d.update_database()        

        self.assertEqual(self.db[constants.COL_SERIES].count(),datas1.max_record)
        test_key = datas4.keys[0]
        test_series = self.db[constants.COL_SERIES].find_one({'key': test_key})
        self.assertEqual(len(test_series['revisions']),2)
        self.assertEqual(len(test_series['values']),12)
        self.assertEqual(test_series['values'][11],'1.0')
        self.assertEqual(len(test_series['release_dates']),12)
        self.assertEqual(test_series['release_dates'][11],datetime(2014,5,1))
        self.assertEqual(test_series['start_date'],datas3.rows[0]['start_date'])
        self.assertEqual(test_series['end_date'],datas3.rows[0]['end_date']+1)
            
        # D. removing observations at the beginning and the end of the series
        s5 = Series(provider_name=f.provider_name, 
                    dataset_code=dataset_code, 
                    last_update=datetime(2014,6,1), 
                    bulk_size=1, 
                    fetcher=f)
        
        datas5 = FakeDatas(provider_name=provider_name, 
                           dataset_code=dataset_code,
                           fetcher=f)

        datas5.keys = datas1.keys
        
        for i,r in enumerate(datas5.rows):
            r['key'] = datas4.keys[i]
            r['frequency'] = datas1.rows[i]['frequency']
            r['start_date'] = datas4.rows[i]['start_date']
            r['end_date'] = datas4.rows[i]['end_date']
        
        datas5.rows[0]['start_date'] = datas4.rows[0]['start_date'] + 1;    
        datas5.rows[0]['end_date'] = datas4.rows[0]['end_date'] - 1;    
        datas5.rows[0]['values'] = datas4.rows[0]['values'][1:-1]
        s5.data_iterator = datas5
        
        d.series = s5
        d.update_database()        

        self.assertEqual(self.db[constants.COL_SERIES].count(),datas1.max_record)
        test_key = datas5.keys[0]
        test_series = self.db[constants.COL_SERIES].find_one({'key': test_key})
        self.assertEqual(len(test_series['revisions']),4)
        self.assertEqual(len(test_series['values']),12)
        self.assertEqual(test_series['values'][0],'na')
        self.assertEqual(test_series['values'][1],datas4.rows[0]['values'][1])
        self.assertEqual(test_series['values'][10],datas4.rows[0]['values'][-2])
        self.assertEqual(test_series['values'][11],'na')
        self.assertEqual(test_series['release_dates'][0],datetime(2014,6,1))
        self.assertEqual(test_series['release_dates'][1],datetime(2014,4,1))
        self.assertEqual(test_series['release_dates'][10],datetime(2014,4,1))
        self.assertEqual(test_series['release_dates'][11],datetime(2014,6,1))
        self.assertEqual(test_series['start_date'],datas4.rows[0]['start_date'])
        self.assertEqual(test_series['end_date'],datas4.rows[0]['end_date'])
                                                             
if __name__ == '__main__':
    unittest.main()
