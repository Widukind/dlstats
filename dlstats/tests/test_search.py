# -*- coding: utf-8 -*-

import uuid
from datetime import datetime
from random import choice, randint

from widukind_common import tags as utils

from dlstats import constants
from dlstats.fetchers._commons import (Fetcher, 
                                       Datasets, 
                                       SeriesIterator)

from dlstats.tests.base import BaseTestCase, BaseDBTestCase

import unittest

SERIES1 = {
    'provider_name': 'Eurostat',
    'dataset_code': 'name_a',
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
        'Country': 'AUS',
        'Scale': 'Billions',
    },
    'last_update': None,
    'start_date': 25, #1995
    'end_date': 44, #2014
    'frequency': 'A'           
}

SERIES2 = SERIES1.copy()
SERIES2["dimensions"]["Country"] = "FRA"

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
        #for key, dim in bson["dimensions"].items():
        #    self.dataset.dimension_list.update_entry(key, dim, key)
        return bson

class FakeDatas():
    """Fake data for series
    """
    
    def __init__(self, 
                 provider_name=None, dataset_code=None, 
                 max_record=10, fetcher=None, dimensions_generator=None):
        
        self.provider_name = provider_name
        self.dataset_code = dataset_code
        self.max_record = max_record
        self.fetcher = fetcher
        self.dimensions_generator = dimensions_generator
        
        self.rows = []
        self.keys = []
        self._create_fixtures()
        self._rows_iter = iter(self.rows)
        
    def _create_fixtures(self):
        for i in range(0, self.max_record):
            
            key = str(uuid.uuid4())
            self.keys.append(key)

            if self.dimensions_generator:
                dimensions = self.dimensions_generator()
            else:
                dimensions = {
                    'Country': choice(['FRA', 'AUS']), 
                }

            n = 9
            frequency = choice(['A', 'Q', 'M']) 
            start_date = randint(10, 100)
            end_date = start_date + n - 1
            data = {'provider_name': self.provider_name, 
                    'dataset_code': self.dataset_code,
                    'key': key, 
                    'name': "%s %s - %s" % (self.dataset_code, dimensions['Country'], i),
                    'frequency': frequency,
                    'start_date': start_date,
                    'end_date': end_date,
                    'values': [str(randint(i+1, 100)) for i in range(n)],
                    'attributes': {},
                    'revisions': {},
                    'dimensions': dimensions
            }
            self.rows.append(data)
    
    def __next__(self):
        row = next(self._rows_iter) 
        if row is None:
            raise StopIteration()
        return(row)
    

class TagsUtilsTestCase(BaseTestCase):

    # nosetests -s -v dlstats.tests.test_search:TagsUtilsTestCase
    
    def test_tags_filter(self):

        tags = ["the", "a", "france", "-", "quaterly"]
        result = sorted([a for a in filter(utils.tags_filter, tags)])
        self.assertEqual(result, ["france", "quaterly"])
        
    def test_tags_map(self):
        
        query = "The a France Quaterly"
        result = sorted(utils.tags_map(query))
        self.assertEqual(result, ["a", "france", "quaterly", "the"])
        
    def test_str_to_tags(self):
        
        self.assertEqual(utils.str_to_tags("Bank's of France"), ['bank', 'france'])        
        
        self.assertEqual(utils.str_to_tags("Bank's of & France"), ['bank', 'france'])
        
        self.assertEqual(utils.str_to_tags("France"), ['france'])
        
        self.assertEqual(utils.str_to_tags("Bank's"), ['bank'])

        self.assertEqual(utils.str_to_tags("The a France Quaterly"), ["france", "quaterly"])        
        

class DBTagsTestCase(BaseDBTestCase):
    
    # nosetests -s -v dlstats.tests.test_search:DBTagsTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)

        self.fetcher = Fetcher(provider_name="p1", 
                               db=self.db)
    
    def test_create_tag(self):
        
        # nosetests -s -v dlstats.tests.test_search:DBTagsTestCase.test_create_tag
        
        d = Datasets(provider_name="Eurostat", 
                    dataset_code="name_a",
                    name="Eurostat name_a",
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=self.fetcher, 
                    is_load_previous_version=False)
        d.dimension_list.update_entry("Country", "FRA", "France")
        d.dimension_list.update_entry("Country", "AUS", "Australie")
        d.dimension_list.update_entry("Scale", "Billions", "Billions Dollars")
        
        series_list = [SERIES1.copy()]
        datas = FakeSeriesIterator(d, series_list)
        d.series.data_iterator = datas
        _id = d.update_database()
        self.assertIsNotNone(_id)
        
        doc = self.db[constants.COL_DATASETS].find_one({"_id": _id})        
        self.assertIsNotNone(doc)
        
        tags = utils.generate_tags(self.db, doc, 
                                   doc_type=constants.COL_DATASETS)
        
        query = {'provider_name': d.provider_name, "dataset_code": d.dataset_code}
        series = self.db[constants.COL_SERIES].find(query)
        self.assertEqual(series.count(), len(series_list))
        
        self.assertEqual(sorted(tags), sorted(['eurostat', 'name_a', 'billions', 'dollars', 'france', 'australie']))
        
        doc = series[0]

        tags = utils.generate_tags(self.db, doc, doc_type=constants.COL_SERIES)
        self.assertEqual(sorted(tags), sorted(['billions', 'dollars', 'eurostat', 'france', 'key1', 'name_a', 'series1']))
        

    def test_update_tag(self):
        
        # nosetests -s -v dlstats.tests.test_search:DBTagsTestCase.test_update_tag
        
        d = Datasets(provider_name="Eurostat", 
                    dataset_code="name_a",
                    name="Eurostat name_a",
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=self.fetcher, 
                    is_load_previous_version=False)
        d.dimension_list.update_entry("Country", "FRA", "France")
        d.dimension_list.update_entry("Scale", "Billions", "Billions Dollars")
        
        series_list = [SERIES1.copy()]
        datas = FakeSeriesIterator(d, series_list)
        d.series.data_iterator = datas
        _id = d.update_database()
        self.assertIsNotNone(_id)

        utils.update_tags(self.db, 
                    provider_name=d.provider_name, 
                    dataset_code=d.dataset_code, 
                    col_name=constants.COL_DATASETS, 
                    max_bulk=20)
        
        utils.update_tags(self.db, 
                    provider_name=d.provider_name, 
                    dataset_code=d.dataset_code, 
                    col_name=constants.COL_SERIES, 
                    max_bulk=20)

        doc = self.db[constants.COL_DATASETS].find_one({"_id": _id})
        self.assertListEqual(sorted(doc['tags']), 
                             sorted(['eurostat', 'name_a', 'billions', 'dollars', 'france']))

        query = {'provider_name': d.provider_name, 
                 "dataset_code": d.dataset_code}
        series = self.db[constants.COL_SERIES].find(query)
        self.assertEqual(series.count(), len(series_list))

        doc = series[0]
        self.assertEqual(sorted(doc["tags"]), sorted(['billions', 'dollars', 'eurostat', 'france', 'key1', 'name_a', 'series1']))
                    
                    
@unittest.skipIf(True, "TODO")    
class DBTagsSearchTestCase(BaseDBTestCase):
    
    #TODO: refaire sans utiliser les fetchers - creation direct de docs dans Series et Datasets collections
    
    # nosetests -s -v dlstats.tests.test_search:DBTagsSearchTestCase
    
    def fixtures(self):

        fetcher = Fetcher(provider_name="p1", 
                               db=self.db)

        max_record = 10
        
        d = Datasets(provider_name="eurostat", 
                    dataset_code="name_a",
                    name="Eurostat name_a",
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=fetcher, 
                    is_load_previous_version=False)
        
        d.dimension_list.update_entry("Country", "FRA", "France")
        d.dimension_list.update_entry("Country", "AUS", "Australie")
        d.dimension_list.update_entry("Scale", "Billions", "Billions Dollars")
        d.dimension_list.update_entry("Scale", "Millions", "Millions Dollars")
        d.dimension_list.update_entry("Currency", "E", "Euro")
        d.dimension_list.update_entry("Currency", "D", "Dollars")
        d.dimension_list.update_entry("Sector", "agr", "Agriculture")
        d.dimension_list.update_entry("Sector", "ind", "Industrie")

        def dimensions_generator():
            return {
                'Country': choice(['FRA', 'AUS', 'FRA']),
                'Sector': choice(['agr', 'ind', 'agr']),
                'Currency': choice(['E', 'D', 'E']) 
            }
        
        datas = FakeDatas(provider_name=d.provider_name, 
                          dataset_code=d.dataset_code,
                          max_record=max_record,
                          dimensions_generator=dimensions_generator)
        d.series.data_iterator = datas
        _id = d.update_database()

        utils.update_tags(self.db, 
                    provider_name=d.provider_name, 
                    dataset_code=d.dataset_code, 
                    col_name=constants.COL_DATASETS, 
                    max_bulk=20)

        utils.update_tags(self.db, 
                    provider_name=d.provider_name, 
                    dataset_code=d.dataset_code, 
                    col_name=constants.COL_SERIES, 
                    max_bulk=20)

    
    def setUp(self):
        BaseDBTestCase.setUp(self)
        self.fixtures()
        
    def test_search_in_datasets(self):

        # nosetests -s -v dlstats.tests.test_search:DBTagsSearchTestCase.test_search_in_datasets
        
        self.db[constants.COL_DATASETS].reindex()

        query = dict(provider_name="eurostat", dataset_code="name_a")
        dataset = self.db[constants.COL_DATASETS].find_one(query)
        self.assertIsNotNone(dataset)
        
        docs, query = utils.search_datasets_tags(self.db, 
                                          search_tags="Australie Euro Agriculture")
        
        self.assertEqual(docs.count(), 1)
        
        '''Search Billion and Dollar - not plural'''
        docs, query = utils.search_datasets_tags(self.db, 
                                          search_tags="Billion Dollar")
        
        self.assertEqual(docs.count(), 1)
        
    def test_search_in_series(self):

        # nosetests -s -v dlstats.tests.test_search:DBTagsSearchTestCase.test_search_in_series
        
        self.db[constants.COL_SERIES].reindex()
        
        docs, query = utils.search_series_tags(self.db, 
                                        search_tags="Australie Euro Agriculture")
        
        #Not count because random datas
        
        #self.assertTrue(docs.count() > 0)
        
        '''Search Billion and Dollar - not plural'''
        docs, query = utils.search_datasets_tags(self.db, 
                                          search_tags="Billion Dollar")        
        
        #self.assertTrue(docs.count() > 0)
        