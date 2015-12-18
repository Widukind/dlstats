# -*- coding: utf-8 -*-

from pprint import pprint
import os
import uuid
from datetime import datetime
from random import choice, randint

from dlstats import constants
from dlstats.fetchers._commons import (Fetcher, 
                                       Datasets)
from dlstats import utils

from dlstats.tests.base import BaseTestCase, BaseDBTestCase

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
            data = {'provider': self.provider_name, 
                    'datasetCode': self.dataset_code,
                    'key': key, 
                    'name': "%s %s - %s" % (self.dataset_code, dimensions['Country'], i),
                    'frequency': frequency,
                    'startDate': start_date,
                    'endDate': end_date,
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
                               db=self.db, es_client=self.es)
    
    def test_create_tag(self):
        
        # nosetests -s -v dlstats.tests.test_search:DBTagsTestCase.test_create_tag
        
        max_record = 10
        
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
        
        datas = FakeDatas(provider_name=d.provider_name, 
                          dataset_code=d.dataset_code,
                          max_record=max_record)
        d.series.data_iterator = datas
        
        id = d.update_database()
        
        doc = self.db[constants.COL_DATASETS].find_one({"_id": id})        
        self.assertIsNotNone(doc)
        
        tags = utils.generate_tags(self.db, doc, 
                                   doc_type=constants.COL_DATASETS)
        
        query = {"provider": d.provider_name, "datasetCode": d.dataset_code}
        series = self.db[constants.COL_SERIES].find(query)
        self.assertEqual(series.count(), max_record)
        
        self.assertListEqual(tags, sorted(['eurostat', 'name_a', 'billions', 'dollars', 'france', 'australie']))
        
        doc = series[0]
         
        tags = utils.generate_tags(self.db, doc, 
                                   doc_type=constants.COL_SERIES)

        self.assertTrue(len(tags) > 0)
        

    def test_update_tag(self):
        
        # nosetests -s -v dlstats.tests.test_search:DBTagsTestCase.test_update_tag
        
        max_record = 10
        
        d = Datasets(provider_name="eurostat", 
                    dataset_code="name_a",
                    name="Eurostat name_a",
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=self.fetcher, 
                    is_load_previous_version=False)
        
        d.dimension_list.update_entry("Country", "FRA", "France")
        d.dimension_list.update_entry("Scale", "Billions", "Billions Dollars")
        
        datas = FakeDatas(provider_name=d.provider_name, 
                          dataset_code=d.dataset_code,
                          max_record=max_record)
        d.series.data_iterator = datas
        id = d.update_database()

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

        doc = self.db[constants.COL_DATASETS].find_one({"_id": id})
        self.assertListEqual(doc['tags'], sorted(['eurostat', 'name_a', 'billions', 'dollars', 'france']))

        query = {"provider": d.provider_name, "datasetCode": d.dataset_code}
        series = self.db[constants.COL_SERIES].find(query)
        self.assertEqual(series.count(), max_record)
        
        for s in series:
            self.assertTrue(len(s['tags']) > 0)

            
                    
class DBTagsSearchTestCase(BaseDBTestCase):
    
    #TODO: refaire sans utiliser les fetchers - creation direct de docs dans Series et Datasets collections
    
    # nosetests -s -v dlstats.tests.test_search:DBTagsSearchTestCase
    
    def fixtures(self):

        fetcher = Fetcher(provider_name="p1", 
                               db=self.db, es_client=self.es)

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
        id = d.update_database()

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
        