# -*- coding: utf-8 -*-

from pprint import pprint
import os
import uuid
from datetime import datetime
from random import choice, randint

from dlstats import constants
from dlstats.fetchers._commons import (Fetcher, 
                                       CodeDict, 
                                       DlstatsCollection, 
                                       Providers,
                                       Categories, 
                                       Datasets, 
                                       Series)
from dlstats import utils

import unittest

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
                    'name': "%s %s" % (self.dataset_code, dimensions['Country']),
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
    


class DBTagsTestCase(BaseDBTestCase):
    
    # nosetests -s -v dlstats.tests.test_search:DBTagsTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)

        self.fetcher = Fetcher(provider_name="p1", 
                               db=self.db, es_client=self.es)
    
    def test_create_tag(self):
        
        # nosetests -s -v dlstats.tests.test_search:DBTagsTestCase.test_create_tag
        
        max_record = 10
        
        d = Datasets(provider_name="eurostat", 
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
        
        #print("")
        #print("DATASET : ")
        #pprint(doc)
        """
        {'_id': ObjectId('565380433cbf6e3bcd06adf1'),
         'attributeList': {},
         'datasetCode': 'name_a',
         'dimensionList': {'Country': [['FRA', 'France']],
                           'Scale': [['Billions', 'Billions Dollars']]},
         'docHref': 'http://www.example.com',
         'lastUpdate': datetime.datetime(2015, 11, 23, 22, 8, 19, 206000),
         'name': 'Eurostat name_a',
         'notes': '',
         'provider': 'eurostat'}        
        """
        self.assertListEqual(tags, sorted(['eurostat', 'name_a', 'billions', 'dollars', 'france', 'australie']))
        
        #print("tags : ", tags)

        doc = series[0]
        #print("SERIES : ")
        #pprint(doc)

        """
        {'_id': ObjectId('565380432d4b2530c40fa519'),
         'attributes': {},
         'datasetCode': 'name_a',
         'dimensions': {'Country': 'FRA'},
         'endDate': 105,
         'frequency': 'A',
         'key': '0c11df88-de8f-4fc9-99aa-5a7f5642206d',
         'name': 'name_a FRA 3',
         'provider': 'eurostat',
         'releaseDates': [datetime.datetime(2015, 11, 23, 22, 8, 19, 206000),
                          datetime.datetime(2015, 11, 23, 22, 8, 19, 206000),
                          datetime.datetime(2015, 11, 23, 22, 8, 19, 206000),
                          datetime.datetime(2015, 11, 23, 22, 8, 19, 206000),
                          datetime.datetime(2015, 11, 23, 22, 8, 19, 206000),
                          datetime.datetime(2015, 11, 23, 22, 8, 19, 206000),
                          datetime.datetime(2015, 11, 23, 22, 8, 19, 206000),
                          datetime.datetime(2015, 11, 23, 22, 8, 19, 206000),
                          datetime.datetime(2015, 11, 23, 22, 8, 19, 206000)],
         'revisions': {},
         'startDate': 97,
         'values': ['74', '48', '52', '99', '83', '68', '31', '43', '75']}
        """
         
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
        
    def test_search1(self):

        # nosetests -s -v dlstats.tests.test_search:DBTagsSearchTestCase.test_search1
        
        """
        FIXME: Refaire avec des doc créés directement sans passer par Fetcher !
        
        
        # Search in all provider and dataset
        >>> docs = utils.search_series_tags(db, frequency="A", search_tags=["Belgium", "Euro", "Agriculture"])
    
        # Filter provider and/or dataset
        >>> docs = utils.search_series_tags(db, provider_name="Eurostat", dataset_code="nama_10_a10", search_tags=["Belgium", "Euro", "Agriculture"])
            
        1 - euro industrial production quarterly : je sais exactement ce que je veux
        2 - euro industrial production : je ne sais pas quelles fréquences sont disponibles
        3 - euro industrial production quarterly monthly : je veux deux fréquences pour comparer les deux séries
        
        """
        
        self.db[constants.COL_SERIES].reindex()
        
        frequencies = self.db[constants.COL_SERIES].distinct("frequency")
        frequency_stats = {}
        for f in frequencies:
            frequency_stats[f] = self.db[constants.COL_SERIES].count({"frequency": f})
        print(frequency_stats)
        
        #doc = self.db[constants.COL_SERIES].find()[0]
        
        #doc = self.db[constants.COL_SERIES].find()        
        docs = utils.search_series_tags(self.db, 
                                        #frequency="A", 
                                        search_tags=["Australie", "Euro", "Agriculture"])
        
        print("COUNT : ", docs.count())
        print("SEARCH : ", ["Australie", "Euro", "Agriculture"])
        for doc in docs:
            print("found : ", doc['name'], doc['tags'], doc['dimensions'])
        
        #pprint(docs.explain())
        """
        'filter': {'$and': [{'tags': {'$eq': 'euro'}},
                            {'tags': {'$eq': 'agriculture'}}]},        
        """
        for doc in self.db[constants.COL_SERIES].find():
            print(doc['tags'], doc['dimensions'])
        