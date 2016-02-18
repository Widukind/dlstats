# -*- coding: utf-8 -*-

from copy import deepcopy
from datetime import datetime

from bson import ObjectId
from voluptuous import MultipleInvalid
from pymongo.errors import DuplicateKeyError

from dlstats import constants
from dlstats import errors
from dlstats.fetchers import schemas
from dlstats.fetchers._commons import (Fetcher, 
                                       CodeDict, 
                                       DlstatsCollection, 
                                       Providers,
                                       Categories,
                                       Datasets, 
                                       Series,
                                       SeriesIterator,
                                       series_is_changed,
                                       series_revisions,
                                       series_set_release_date,
                                       series_update)
from dlstats.fetchers.dummy import DUMMY, DUMMY_SAMPLE_SERIES

import unittest
from unittest import mock

from dlstats.tests.base import BaseTestCase, BaseDBTestCase

def update_mongo_collection(self, collection, keys, bson):
    pass

class FakeSeriesIterator(SeriesIterator):
    
    def __init__(self, dataset, series_list):
        super().__init__(dataset)
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
    'slug': 'p1-d1-key1',
    'values': [
        {
            'release_date': datetime(2015, 1, 1, 0, 0, 0),
            'ordinal': 25,
            #'period_o': '1995',
            'period': '1995',
            'value': '1.0',
            'attributes': {
                'OBS_STATUS': 'a'
            },
        },
        {
            'release_date': datetime(2015, 1, 1, 0, 0, 0),
            'ordinal': 44,
            #'period_o': '2014',
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
    'start_ts': datetime(1995, 1, 1, 0, 0),
    'end_ts': datetime(2014, 12, 31, 23, 59, 59, 999000),
    'frequency': 'A'
}

SERIES1_dataset_concepts = {
    "Country": "Country",
    "Scale": "Scale",
    "OBS_STATUS": "obs status"
}

SERIES1_dataset_codelists = {
    "Country": {"AFG": "Afg"},
    "Scale": {"Billions": "Billions"},
    "OBS_STATUS": {"a": "Normal"},
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

class CodeDictTestCase(BaseTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test__commons:CodeDictTestCase
    
    def test_update_entry(self):
        
        dimension_list = CodeDict()

        concept = dimension_list.update_entry('concept', 
                                              'short1', 
                                              "Long 1")
        self.assertEqual(concept, "short1")
        self.assertEqual(dimension_list.get_list(),
                         {'concept': [('short1', 'Long 1')]})
        

        dimension_list = CodeDict()
        concept = dimension_list.update_entry('concept', 
                                              None, 
                                              "Concept 1")
        
        self.assertEqual(concept, "0")
        
        concept = dimension_list.update_entry('concept', 
                                              '', 
                                              "Concept 2")
        
        self.assertEqual(concept, "1")
        
        self.assertEqual(dimension_list.get_list(),
                         {'concept': [('0', 'Concept 1'), ('1', 'Concept 2')]})
        


        dimension_list = CodeDict()
        concept = dimension_list.update_entry('concept', 
                                              None, 
                                              "Concept 1")
        concept = dimension_list.update_entry('concept', 
                                              None, 
                                              "Concept 1")
        self.assertEqual(dimension_list.get_list(),
                         {'concept': [('0', 'Concept 1')]})

class DlstatsCollectionTestCase(BaseTestCase):

    def test_constructor(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test__commons:DlstatsCollectionTestCase.test_constructor

        with self.assertRaises(ValueError):
            DlstatsCollection()

        with self.assertRaises(TypeError):
            DlstatsCollection(fetcher="abc")

        f = Fetcher(provider_name="test", is_indexes=False)
        DlstatsCollection(fetcher=f)

class ProvidersTestCase(BaseTestCase):

    def test_constructor(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:ProvidersTestCase.test_constructor

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

class DatasetsTestCase(BaseTestCase):
    
    def test_constructor(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DatasetsTestCase.test_constructor
        
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
        self.assertTrue(isinstance(bson["concepts"], dict))
        self.assertTrue(isinstance(bson["codelists"], dict))
        self.assertIsNone(bson["last_update"])
        self.assertEqual(bson["slug"], "p1-d1")

class SeriesIteratorTestCase(BaseTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test__commons:SeriesIteratorTestCase

    def test_constructor(self):

        with self.assertRaises(TypeError):
            SeriesIterator("")
            
        fetcher = Fetcher(provider_name="p1", is_indexes=False)
                
        dataset = Datasets(provider_name="p1", 
                    dataset_code="d1",
                    name="d1 Name",
                    fetcher=fetcher, 
                    is_load_previous_version=False)
        
        dclass = SeriesIterator(dataset)
        
        self.assertEqual(dclass.dataset_code, "d1")
        self.assertEqual(dclass.provider_name, "p1")
        self.assertIsNone(dclass.rows)

        with self.assertRaises(NotImplementedError):
            dclass.build_series({})

    def test_implement(self):
        
        fetcher = Fetcher(provider_name="p1", is_indexes=False)
                
        dataset = Datasets(provider_name="p1", 
                    dataset_code="d1",
                    name="d1 Name",
                    fetcher=fetcher, 
                    is_load_previous_version=False)

        class DataClass(SeriesIterator):
            
            def __init__(self, dataset):
                super().__init__(dataset)
                self.rows = self._process()
            
            def _process(self):
                yield {"key": "k1", "badfield": 1}, None
            
            def clean_field(self, bson):
                bson.pop("badfield")
                return bson
            
            def build_series(self, bson):
                return bson
        
        dclass = DataClass(dataset)
        bson = next(dclass)
        self.assertEqual(bson, {"key": "k1"})
        
        with self.assertRaises(StopIteration):
            next(dclass)
        
class SeriesTestCase(BaseTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test__commons:SeriesTestCase
    
    def test_constructor(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:SeriesTestCase.test_constructor

        with self.assertRaises(ValueError):
            Series()
            
        f = Fetcher(provider_name="p1", is_indexes=False)
            
        d = Datasets(provider_name="p1", 
                    dataset_code="d1",
                    name="d1 Name",
                    doc_href="http://www.example.com",
                    fetcher=f, 
                    is_load_previous_version=False)
            
        s = Series(dataset=d,
                   provider_name="p1", 
                   dataset_code="d1", 
                   last_update=None, 
                   bulk_size=1, 
                   fetcher=f)
        
        self.assertFalse(hasattr(s, "data_iterator"))

    def test_series_set_release_date(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:SeriesTestCase.test_series_set_release_date
        
        bson = None
        last_update = None
        with self.assertRaises(ValueError) as err:
            series_set_release_date(bson, last_update)
        self.assertEqual(str(err.exception), 
                         "no bson or not dict instance")
        
        bson = {"field": None}
        last_update = None
        with self.assertRaises(ValueError) as err:
            series_set_release_date(bson, last_update)
        self.assertEqual(str(err.exception), 
                         "no last_update or not datetime instance")

        bson = {"field": None}
        last_update = datetime.now()
        with self.assertRaises(ValueError) as err:
            series_set_release_date(bson, last_update)
        self.assertEqual(str(err.exception), 
                         "not values field in bson")

        '''Add release_date field to values'''
        bson = {
            "values": [
                {"period": "2000", "value": "1"},
                {"period": "2001", "value": "2"},
            ]
        }
        last_update = datetime.now()
        series_set_release_date(bson, last_update)
        self.assertTrue("release_date" in bson["values"][0])
        self.assertTrue("release_date" in bson["values"][1])
        self.assertEqual(bson["values"][0]["release_date"], last_update)
        self.assertEqual(bson["values"][1]["release_date"], last_update)

        '''Existing release_date field'''
        bson = {
            "values": [
                {"period": "2000", "value": "1", "release_date": datetime(2017, 1, 1, 0, 0, 0, 0, tzinfo=None)},
            ]
        }
        series_set_release_date(bson, last_update)
        self.assertTrue(bson["values"][0]["release_date"],
                        datetime(2017, 1, 1, 0, 0, 0, 0, tzinfo=None))


    def test_series_is_changed(self):

        old_bson = None
        new_bson = None
        with self.assertRaises(ValueError) as err:
            series_is_changed(new_bson, old_bson)
        self.assertEqual(str(err.exception), 
                         "no new_bson or not dict instance")

        old_bson = None
        new_bson = {"field": None}
        with self.assertRaises(ValueError) as err:
            series_is_changed(new_bson, old_bson)
        self.assertEqual(str(err.exception), 
                         "no old_bson or not dict instance")

        old_bson = {"field": None}
        new_bson = {"field": None}
        with self.assertRaises(ValueError) as err:
            series_is_changed(new_bson, old_bson)
        self.assertEqual(str(err.exception), 
                         "not values field in new_bson")
                
        old_bson = {"field": None}
        new_bson = {"values": None}
        with self.assertRaises(ValueError) as err:
            series_is_changed(new_bson, old_bson)
        self.assertEqual(str(err.exception), 
                         "not values field in old_bson")
        
        
        '''No change'''        
        old_bson = {
            "start_date": 10, "end_date": 20, "dimensions": {"a": "1"}, "attributes": None, "notes": None,
            "values": [{}],
        }
        new_bson = {
            "start_date": 10, "end_date": 20, "dimensions": {"a": "1"}, "attributes": None, "notes": None,
            "values": [{}],
        }
        self.assertFalse(series_is_changed(new_bson, old_bson))

        '''Add values entry'''
        #Already in revisions process - before run series_is_changed
        """
        old_bson = {
            "start_date": 10, "end_date": 20, "dimensions": {"a": "1"}, "attributes": None, "notes": None,
            "values": [{}],
        }
        new_bson = {
            "start_date": 10, "end_date": 20, "dimensions": {"a": "1"}, "attributes": None, "notes": None,
            "values": [{}, {}],
        }
        self.assertTrue(series_is_changed(new_bson, old_bson))
        """

        '''Change notes'''        
        old_bson = {
            "start_date": 10, "end_date": 20, "dimensions": {"a": "1"}, "attributes": None, "notes": None,
            "values": [{}],
        }
        new_bson = {
            "start_date": 10, "end_date": 20, "dimensions": {"a": "1"}, "attributes": None, "notes": "xxx",
            "values": [{}],
        }
        self.assertTrue(series_is_changed(new_bson, old_bson))

        '''Change dimension keys'''
        old_bson = {
            "start_date": 10, "end_date": 20, "dimensions": {"a": "1"}, "attributes": None, "notes": None,
            "values": [{}],
        }
        new_bson = {
            "start_date": 10, "end_date": 20, "dimensions": {"a": "1", "b": "2"}, "attributes": None, "notes": None,
            "values": [{}],
        }
        self.assertTrue(series_is_changed(new_bson, old_bson))

        '''Change dimension values'''        
        old_bson = {
            "start_date": 10, "end_date": 20, "dimensions": {"a": "1"}, "attributes": None, "notes": None,
            "values": [{}],
        }
        new_bson = {
            "start_date": 10, "end_date": 20, "dimensions": {"a": "20"}, "attributes": None, "notes": None,
            "values": [{}],
        }
        self.assertTrue(series_is_changed(new_bson, old_bson))
        
        '''Change attribute keys'''
        old_bson = {
            "start_date": 10, "end_date": 20, "dimensions": {"a": "1"}, "attributes": {"OBS_STATUS": "e"}, "notes": None,
            "values": [{}],
        }
        new_bson = {
            "start_date": 10, "end_date": 20, "dimensions": {"a": "1"}, "attributes": None, "notes": None,
            "values": [{}],
        }
        self.assertTrue(series_is_changed(new_bson, old_bson))
        
        '''Change attribute values'''        
        old_bson = {
            "start_date": 10, "end_date": 20, "dimensions": {"a": "1"}, "attributes": {"OBS_STATUS": "e"}, "notes": None,
            "values": [{}],
        }
        new_bson = {
            "start_date": 10, "end_date": 20, "dimensions": {"a": "1"}, "attributes": {"OBS_STATUS": "f"}, "notes": None,
            "values": [{}],
        }
        self.assertTrue(series_is_changed(new_bson, old_bson))

        '''Change start_date'''        
        old_bson = {
            "start_date": 10, "end_date": 20, "dimensions": {"a": "1"}, "attributes": None, "notes": None,
            "values": [{}],
        }
        new_bson = {
            "start_date": 5, "end_date": 20, "dimensions": {"a": "1"}, "attributes": None, "notes": None,
            "values": [{}],
        }
        self.assertTrue(series_is_changed(new_bson, old_bson))
        
        '''Change end_date'''        
        old_bson = {
            "start_date": 10, "end_date": 20, "dimensions": {"a": "1"}, "attributes": None, "notes": None,
            "values": [{}],
        }
        new_bson = {
            "start_date": 10, "end_date": 30, "dimensions": {"a": "1"}, "attributes": None, "notes": None,
            "values": [{}],
        }
        self.assertTrue(series_is_changed(new_bson, old_bson))

    def test_series_schema(self):

        bson = {
            'provider_name': "p1", 'dataset_code': "d1",
            'name': "name1", 'key': "key1", "slug": "p1-d1-key1",             
            'attributes': None,
            'dimensions': {"COUNTRY": "FRA"},
            'start_date': 30, 'end_date': 30,
            'start_ts': datetime(2000, 1, 1, 0, 0),
            'end_ts': datetime(2000, 12, 31, 23, 59, 59, 999999),
            'frequency': "A",
            'values': [
                {
                    "period": "2000", 
                    "value": "1", 
                    "ordinal": 30, "attributes": None,
                    "release_date": datetime(2017, 1, 1, 0, 0, 0, 0, tzinfo=None),
                    "revisions": [{
                        "revision_date": datetime(2016, 1, 1, 0, 0, 0, 0, tzinfo=None),
                        "value": "1",
                        "attributes": None
                    }]
                 }
            ],                
        }
        schemas.series_revision_schema(bson["values"][0]["revisions"][0])
        schemas.series_value_schema(bson["values"][0])
        schemas.series_schema(bson)

    def test_series_update(self):

        last_update = datetime(2017, 1, 1, 0, 0, 0, 0, tzinfo=None)
        
        '''No old_bson - insert'''
        new_bson = {
            'provider_name': "p1", 'dataset_code': "d1",
            'name': "name1", 'key': "key1", "slug": "p1-d1-key1",             
            'attributes': None,
            'dimensions': {"COUNTRY": "FRA"},
            'start_date': 30, 'end_date': 30,
            'start_ts': datetime(2000, 1, 1, 0, 0),
            'end_ts': datetime(2000, 12, 31, 23, 59, 59, 999999),
            'frequency': "A",
            'last_update': last_update,
            'values': [
                {"period": "2000", 
                 "value": "1", "ordinal": 30, "attributes": None}
            ],                
        }
        modify_bson = series_update(new_bson, last_update=last_update)
        self.assertIsNotNone(modify_bson)
        
        values_0 = modify_bson["values"][0]
        self.assertTrue("release_date" in values_0) 
        self.assertEqual(values_0["release_date"], last_update)


        '''old_bson without change - bypass'''
        new_bson = {
            'provider_name': "p1", 'dataset_code': "d1",
            'name': "name1", 'key': "key1", "slug": "p1-d1-key1",             
            'attributes': None,
            'dimensions': {"COUNTRY": "FRA"},
            'start_date': 30, 'end_date': 30,
            'start_ts': datetime(2000, 1, 1, 0, 0),
            'end_ts': datetime(2000, 12, 31, 23, 59, 59, 999999),
            'frequency': "A",
            'last_update': last_update,
            'values': [
                {"period": "2000", 
                 #"period_o": "2000", 
                 "value": "1", "ordinal": 30, "attributes": None}
            ],                
        }
        old_bson = deepcopy(new_bson)
        modify_bson = series_update(new_bson, old_bson=old_bson, last_update=last_update)
        self.assertIsNone(modify_bson)
        
    def test_series_revisions_exceptions(self):

        new_bson = None
        old_bson = None
        last_update = None
        with self.assertRaises(ValueError) as err:
            series_revisions(new_bson, old_bson, last_update)
        self.assertEqual(str(err.exception), 
                         "no new_bson or not dict instance")

        new_bson = {"field": None}
        old_bson = None
        last_update = None
        with self.assertRaises(ValueError) as err:
            series_revisions(new_bson, old_bson, last_update)
        self.assertEqual(str(err.exception), 
                         "no old_bson or not dict instance")

        new_bson = {"field": None}
        old_bson = {"field": None}
        last_update = None
        with self.assertRaises(ValueError) as err:
            series_revisions(new_bson, old_bson, last_update)
        self.assertEqual(str(err.exception), 
                         "no last_update or not datetime instance")

        new_bson = {"field": None}
        old_bson = {"field": None}
        last_update = datetime.now()
        with self.assertRaises(ValueError) as err:
            series_revisions(new_bson, old_bson, last_update)
        self.assertEqual(str(err.exception), 
                         "not values field in new_bson")
                
        new_bson = {"values": None}
        old_bson = {"field": None}
        last_update = datetime.now()
        with self.assertRaises(ValueError) as err:
            series_revisions(new_bson, old_bson, last_update)
        self.assertEqual(str(err.exception), 
                         "not values field in old_bson")

    def test_series_revisions_no_change(self):

        release_date = datetime(2015, 1, 1, 0, 0, 0, 0, tzinfo=None)
        last_update = datetime(2016, 1, 1, 0, 0, 0, 0, tzinfo=None)

        old_bson = {
            "values": [
                {"period": "2000", "value": "1", "ordinal": 30, 
                 "release_date": release_date, "attributes": None},
            ]
        }
        new_bson = {
            "values": [
                {"period": "2000", "value": "1", "ordinal": 30,
                 "release_date": release_date, "attributes": None},
            ]
        }
        
        changed = series_revisions(new_bson, old_bson, last_update)
        self.assertFalse(changed)


    def test_series_revisions_change_one_value(self):

        release_date = datetime(2015, 1, 1, 0, 0, 0, 0, tzinfo=None)
        last_update = datetime(2017, 1, 1, 0, 0, 0, 0, tzinfo=None)

        '''Change one value - add one revision'''
        old_bson = {
            "values": [
                {"period": "2000", "value": "1", "ordinal": 30, 
                 "release_date": release_date, "attributes": None},
            ]
        }
        new_bson = {
            "values": [
                {"period": "2000", "value": "1000", "ordinal": 30, 
                 "release_date": release_date, "attributes": None},
            ]
        }
        
        changed = series_revisions(new_bson, old_bson, last_update)
        self.assertTrue(changed)
        self.assertTrue("revisions" in new_bson["values"][0])
        self.assertEqual(len(new_bson["values"][0]["revisions"]), 1)
        revision_entry = {
            "revision_date": release_date,
            "value": "1",
            "attributes": None
        } 
        
        self.assertEqual(new_bson["values"][0]["value"], "1000")
        self.assertEqual(new_bson["values"][0]["release_date"], last_update)
        self.assertEqual(new_bson["values"][0]["revisions"][0], revision_entry)

    def test_series_more_values_in_old_json(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:SeriesTestCase.test_series_more_values_in_old_json

        release_date = datetime(2015, 1, 1, 0, 0, 0, 0, tzinfo=None)
        last_update = datetime(2017, 1, 1, 0, 0, 0, 0, tzinfo=None)
        
        '''Plus de values avant que apres ou plages diff - cas OECD/EO'''
        
        """
        cas EO: 
            2010 -> 2060 PUIS 2007 -> 2017
            Chevauchement de: 2010 -> 2017
            Result final: 2007 -> 2060
        """

        '''Change one value - add one revision AND add new value'''
        old_bson = {
            "start_date": 40, # 2010
            "end_date": 90,   # 2060
            "values": []
        }
        '''Generate period FROM 2010 TO 2060'''
        start_date = old_bson["start_date"] - 1 #39        
        for i in range(2010, 2060+1):
            start_date += 1
            old_bson["values"].append({
                "period": str(i), "value": "1", "ordinal": start_date, 
                "release_date": release_date, 
                "attributes": None
            })

        self.assertEqual(len(old_bson["values"]), 51)

        self.assertEqual(old_bson["values"][0]["period"], "2010")
        self.assertEqual(old_bson["values"][0]["ordinal"], 40)
        self.assertEqual(old_bson["values"][-1]["period"], "2060")
        self.assertEqual(old_bson["values"][-1]["ordinal"], 90)
                
        new_bson = {
            "start_date": 37, # 2007
            "end_date": 47,   # 2017
            "values": []
        }
        '''Generate period FROM 2007 TO 2017'''
        start_date = new_bson["start_date"] -1 #36
        for i in range(2007, 2017+1):
            start_date += 1 #37
            new_bson["values"].append({
                "period": str(i), "value": "1", "ordinal": start_date,
                "release_date": release_date, 
                "attributes": None
            })

        self.assertEqual(len(new_bson["values"]), 11)

        self.assertEqual(new_bson["values"][0]["period"], "2007")
        self.assertEqual(new_bson["values"][0]["ordinal"], 37)
        self.assertEqual(new_bson["values"][-1]["period"], "2017")
        self.assertEqual(new_bson["values"][-1]["ordinal"], 47)

        '''Change value for 2010 TO 2017'''
        for i in range(3, 11):
            new_bson["values"][i]["value"] = "2"
        
        '''Verify changes'''
        self.assertEqual(new_bson["values"][2]["period"], "2009")
        self.assertEqual(new_bson["values"][2]["value"], "1")
        self.assertEqual(new_bson["values"][3]["period"], "2010")
        self.assertEqual(new_bson["values"][3]["value"], "2")
        self.assertEqual(new_bson["values"][10]["period"], "2017")
        self.assertEqual(new_bson["values"][10]["value"], "2")

        changed = series_revisions(new_bson, old_bson, last_update)
        
        self.assertTrue(changed)
        #print()
        #for v in new_bson["values"]:
        #    print(v["period"], v["value"], "revisions" in v)
        """
        2007 1 False
        2008 1 False
        2009 1 False
        2010 2 True
        2011 2 True
        2012 2 True
        2013 2 True
        2014 2 True
        2015 2 True
        2016 2 True
        2017 2 True
        2018 1 False
        """

        self.assertEqual(len(new_bson["values"]), 54)
        self.assertTrue("revisions" in new_bson["values"][3])
        self.assertEqual(len(new_bson["values"][3]["revisions"]), 1)

        revision_entry = {
            "revision_date": release_date,
            "value": "1", #old value
            "attributes": None
        }
        self.assertEqual(new_bson["values"][3]["value"], "2")
        self.assertEqual(new_bson["values"][3]["release_date"], last_update)
        self.assertEqual(new_bson["values"][3]["revisions"][0], revision_entry)


    def test_series_revisions_change_one_value_add_existing_revision(self):

        release_date = datetime(2016, 1, 1, 0, 0, 0, 0, tzinfo=None)
        last_update = datetime(2017, 1, 1, 0, 0, 0, 0, tzinfo=None)

        '''Change one value - add revision entry'''

        old_bson = {
            "values": [
                {"period": "2000", "value": "10", "ordinal": 30, 
                 "release_date": release_date, "attributes": None,
                 "revisions": [{
                    "revision_date": datetime(2015, 1, 1, 0, 0, 0, 0, tzinfo=None),
                    "value": "5",
                    "attributes": None
                }]},
            ]
        }
        new_bson = {
            "values": [
                {"period": "2000", "value": "20", "ordinal": 30, 
                 "release_date": release_date, "attributes": None},
            ]
        }
        changed = series_revisions(new_bson, old_bson, last_update)

        self.assertTrue(changed)
        self.assertEqual(len(new_bson["values"][0]["revisions"]), 2)

        self.assertEqual(new_bson["values"][0]["value"], "20")
        revision_0 = new_bson["values"][0]["revisions"][0]
        revision_1 = new_bson["values"][0]["revisions"][1]
        self.assertEqual(revision_0["value"], "5")
        self.assertEqual(revision_1["value"], "10")
        
        self.assertEqual(revision_0["revision_date"].year, 2015)
        self.assertEqual(revision_1["revision_date"].year, 2016)

    def test_series_revisions_change_only_attribute(self):

        #FIXME: not only attribute change

        release_date = datetime(2015, 1, 1, 0, 0, 0, 0, tzinfo=None)
        last_update = datetime(2017, 1, 1, 0, 0, 0, 0, tzinfo=None)

        '''change only one attribute'''
        
        old_bson = {
            "values": [
                {"period": "2000", "value": "1", "ordinal": 30, 
                 "release_date": release_date, 
                 "attributes": {"OBS_STATUS": "e"}}, # estimated
            ]
        }
        new_bson = {
            "values": [
                {"period": "2000", "value": "10", "ordinal": 30,
                 "release_date": release_date, 
                 "attributes": None}, # fixe value
            ]
        }
        
        changed = series_revisions(new_bson, old_bson, last_update)
        self.assertTrue(changed)
        self.assertTrue("revisions" in new_bson["values"][0])
        self.assertEqual(len(new_bson["values"][0]["revisions"]), 1)

        self.assertIsNone(new_bson["values"][0]["attributes"])
        
        revision_0 = new_bson["values"][0]["revisions"][0]
        self.assertEqual(revision_0["attributes"], {"OBS_STATUS": "e"})

    @mock.patch("dlstats.fetchers._commons.DlstatsCollection.update_mongo_collection", update_mongo_collection)
    def test_process_series_data(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:SeriesTestCase.test_process_series_data

        f = Fetcher(provider_name="p1",
                    max_errors=1, 
                    is_indexes=False)

        dataset = Datasets(provider_name="p1", 
                    dataset_code="d1",
                    name="d1 Name",
                    last_update=datetime.now(),
                    fetcher=f, 
                    is_load_previous_version=False)
        
        class MockSeries(Series):
            def update_series_list(self):
                pass
            
        s = MockSeries(dataset=dataset,
                       provider_name="p1", 
                       dataset_code="d1", 
                       last_update=None, 
                       fetcher=f)
        
        class MyFetcher_Data(SeriesIterator):
            
            def rows_generator(self):
                yield None, errors.RejectFrequency(frequency="S")
                yield None, errors.RejectUpdatedSeries(key="series-updated")
                yield None, errors.RejectEmptySeries()
                yield SERIES1.copy(), None
                yield {}, Exception("NOT CAPTURED EXCEPTION")
                yield SERIES1, None
            
            def __init__(self, dataset):
                super().__init__(dataset)
                self.rows = self.rows_generator()
                
            def build_series(self, bson):
                return bson
        
        s.data_iterator = MyFetcher_Data(dataset)

        with self.assertRaises(Exception) as err:
            s.process_series_data()
        
        self.assertEqual(str(err.exception), 
                         "NOT CAPTURED EXCEPTION")
        
        self.assertEqual(len(s.series_list), 1)
        self.assertTrue(s.fatal_error)
        
        self.assertEqual(s.count_accepts, 1)
        self.assertEqual(s.count_rejects, 3)
        
        
class DB_IndexesTestCase(BaseDBTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test__commons:DB_IndexesTestCase

    @unittest.skipIf(True, "TODO")    
    def test_indexes_providers(self):
        pass

    @unittest.skipIf(True, "TODO")    
    def test_indexes_categories(self):
        pass
    
    def test_indexes_datasets(self):
        
        indexes = self.db[constants.COL_DATASETS].index_information()
        
        self.assertEqual(sorted(list(indexes.keys())),
                         ['_id_',
                          'enable_idx',
                          'last_update_idx',
                          'name_idx',
                          'provider_dataset_idx',
                          'provider_idx',
                          'slug_idx',
                          'tags_idx'])
        
        provider_dataset_idx = indexes["provider_dataset_idx"] 
        self.assertEqual(len(provider_dataset_idx['key']), 2)
        self.assertEqual(provider_dataset_idx['key'][0][0], "provider_name")
        self.assertEqual(provider_dataset_idx['key'][1][0], "dataset_code")
        self.assertTrue(provider_dataset_idx['unique'])

    @unittest.skipIf(True, "TODO")    
    def test_indexes_series(self):
        pass
    
class DB_FetcherTestCase(BaseDBTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test__commons:DB_FetcherTestCase
    
    @unittest.skipIf(True, "TODO")    
    def test_upsert_data_tree(self):
        pass
    
    @unittest.skipIf(True, "TODO")    
    def test_load_provider_from_db(self):
        pass

    @unittest.skipIf(True, "TODO")    
    def test_get_selected_datasets(self):
        pass

    @unittest.skipIf(True, "TODO")    
    def test_datasets_list(self):
        pass

    @unittest.skipIf(True, "TODO")    
    def test_provider(self):
        pass

    @unittest.skipIf(True, "TODO")    
    def test_upsert_all_datasets(self):
        pass

    @unittest.skipIf(True, "TODO")    
    def test_get_ordinal_from_period(self):
        pass

    @unittest.skipIf(True, "TODO")    
    def test_wrap_upsert_dataset(self):
        pass

    @unittest.skipIf(True, "TODO")    
    def test__hook_remove_temp_files(self):
        pass
    
    @unittest.skipIf(True, "TODO")    
    def test_hook_before_dataset(self):
        pass
    
    @unittest.skipIf(True, "TODO")    
    def test_hook_after_dataset(self):
        pass
    
    @unittest.skipIf(True, "TODO")    
    def test_load_datasets_first(self):
        pass
    
    @unittest.skipIf(True, "TODO")    
    def test_load_datasets_update(self):
        pass
    
    @unittest.skipIf(True, "TODO")    
    def test_build_data_tree(self):
        pass
    
    @unittest.skipIf(True, "TODO")    
    def test_get_calendar(self):
        pass
    
    @unittest.skipIf(True, "TODO")    
    def test_upsert_dataset(self):
        pass

class DB_DlstatsCollectionTestCase(BaseDBTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test__commons:DB_DlstatsCollectionTestCase
    
    @unittest.skipIf(True, "TODO")    
    def test_update_mongo_collection(self):
        pass
    
class DB_ProvidersTestCase(BaseDBTestCase):

    @unittest.skipIf(True, "TODO")    
    def test_schema(self):
        pass

    def test_unique_constraint(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DB_ProvidersTestCase.test_unique_constraint

        f = Fetcher(provider_name="p1", 
                    db=self.db)

        p = Providers(name="p1", 
                      long_name="Provider One",
                      version=1,
                      region="Dreamland",
                      website="http://www.example.com", 
                      fetcher=f)
        
        #f.provider = p

        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 0)
        
        result = p.update_database()
        self.assertIsNotNone(result)
        
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

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DB_ProvidersTestCase.test_version_field

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

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DB_ProvidersTestCase.test_update_database

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

class DB_CategoriesTestCase(BaseDBTestCase):

    @unittest.skipIf(True, "TODO")    
    def test_schema(self):
        pass

    def test_unique_constraint(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DB_CategoriesTestCase.test_unique_constraint
    
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

    @unittest.skipIf(True, "TODO")
    def test_slug(self):
        pass

    @unittest.skipIf(True, "TODO")
    def test_bson(self):
        pass

    @unittest.skipIf(True, "TODO")
    def test_categories(self):
        pass

    @unittest.skipIf(True, "TODO")
    def test_count(self):
        pass

    @unittest.skipIf(True, "TODO")
    def test_remove_all(self):
        pass

    @unittest.skipIf(True, "TODO")
    def test_search_category_for_dataset(self):
        pass

    @unittest.skipIf(True, "TODO")
    def test_root_categories(self):
        pass

    @unittest.skipIf(True, "TODO")
    def test_iter_parent(self):
        pass

    @unittest.skipIf(True, "TODO")
    def test_update_database(self):
        pass

    def test_add_categories(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test__commons:DB_CategoriesTestCase.test_add_categories

        f = Fetcher(provider_name="p1", 
                    is_indexes=False, 
                    db=self.db)

        p = Providers(name="p1",
                      long_name="Provider One",
                      version=1,
                      region="Dreamland",
                      website="http://www.example.com", 
                      fetcher=f)        
        p.update_database()
        
        minimal_category = { 'category_code': "c0", 'name': "p1"}
        result = f.upsert_data_tree([minimal_category])
        self.assertEqual(len(result), 1)

        cat = self.db[constants.COL_CATEGORIES].find_one({"_id": result[0]})
        self.assertIsNotNone(cat)
        
        _categories = {'c0': {'all_parents': None,
                'category_code': 'c0',
                'datasets': [],
                'doc_href': None,
                'enable': True,
                'lock': False,
                'metadata': None,
                'name': 'p1',
                'parent': None,
                'position': 0,
                'provider_name': 'p1',
                'slug': 'p1-c0'}}        
        
        cats = Categories.categories(f.provider_name, db=self.db)
        self.assertEqual(len(cats), 1)
        self.assertTrue("c0" in cats)
        cats["c0"].pop("_id")
        self.assertEqual(cats, _categories)


class DB_DatasetsTestCase(BaseDBTestCase):

    @unittest.skipIf(True, "TODO")    
    def test_schema(self):
        pass

    def test_unique_constraint(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DB_DatasetsTestCase.test_unique_constraint
    
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

    @unittest.skipIf(True, "TODO")
    def test_load_previous_version(self):
        pass

    @unittest.skipIf(True, "TODO")
    def test_is_recordable(self):
        pass

    @unittest.skipIf(True, "TODO")
    def test_add_frequency(self):
        pass

    def test_update_database(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DB_DatasetsTestCase.test_update_database

        f = Fetcher(provider_name="p1", 
                    db=self.db)

        f.provider = Providers(name="p1",
                      long_name="Provider One",
                      version=1,
                      region="Dreamland",
                      website="http://www.example.com", 
                      fetcher=f)
        f.provider.update_database()

        d = Datasets(provider_name="p1", 
                    dataset_code="d1",
                    name="d1 Name",
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=f, 
                    is_load_previous_version=False)

        d.concepts = SERIES1_dataset_concepts
        d.codelists = SERIES1_dataset_codelists

        series_list = [SERIES1.copy()]
        datas = FakeSeriesIterator(d, series_list)
        d.series.data_iterator = datas

        _id = d.update_database()
        self.assertIsNotNone(_id)
        self.assertIsInstance(_id, ObjectId)
        
        self.assertTrue(d.enable)
        self.assertEqual(d.series.count_accepts, 1)
        self.assertEqual(d.series.count_inserts, 1)
        
        self.db[constants.COL_DATASETS].find_one({'_id': _id})
        
        bson = self.db[constants.COL_DATASETS].find_one({'provider_name': "p1", 
                                                         "dataset_code": "d1"})
        self.assertIsNotNone(bson)
    
        self.assertEqual(bson['provider_name'], "p1")
        self.assertEqual(bson["dataset_code"], "d1")
        self.assertEqual(bson["name"], "d1 Name")
        self.assertEqual(bson["doc_href"], "http://www.example.com")
        self.assertTrue(isinstance(bson["concepts"], dict))
        self.assertTrue(isinstance(bson["codelists"], dict))

        count = self.db[constants.COL_SERIES].count({'provider_name': f.provider_name, 
                                                     "dataset_code": d.dataset_code})
        self.assertEqual(count, 1)

    def test_not_recordable_dataset(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DB_DatasetsTestCase.test_not_recordable_dataset

        f = Fetcher(provider_name="p1",
                    max_errors=1, 
                    db=self.db)
        f.provider = Providers(name="p1",
                      long_name="Provider One",
                      version=1,
                      region="Dreamland",
                      website="http://www.example.com", 
                      fetcher=f)
        f.provider.update_database()

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
                
class DB_SeriesTestCase(BaseDBTestCase):

    @unittest.skipIf(True, "TODO")    
    def test_schema(self):
        pass

    def test_reject_series(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DB_SeriesTestCase.test_reject_series

        class MyFetcher_Data(SeriesIterator):
            
            def rows_generator(self):
                yield None, errors.RejectFrequency(frequency="S")
                yield None, errors.RejectUpdatedSeries(key="series-updated")
                yield None, errors.RejectEmptySeries()
                yield SERIES1.copy(), None
                yield {}, Exception("NOT CAPTURED EXCEPTION")
                yield SERIES1.copy(), None
            
            def __init__(self, dataset):
                super().__init__(dataset)
                self.rows = self.rows_generator()
                
            def build_series(self, bson):
                bson['last_update'] = self.dataset.last_update
                return bson
        
        fetcher = Fetcher(provider_name="test",                           
                          db=self.db,
                          max_errors=1)

        fetcher.provider = Providers(name="p1",
                      long_name="Provider One",
                      version=1,
                      region="Dreamland",
                      website="http://www.example.com", 
                      fetcher=fetcher)
        fetcher.provider.update_database()
        
        dataset = Datasets(provider_name="p1", 
                           dataset_code="d1",
                           name="d1 Name",
                           last_update=datetime.now(),
                           fetcher=fetcher, 
                           is_load_previous_version=False)
        
        dataset.series.data_iterator = MyFetcher_Data(dataset)
        s = dataset.series
        s.bulk_size = 1

        result = dataset.update_database()
        self.assertIsNotNone(result)

        self.assertTrue(s.fatal_error)
        self.assertEqual(s.count_accepts, 1)
        self.assertEqual(s.count_rejects, 3)
        self.assertEqual(s.count_updates, 0)
        self.assertEqual(s.count_inserts, 1)
        
        self.assertEqual(self.db[constants.COL_SERIES].count(), 1)
        

        """
        TODO: capturer logs
        dlstats.fetchers._commons: WARNING: Reject empty series for dataset[d1]
        dlstats.fetchers._commons: DEBUG: Reject series updated for dataset[d1] - key[series-updated]
        dlstats.fetchers._commons: WARNING: Reject frequency for dataset[d1] - frequency[S]        
        """
        self.assertEqual(self.db[constants.COL_SERIES].count(), 1)
        
        datasets = self.db[constants.COL_DATASETS].find()
        self.assertEqual(datasets.count(), 1)
        self.assertFalse(datasets[0]["enable"]) 

    def test_update_series_list(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test__commons:DB_SeriesTestCase.test_update_series_list

        provider_name = "p1"
        dataset_code = "d1"
        dataset_name = "d1 name"
    
        f = Fetcher(provider_name=provider_name, 
                    db=self.db)

        f.provider = Providers(name="p1",
                      long_name="Provider One",
                      version=1,
                      region="Dreamland",
                      website="http://www.example.com", 
                      fetcher=f)
        f.provider.update_database()

        d = Datasets(provider_name=provider_name, 
                    dataset_code=dataset_code,
                    name=dataset_name,
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=f, 
                    is_load_previous_version=False)
        
        s = Series(dataset=d,
                   provider_name=f.provider_name, 
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

    @unittest.skipIf(True, "TODO")    
    def test_series_update_dataset_lists(self):

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DB_SeriesTestCase.test_series_update_dataset_lists

        provider_name = "p1"
        dataset_code = "d1"
        dataset_name = "d1 name"
    
        f = Fetcher(provider_name=provider_name, 
                    db=self.db)

        f.provider = Providers(name="p1",
                      long_name="Provider One",
                      version=1,
                      region="Dreamland",
                      website="http://www.example.com", 
                      fetcher=f)
        f.provider.update_database()

        dataset = Datasets(provider_name=provider_name, 
                    dataset_code=dataset_code,
                    name=dataset_name,
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=f, 
                    is_load_previous_version=False)
        
        s = Series(dataset=dataset,
                   provider_name=f.provider_name, 
                   dataset_code=dataset_code, 
                   last_update=datetime(2013,10,28), 
                   bulk_size=1, 
                   fetcher=f)

        dataset.concepts = {
            "COUNTRY": "Country",
            "FREQ": "Frequency",
            "COLLECTION": "Collection Indicator",                     
            "DECIMALS": "Decimals",                     
            "OBS_STATUS": "Observation status",                     
            "OBS_CONF": "Observation confidentiality",                     
        }
        
        dataset.codelists = {
            "COUNTRY": {"FRA": "France", "AUS": "Australia"},
            "FREQ": {"Q": "Quarterly", "M": "Monthly"},
            "COLLECTION": {"S": "Summed through period", "E": "End of period"},
            "DECIMALS": {"15": "Fifteen", "6": "Six"},
            "OBS_STATUS": {"E": "Estimated value", "U": "Low reliability"},                             
            "OBS_CONF": {"C": "Confidential statistical information", "F": "Free"},
        }
        
        dataset.dimension_keys = ["COUNTRY"]

        series1 = SERIES1.copy()
        series1["dimensions"] = {
            "COUNTRY": "FRA"
        }
        series1["attributes"] = {
            "COLLECTION": "S"                     
        }
        series1["values"][0]["attributes"] = {
            "OBS_STATUS": "E"
        }

        series_list = [series1]
        datas = FakeSeriesIterator(dataset, series_list)
        s.data_iterator = datas
        
        dataset.series = s
        dataset.update_database()        

        #self.assertEqual(s.dimension_keys, ["COUNTRY"])
        #self.assertEqual(s.attribute_keys, ["COLLECTION", "OBS_STATUS"])
        
        '''Count All series'''
        self.assertEqual(self.db[constants.COL_SERIES].count(), len(series_list))
        
        self.assertEqual(dataset.concepts, {
            "COUNTRY": "Country",
            "COLLECTION": "Collection Indicator",                     
            "OBS_STATUS": "Observation status",                     
        })
        
        self.assertEqual(dataset.codelists, {
            "COUNTRY": {"FRA": "France"},
            "COLLECTION": {"S": "Summed through period"},
            "OBS_STATUS": {"E": "Estimated value"},                             
        })
        
        self.assertEqual(dataset.dimension_keys, ["COUNTRY"])
        self.assertEqual(dataset.attribute_keys, ["COLLECTION", "OBS_STATUS"])


    def test_revisions(self):        

        # nosetests -s -v dlstats.tests.fetchers.test__commons:DB_SeriesTestCase.test_revisions

        provider_name = "p1"
        dataset_code = "d1"
        dataset_name = "d1 name"
    
        f = Fetcher(provider_name=provider_name, 
                    db=self.db)

        f.provider = Providers(name="p1",
                      long_name="Provider One",
                      version=1,
                      region="Dreamland",
                      website="http://www.example.com", 
                      fetcher=f)
        f.provider.update_database()

        d = Datasets(provider_name=provider_name, 
                    dataset_code=dataset_code,
                    name=dataset_name,
                    last_update=datetime.now(),
                    doc_href="http://www.example.com",
                    fetcher=f, 
                    is_load_previous_version=False)
        
        s1 = Series(dataset=d,
                    provider_name=f.provider_name, 
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
        old_value = SERIES1["values"][0]["value"] #1.0
        old_release_date = SERIES1["values"][0]["release_date"]

        SERIES2 = SERIES1.copy()
        SERIES2["values"][0]["value"] = "10"

        s1.last_update = datetime(old_release_date.year+1, 
                                  old_release_date.month, 
                                  old_release_date.day)

        SERIES2["last_update"] = s1.last_update

        def _iter():
            yield SERIES2

        s1.data_iterator = _iter()
        d.series = s1
        d.update_database()
        
        bson = self.db[constants.COL_SERIES].find_one({'key': test_key})

        self.assertTrue("revisions" in bson["values"][0])
        
        self.assertEqual(bson["values"][0]["value"], "10")
        self.assertEqual(bson["values"][0]["release_date"], datetime(2016, 1, 1, 0, 0))
        
        self.assertEqual(len(bson["values"][0]["revisions"]), 1)        
        self.assertEqual(bson["values"][0]["revisions"][0]["value"], old_value)
        self.assertEqual(bson["values"][0]["revisions"][0]["revision_date"], old_release_date)
        
class DB_DummyTestCase(BaseDBTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test__commons:DB_DummyTestCase
    
    def test_upsert_dataset(self):
        
        fetcher = DUMMY(db=self.db)
        
        datatree = fetcher.build_data_tree()
        
        self.assertEqual(datatree,
                        [{'category_code': 'c1',
                          'datasets': [{'dataset_code': 'ds1',
                                        'last_update': None,
                                        'metadata': None,
                                        'name': 'My Dataset Name'}],
                          'doc_href': 'http://www.example.org/c1',
                          'name': 'category 1'}])

        result = fetcher.upsert_dataset("ds1")
        self.assertIsNotNone(result)
        
        query = {"provider_name": fetcher.provider_name,
                 "dataset_code": "ds1"}
        cursor = self.db[constants.COL_SERIES].find(query)
        count = cursor.count()
        self.assertEqual(count, len(DUMMY_SAMPLE_SERIES))
        
        series = cursor[0]
        
        self.maxDiff = None
        
        series.pop('_id')
        for v in series["values"]:
            v.pop("release_date")
        
        bson = {
         'attributes': None,
         'dataset_code': 'ds1',
         'dimensions': {'COUNTRY': 'FRA'},
         'frequency': 'A',
         'key': 'key1',
         'name': 'name1',
         'provider_name': 'DUMMY',
         'slug': 'dummy-ds1-key1',
         'start_date': 30,
         'end_date': 31,
         'start_ts': datetime(2000, 1, 1, 0, 0),
         'end_ts': datetime(2001, 12, 31, 23, 59, 59, 999000), #FIXME: bug mongo
         'values': [{'attributes': {'OBS_STATUS': 'A'},
                     'ordinal': 30,
                     'period': '2000',
                     #'release_date': datetime(2016, 2, 8, 9, 35, 16),
                     'value': '1'},
                    {'attributes': None,
                     'ordinal': 31,
                     'period': '2001',
                     #'release_date': datetime(2016, 2, 8, 9, 35, 16),
                     'value': '10'}]}        
        #from pprint import pprint
        #print()
        #pprint(series)
        self.assertEqual(series, bson)