# -*- coding: utf-8 -*-

from datetime import datetime
import os
from pprint import pprint

import pandas

from dlstats.fetchers._commons import Datasets
from dlstats.fetchers.insee import INSEE, INSEE_Data, ContinueRequest
from dlstats import constants

import unittest
from unittest import mock
import httpretty

from dlstats.tests.base import RESOURCES_DIR, BaseDBTestCase

"""
Load files with httpie tools:
    http http://www.bdm.insee.fr/series/sdmx/dataflow references==all Accept:application/xml Content-Type:application/xml > insee-dataflow.xml
    http http://www.bdm.insee.fr/series/sdmx/datastructure/FR1/IPI-2010-A21 references==all Accept:application/xml Content-Type:application/xml > insee-IPI-2010-A21-datastructure.xml
    http http://www.bdm.insee.fr/series/sdmx/data/CNA-2010-CONSO-SI-A17 Accept:application/vnd.sdmx.genericdata+xml;version=2.1 > insee-IPI-2010-A21-data.xml
    
    http http://www.bdm.insee.fr/series/sdmx/datastructure/FR1/CNA-2010-CONSO-SI-A17 references==all Accept:application/xml Content-Type:application/xml > insee-bug-data-namedtuple-datastructure.xml
    
"""
DATAFLOW_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "insee-dataflow.xml"))

DATASETS = {
    'IPI-2010-A21': {
        'data-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "insee-IPI-2010-A21-data.xml")),
        'datastructure-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "insee-IPI-2010-A21-datastructure.xml")),
        'series_count': 20,
    },
    'CNA-2010-CONSO-SI-A17': {
        'data-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "insee-bug-data-namedtuple.xml")),
        'datastructure-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "insee-bug-data-namedtuple-datastructure.xml")),
        'series_count': 1,
    },
}

class MockINSEE_Data(INSEE_Data):
    
    def __init__(self, **kwargs):
        self._series = []
        super().__init__(**kwargs)

    def __next__(self):          
        try:      
            _series = next(self.rows)
            if not _series:
                raise StopIteration()
        except ContinueRequest:
            _series = next(self.rows)
            
        bson = self.build_series(_series)
        self._series.append(bson)
        return bson
    
def mock_upsert_dataset(self, dataset_code):

    self.load_structure(force=False)
    
    if not dataset_code in self._dataflows:
        raise Exception("This dataset is unknown" + dataset_code)
    
    dataflow = self._dataflows[dataset_code]
    
    dataset = Datasets(provider_name=self.provider_name, 
                       dataset_code=dataset_code,
                       name=dataflow.name.en,
                       doc_href=None,
                       last_update=datetime(2015, 12, 24),
                       fetcher=self)
    
    query = {"provider": self.provider_name, "datasetCode": dataset_code}
    dataset_doc = self.db[constants.COL_DATASETS].find_one(query)
    
    self.insee_data = MockINSEE_Data(dataset=dataset,
                                     dataset_doc=dataset_doc, 
                                     dataflow=dataflow, 
                                     sdmx=self.sdmx)
    dataset.series.data_iterator = self.insee_data
    result = dataset.update_database()

    return result


class InseeTestCase(BaseDBTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_insee:InseeTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)
        self.insee = INSEE(db=self.db)
        
    def _load_dataset(self, dataset_code):

        def _body(filepath):
            '''body for large file'''
            with open(filepath, 'rb') as fp:
                for line in fp:
                    yield line        
        
        url_dataflow = "http://www.bdm.insee.fr/series/sdmx/dataflow/INSEE"
        httpretty.register_uri(httpretty.GET, 
                               url_dataflow,
                               body=_body(DATAFLOW_FP),
                               match_querystring=True,
                               status=200,
                               streaming=True,
                               content_type="application/xml")
        
        self.insee._dataflows = self.insee.sdmx.get(resource_type='dataflow').msg.dataflows
        
        url_datastructure = "http://www.bdm.insee.fr/series/sdmx/datastructure/INSEE/%s" % dataset_code
        httpretty.register_uri(httpretty.GET, 
                               url_datastructure,
                               body=_body(DATASETS[dataset_code]['datastructure-fp']),
                               streaming=True,
                               status=200,
                               content_type="application/xml")

        url_data = "http://www.bdm.insee.fr/series/sdmx/data/%s" % dataset_code
        httpretty.register_uri(httpretty.GET, 
                               url_data,
                               body=_body(DATASETS[dataset_code]['data-fp']),
                               streaming=True,
                               status=200,
                               content_type="application/xml")
        
   
    @httpretty.activate     
    def test_load_dataset(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_insee:InseeTestCase.test_load_dataset
        
        dataset_code = 'IPI-2010-A21'
        
        self._load_dataset(dataset_code)
        
        self.assertEqual(len(self.insee._dataflows.keys()), 663)
        
        self.assertTrue(dataset_code in self.insee._dataflows)

        result = self.insee.upsert_dataset(dataset_code)
        
        query = {
            "provider": self.insee.provider_name,
            "datasetCode": dataset_code
        }

        dataset = self.db[constants.COL_DATASETS].find_one(query)
        self.assertIsNotNone(dataset)
        
        series_list = self.db[constants.COL_SERIES].find(query)
        
        count = series_list.count()
        
        self.assertEqual(count, DATASETS[dataset_code]['series_count'])
        
        query['key'] = "001654489" 
        series_001654489 = self.db[constants.COL_SERIES].find_one(query)
        self.assertIsNotNone(series_001654489)
        
        #1990-01
        self.assertEqual(series_001654489["values"][0], "139.22")

        #2015-10
        self.assertEqual(series_001654489["values"][-1], "105.61")
        
        frequency = series_001654489["frequency"]
        startDate = str(pandas.Period(ordinal=series_001654489["startDate"], freq=frequency))
        self.assertEqual(startDate, '1990-01')

        endDate = str(pandas.Period(ordinal=series_001654489["endDate"], freq=frequency))
        self.assertEqual(endDate, '2015-10')
        
    @unittest.skipIf(True, "TODO")
    def test_dimensions_to_dict(self):
        pass
    
    @httpretty.activate     
    @mock.patch('dlstats.fetchers.insee.INSEE.upsert_dataset', mock_upsert_dataset)    
    def test_invalid_series_key(self):

        # nosetests -s -v dlstats.tests.fetchers.test_insee:InseeTestCase.test_invalid_series_key
        
        dataset_code = 'CNA-2010-CONSO-SI-A17'
        
        self._load_dataset(dataset_code)
        self.insee.upsert_dataset(dataset_code)
        series_list = self.insee.insee_data._series
        self.assertEqual(len(series_list), DATASETS[dataset_code]['series_count'])
        
        series = series_list[0]
        self.assertTrue('SECT-INST' in series['dimensions'])
    
    @unittest.skipIf(True, "TODO")
    def test_select_short_dimension(self):
        pass
    
    @unittest.skipIf(True, "TODO")    
    def test_is_valid_frequency(self):
        pass
    
    @unittest.skipIf(True, "TODO")    
    def test_get_series(self):
        pass
    
    @unittest.skipIf(True, "TODO")    
    def test_build_series(self):
        pass
    
    @httpretty.activate     
    @mock.patch('dlstats.fetchers.insee.INSEE.upsert_dataset', mock_upsert_dataset)    
    @unittest.skipIf(True, "TODO")
    def test_is_updated(self):

        # nosetests -s -v dlstats.tests.fetchers.test_insee:InseeTestCase.test_is_updated

        dataset_code = 'IPI-2010-A21'
        
        self._load_dataset(dataset_code)
        self.insee.upsert_dataset(dataset_code)
        _series = self.insee.insee_data._series
        self.assertEqual(len(_series), DATASETS[dataset_code]['series_count'])
        
        self._load_dataset(dataset_code)
        self.insee.upsert_dataset(dataset_code)
        _series = self.insee.insee_data._series
        self.assertEqual(len(_series), 0)

        '''series avec un LAST_UPDATE > au dataset'''
        query = {
            "provider": self.insee.provider_name,
            "datasetCode": dataset_code
        }
        new_datetime = datetime(2015, 12, 9)
        result = self.db[constants.COL_DATASETS].update_one(query, {"$set": {'lastUpdate': new_datetime}})
        pprint(result.raw_result)
        self._load_dataset(dataset_code)
        self.insee.upsert_dataset(dataset_code)
        _series = self.insee.insee_data._series
        #pprint(_series)
        for s in _series:
            print(s['key'])
        d = self.db[constants.COL_DATASETS].find_one(query)
        print("dataset : ", d['lastUpdate'])
        self.assertEqual(len(_series), 11)
        
        
        
