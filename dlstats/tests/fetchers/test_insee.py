# -*- coding: utf-8 -*-

from datetime import datetime
import os
from pprint import pprint

import pandas

from dlstats.fetchers._commons import Datasets
from dlstats.fetchers.insee import INSEE as Fetcher, INSEE_Data, ContinueRequest
from dlstats import constants

import unittest
from unittest import mock
import httpretty

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR, BaseDBTestCase
from dlstats.tests.fetchers.base import BaseFetcherTestCase, body_generator

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "insee"))
DATAFLOW_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "insee-dataflow.xml"))
#http ""http://www.bdm.insee.fr/series/sdmx/codelist/INSEE/CL_UNIT" > insee-codelist-cl_unit.xml
CL_UNIT_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "insee-codelist-cl_unit.xml"))
CL_AREA_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "insee-codelist-cl_area.xml"))
CL_TIME_COLLECT_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "insee-codelist-cl_time_collect.xml"))
CL_OBS_STATUS_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "insee-codelist-cl_obs_status.xml"))

DATASETS = {
    'IPI-2010-A21': {
        'data-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "insee-IPI-2010-A21-specificdata.xml")),
        'datastructure-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "insee-IPI-2010-A21-datastructure.xml")),
        'series_count': 20,
    },
    'CNA-2010-CONSO-SI-A17': {
        'data-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "insee-bug-data-namedtuple.xml")),
        'datastructure-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "insee-bug-data-namedtuple-datastructure.xml")),
        'series_count': 1,
    },
}

ALL_DATASETS = {
    'IPI-2010-A21': { #http://www.bdm.insee.fr/series/sdmx/data/IPI-2010-A21/
        'dataflow-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "insee-dataflow.xml")),
        'data-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "insee-IPI-2010-A21-specificdata.xml")),
        'datastructure-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "insee-IPI-2010-A21-datastructure.xml")),
        'series_count': 20,
        'first_series': {
            "key": "001654489",
            "name": "Indice brut de la production industrielle (base 100 en 2010) - Industries extractives (NAF rév. 2, niveau section, poste B)",
            "frequency": "M",
            "first_value": "139.22",
            "first_date": "1990-01",
            "last_value": "96.98",
            "last_date": "2015-11",
        },
        'last_series': {
            "key": "001655704",
            "name": "Pondération IPI (indice 2010) - Construction (NAF rév. 2, niveau section, poste F)",
            "frequency": "A",
            "first_value": "106368",
            "first_date": "2010",
            "last_value": "106368",
            "last_date": "2010",
        },
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
    
    query = {'provider_name': self.provider_name, "dataset_code": dataset_code}
    dataset_doc = self.db[constants.COL_DATASETS].find_one(query)
    
    self.insee_data = MockINSEE_Data(dataset=dataset,
                                     dataset_doc=dataset_doc, 
                                     dataflow=dataflow, 
                                     sdmx=self.sdmx)
    dataset.series.data_iterator = self.insee_data
    result = dataset.update_database()

    return result


class InseeTestCase(BaseFetcherTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_insee:InseeTestCase

    FETCHER_KLASS = Fetcher
    DATASETS = ALL_DATASETS
    
    def _load_dataset(self, dataset_code):

        url_dataflow = "http://www.bdm.insee.fr/series/sdmx/dataflow/INSEE"
        httpretty.register_uri(httpretty.GET, 
                               url_dataflow,
                               body=body_generator(DATAFLOW_FP),
                               match_querystring=True,
                               status=200,
                               streaming=True,
                               content_type="application/xml")
        
        url_cl = "http://www.bdm.insee.fr/series/sdmx/codelist/INSEE/CL_UNIT"
        httpretty.register_uri(httpretty.GET, 
                               url_cl,
                               body=body_generator(CL_UNIT_FP),
                               status=200,
                               streaming=True,
                               content_type="application/xml")
        
        url_cl = "http://www.bdm.insee.fr/series/sdmx/codelist/INSEE/CL_AREA"
        httpretty.register_uri(httpretty.GET, 
                               url_cl,
                               body=body_generator(CL_AREA_FP),
                               status=200,
                               streaming=True,
                               content_type="application/xml")
        
        url_cl = "http://www.bdm.insee.fr/series/sdmx/codelist/INSEE/CL_TIME_COLLECT"
        httpretty.register_uri(httpretty.GET, 
                               url_cl,
                               body=body_generator(CL_TIME_COLLECT_FP),
                               status=200,
                               streaming=True,
                               content_type="application/xml")
                        
        url_cl = "http://www.bdm.insee.fr/series/sdmx/codelist/INSEE/CL_OBS_STATUS"
        httpretty.register_uri(httpretty.GET, 
                               url_cl,
                               body=body_generator(CL_OBS_STATUS_FP),
                               status=200,
                               streaming=True,
                               content_type="application/xml")

        url_datastructure = "http://www.bdm.insee.fr/series/sdmx/datastructure/INSEE/%s?reference=children" % dataset_code
        httpretty.register_uri(httpretty.GET, 
                               url_datastructure,
                               body=body_generator(DATASETS[dataset_code]['datastructure-fp']),
                               match_querystring=True,
                               streaming=True,
                               status=200,
                               content_type="application/xml")

        url_data = "http://www.bdm.insee.fr/series/sdmx/data/%s" % dataset_code
        httpretty.register_uri(httpretty.GET, 
                               url_data,
                               body=body_generator(DATASETS[dataset_code]['data-fp']),
                               streaming=True,
                               status=200,
                               content_type="application/xml")
        
    @httpretty.activate     
    def test_upsert_dataset_ipi_2010_a21(self):

        # nosetests -s -v dlstats.tests.fetchers.test_insee:InseeTestCase.test_upsert_dataset_ipi_2010_a21

        dataset_code = 'IPI-2010-A21'

        self._load_dataset(dataset_code)
        
        #TODO: analyse result
        result = self.fetcher.upsert_dataset(dataset_code)
        self.assertIsNotNone(result)
        
        self.assertDatasetOK(dataset_code)        
        self.assertSeriesOK(dataset_code)
   
    @unittest.skipIf(True, "TODO")
    def test_dimensions_to_dict(self):
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
            'provider_name': self.insee.provider_name,
            "dataset_code": dataset_code
        }
        new_datetime = datetime(2015, 12, 9)
        result = self.db[constants.COL_DATASETS].update_one(query, {"$set": {'last_update': new_datetime}})
        pprint(result.raw_result)
        self._load_dataset(dataset_code)
        self.insee.upsert_dataset(dataset_code)
        _series = self.insee.insee_data._series
        #pprint(_series)
        for s in _series:
            print(s['key'])
        d = self.db[constants.COL_DATASETS].find_one(query)
        print("dataset : ", d['last_update'])
        self.assertEqual(len(_series), 11)
        
        
        
