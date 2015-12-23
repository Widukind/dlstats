# -*- coding: utf-8 -*-

import datetime
import os
from pprint import pprint

from dlstats.fetchers._commons import Datasets
from dlstats.fetchers.insee import INSEE
from dlstats import constants

import unittest
import httpretty

from dlstats.tests.base import RESOURCES_DIR, BaseTestCase, BaseDBTestCase

"""
Load files with httpie tools:
    http http://www.bdm.insee.fr/series/sdmx/dataflow references==all Accept:application/xml Content-Type:application/xml > insee-dataflow.xml
    http http://www.bdm.insee.fr/series/sdmx/datastructure/FR1/IPI-2010-A21 references==all Accept:application/xml Content-Type:application/xml > insee-IPI-2010-A21-datastructure.xml
    http http://www.bdm.insee.fr/series/sdmx/data/IPI-2010-A21 Accept:application/vnd.sdmx.genericdata+xml;version=2.1 > insee-IPI-2010-A21-data.xml
"""
DATAFLOW_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "insee-dataflow.xml"))

DATASETS = {
    'IPI-2010-A21': {
        'data-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "insee-IPI-2010-A21-data.xml")),
        'datastructure-fp': os.path.abspath(os.path.join(RESOURCES_DIR, "insee-IPI-2010-A21-datastructure.xml")),
        'series_count': 20,
    }
}

class InseeTestCase(BaseDBTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_insee:InseeTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)
        self.insee = INSEE(db=self.db)
   
    @httpretty.activate     
    def test_load_dataset(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_insee:InseeTestCase.test_load_dataset

        def _body(filepath):
            '''body for large file'''
            with open(filepath, 'rb') as fp:
                for line in fp:
                    yield line        
        
        dataset_code = 'IPI-2010-A21'
        
        url_dataflow = "http://www.bdm.insee.fr/series/sdmx/dataflow/INSEE"
        httpretty.register_uri(httpretty.GET, 
                               url_dataflow,
                               body=_body(DATAFLOW_FP),
                               match_querystring=True,
                               status=200,
                               streaming=True,
                               content_type="application/xml")
        
        self.insee._dataflows = self.insee.sdmx.get(resource_type='dataflow').msg.dataflows
        dataflow = self.insee._dataflows[dataset_code]
        
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

    @unittest.skipIf(True, "TODO")    
    def test_dimensions_to_dict(self):
        pass
    
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
    
