# -*- coding: utf-8 -*-

import os
import datetime

from dlstats.fetchers._commons import Datasets
from dlstats.fetchers import oecd
from dlstats import constants

import unittest
from unittest import mock
import httpretty

from dlstats.tests.base import RESOURCES_DIR, BaseTestCase, BaseDBTestCase

class OECDDatasetsTestCase(BaseTestCase):
    """Fetchers Tests - No DB access
    """

    # nosetests -s -v dlstats.tests.fetchers.test_oecd:OECDDatasetsTestCase
    
    def setUp(self):
        BaseTestCase.setUp(self)
        
        self.fetcher = oecd.OECD(is_indexes=False)

        self.dataset = Datasets(provider_name="OCED", 
                            dataset_code="MEI", 
                            name="Main Economic Indicator", 
                            fetcher=self.fetcher,
                            is_load_previous_version=False)
    
        self.oecd_data = oecd.OECD_Data(self.dataset, 
                                        limited_countries=["TEST"], 
                                        is_autoload=False)
    
    def test__patch_period(self):

        period = self.oecd_data._patch_period("1999", "A")
        self.assertEqual(period, "1999")
        
        period = self.oecd_data._patch_period("Q1-1999", "Q")
        self.assertEqual(period, "1999-Q1")
        
        period = self.oecd_data._patch_period("10-1999", "M")
        self.assertEqual(period, "1999-10")
        
        with self.assertRaises(Exception) as ex:
            self.oecd_data._patch_period("xx-1999", "X")
            self.assertEqual(str(ex), "Not implemented Frequency[X]")

    def test_get_temp_file(self):

        filepath, fp = self.oecd_data.get_temp_file(mode='w')
        
        self.assertIsNotNone(filepath)
        self.assertTrue(os.path.exists(filepath))
                
        fp.close()
        os.remove(filepath)
    
    @httpretty.activate
    def test_build_series(self):

        # nosetests -s -v dlstats.tests.fetchers.test_oecd:OECDDatasetsTestCase.test_build_series

        mei_metadatas = os.path.abspath(os.path.join(RESOURCES_DIR, "oecd_mei_metadata.json"))
        body = None
        with open(mei_metadatas) as fp:
            body = fp.read()        

        httpretty.register_uri(httpretty.GET, 
                               "http://stats.oecd.org/sdmx-json/metadata/MEI",
                               body=body,
                               status=200,
                               content_type="application/json"
                               )

        self.oecd_data.load_codes()
        
        '''Country TEST'''
        row = {
            'id': 'KEY1',
            'values': [1.74265791869857, 1.81348933188545],
            'dimensions': {
                'Frequency': 'A',
                'Country': 'FRA',
                'Measure': 'MEI',
                'Subject': 'BPFAFD01',
            },            
            'periods': ['1980', '1981'],
        }
        
        result = self.oecd_data.build_serie(row)
        
        data = {'provider': self.dataset.provider_name,
                'dataset_code': self.dataset.dataset_code,
                'name': 'France - Annual - Main Economic Indicators - Balance of Payments > Financial Account > Financial derivatives > Net financial derivatives',
                'key': "FRA.A.MEI.BPFAFD01",
                'values': [str(v) for v in row['values']],
                'attributes': {},
                'dimensions': {
                    'Country': 'FRA',
                    'Frequency': 'A',
                    'Measure': 'MEI',
                    'Subject': 'BPFAFD01'
                },                
                #'dimensions': {
                #    "Measure" : "Main Economic Indicators",
                #    "Country" : "France",
                #    "Frequency" : "Annual",
                #    "Subject" : "Balance of Payments > Financial Account > Financial derivatives > Net financial derivatives"
                #},
                'last_update': self.oecd_data.prepared,
                'start_date': 10,
                'end_date': 11,
                'frequency': 'A'}        

        self.assertDictEqual(result, data)
    
    @httpretty.activate
    def test_load_codes_mei(self):
        
        mei_metadatas = os.path.abspath(os.path.join(RESOURCES_DIR, "oecd_mei_metadata.json"))
        body = None
        with open(mei_metadatas) as fp:
            body = fp.read()        

        #TODO: tester erreur 4xx et 5xx        
        httpretty.register_uri(httpretty.GET, 
                               "http://stats.oecd.org/sdmx-json/metadata/MEI",
                               body=body,
                               status=200,
                               content_type="application/json"
                               )

        self.assertFalse(self.oecd_data.codes_loaded)

        self.oecd_data.load_codes()

        self.assertTrue(self.oecd_data.codes_loaded)
        
        dt = datetime.datetime(2015, 10, 29, 6, 39, 18, 634125)
        self.assertEqual(self.oecd_data.prepared, dt)
        self.assertEqual(self.oecd_data.dataset.last_update, dt)
        
        self.assertEqual(len(self.oecd_data.countries), 1)
        self.assertEqual(list(self.oecd_data.countries.keys())[0], 'FRA')
        
        self.assertEqual(len(self.oecd_data.dimension_keys), 4)
        
        for dim in ['Country', 'Subject', 'Measure', 'Frequency']:
            self.assertTrue(dim in self.oecd_data.dimension_keys)
            self.assertTrue(dim in list(self.oecd_data.codes.keys()))


class OECDDatasetsDBTestCase(BaseDBTestCase):
    """Fetchers Tests - with DB
    """
    
    # nosetests -s -v dlstats.tests.fetchers.test_oecd:OECDDatasetsDBTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)
        self.fetcher = oecd.OECD(db=self.db)

    @unittest.skipIf(True, "TODO - sdmx tests")
    def test_mei(self):
        pass
