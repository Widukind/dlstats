# -*- coding: utf-8 -*-

from datetime import datetime
from pprint import pprint
import os
from dlstats import constants
from dlstats.fetchers.destatis import DESTATIS as Fetcher

import unittest
import httpretty

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "destatis"))

ALL_DATASETS = {
    'DCS': { #https://www.destatis.de/sddsplus/DCS.xml
        'filepath': os.path.abspath(os.path.join(RESOURCES_DIR, "destatis-data-compact-2.0.xml")),
        'series_count': 7, # les autres sont vides
        'first_series': {
            "key": "M.DCS.DE.FM1_EUR.U2",
            "name": "M-DCS-DE-FM1_EUR-U2",
            "frequency": "M",
            "first_value": "593935",
            "first_date": "2001-09",
            "last_value": "1789542",
            "last_date": "2015-11",
        },
        'last_series': {
            "key": "M.DCS.DE.FDSLF_EUR.U2",
            "name": "M-DCS-DE-FDSLF_EUR-U2",
            "frequency": "M",
            "first_value": "746547",
            "first_date": "2001-09",
            "last_value": "725274",
            "last_date": "2015-11",
        },
    },
}

class FetcherTestCase(BaseFetcherTestCase):
    
    FETCHER_KLASS = Fetcher
    DATASETS = ALL_DATASETS

    def _common(self, dataset_code):
        url = "https://www.destatis.de/sddsplus/%s.xml" % dataset_code
        self.register_url(url, self.DATASETS[dataset_code]["filepath"],
                          content_type='application/xml')

    @httpretty.activate     
    def test_upsert_dataset_dcs(self):
        
        dataset_code = "DCS"
        
        self._common(dataset_code)
        
        result = self.fetcher.upsert_dataset(dataset_code)
        self.assertIsNotNone(result)
        
        query = {
            'provider_name': self.fetcher.provider_name,
            "dataset_code": dataset_code
        }

        dataset = self.db[constants.COL_DATASETS].find_one(query)
        self.assertIsNotNone(dataset)
        
        for field in ['_id', 'download_first', 'download_last', 'last_update']:
            dataset.pop(field, None)
        
        _dataset = {
             'attribute_list': {},
             'dataset_code': 'DCS',
             'dimension_list': {'COUNTERPART_AREA': [['COUNTERPART_AREA', 'U2']],
                                'DATA_DOMAIN': [['DATA_DOMAIN', 'DCS']],
                                'FREQ': [['FREQ', 'M']],
                                'INDICATOR': [['INDICATOR', 'FM1_EUR']],
                                'REF_AREA': [['REF_AREA', 'DE']]},
             'doc_href': 'http://www.bundesbank.de/Redaktion/EN/Standardartikel/Statistics/sdds_german_contribution_to_the_consolidated_balance_sheet.html',
             'name': 'Depository corporations survey',
             'notes': '',
             'provider_name': 'DESTATIS',
             'slug': 'destatis-dcs'
        }
        
        self.assertEqual(dataset, _dataset)
        
        self.assertDatasetOK(dataset_code)        
        self.assertSeriesOK(dataset_code)

    @unittest.skipIf(True, "TODO")    
    @httpretty.activate     
    def test_upsert_dataset_nag(self):
        """
        https://www.destatis.de/sddsplus/NAG.xml
        
        TODO: tester surtout le problème du TIME_FORMAT P3M        
        """
