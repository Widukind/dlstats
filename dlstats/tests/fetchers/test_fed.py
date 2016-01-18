# -*- coding: utf-8 -*-

from datetime import datetime
import os
from dlstats import constants
from dlstats.fetchers.fed import FED as Fetcher
from dlstats.fetchers.fed import DATASETS as FETCHER_DATASETS

import unittest
import httpretty

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "fed"))

ALL_DATASETS = {
    'G19': { #http://www.federalreserve.gov/datadownload/Output.aspx?rel=G19&filetype=zip
        'filepath': os.path.abspath(os.path.join(RESOURCES_DIR, "FRB_G19.zip")),
        'series_count': 80, #FIXME: 81
        'first_series': {
            "key": "RIFLPBCIANM48_N.M",
            "name": "RIFLPBCIANM48_N.M",
            "frequency": "M",
            "first_value": "10.20",
            "first_date": "1972-02-29",
            "last_value": "4.11",
            "last_date": "2015-08-31",
        },
        'last_series': { #<kf:Series CREDTYP="MOTOR" CURRENCY="USD" DATAREP="MILLDOLL" FREQ="129" HOLDER="ALL" SA="NSA" SERIES_NAME="DTCTLNV_XDF_BA_N.M" UNIT="Currency" UNIT_MULT="1000000"  > 
            "key": "DTCTLNV_XDF_BA_N.M",
            "name": "DTCTLNV_XDF_BA_N.M",
            "frequency": "M",
            "first_value": "-30.00",
            "first_date": "1943-06-30",
            "last_value": "8655.11",
            "last_date": "2015-06-30",
        },
    },
}

class FetcherTestCase(BaseFetcherTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_fed:FetcherTestCase

    FETCHER_KLASS = Fetcher
    DATASETS = ALL_DATASETS

    @httpretty.activate     
    def test_upsert_dataset_g19(self):
        
        dataset_code = "G19"
        
        url = FETCHER_DATASETS[dataset_code]["url"]
        self.register_url(url, self.DATASETS[dataset_code]["filepath"],
                          content_type='application/xml')
        
        result = self.fetcher.upsert_dataset(dataset_code)
        self.assertIsNotNone(result)
        
        query = {
            'provider_name': self.fetcher.provider_name,
            "dataset_code": dataset_code
        }

        dataset = self.db[constants.COL_DATASETS].find_one(query)
        self.assertIsNotNone(dataset)
        
        self.assertDatasetOK(dataset_code)        
        self.assertSeriesOK(dataset_code)

    @unittest.skipIf(True, "TODO")    
    @httpretty.activate     
    def test_upsert_dataset_z1(self):
        """
        """
