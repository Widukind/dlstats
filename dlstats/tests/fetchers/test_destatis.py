# -*- coding: utf-8 -*-

import os
from dlstats.fetchers.destatis import DESTATIS as Fetcher
from dlstats.fetchers.destatis import DATASETS as FETCHER_DATASETS

import unittest
import httpretty

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase
from dlstats.tests.resources import xml_samples

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "destatis"))

@unittest.skipIf(True, "TODO")
class FetcherTestCase(BaseFetcherTestCase):

    # nosetests -s -v dlstats.tests.fetchers.test_destatis:FetcherTestCase
    
    FETCHER_KLASS = Fetcher
    DATASETS = {
        "DCS": xml_samples.DATA_DESTATIS.copy()
    }

    def _common(self, dataset_code):
        #url = "https://www.destatis.de/sddsplus/%s.xml" % dataset_code
        #TODO: dsd filepath
        url = FETCHER_DATASETS[dataset_code]["url"]
        self.register_url(url, self.DATASETS[dataset_code]["filepath"],
                          content_type='application/xml')

    @httpretty.activate     
    def test_upsert_dataset_dcs(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_destatis:FetcherTestCase.test_upsert_dataset_dcs
        
        dataset_code = "DCS"
        
        self._common(dataset_code)
        
        self.assertDataset(dataset_code)        
        self.assertSeries(dataset_code)

    @unittest.skipIf(True, "TODO")    
    @httpretty.activate     
    def test_upsert_dataset_nag(self):
        pass
        #https://www.destatis.de/sddsplus/NAG.xml
        #TODO: tester surtout le problème du TIME_FORMAT P3M        
