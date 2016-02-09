# -*- coding: utf-8 -*-

from copy import deepcopy
from datetime import datetime
import os
from pprint import pprint

from dlstats.fetchers.insee import INSEE as Fetcher
from dlstats import constants

import unittest
import httpretty

from dlstats.tests.fetchers.base import BaseFetcherTestCase
from dlstats.tests.resources import xml_samples

LOCAL_DATASETS_UPDATE = {
    "IPI-2010-A21": {
        "categories_root": ['COMPTA-NAT', 'CONDITIONS-VIE-SOCIETE', 'DEMO-ENT', 
                            'ECHANGES-EXT', 'ENQ-CONJ', 'MARCHE-TRAVAIL', 
                            'POPULATION', 'PRIX', 'PRODUCTION-ENT', 
                            'SALAIRES-REVENUS', 'SERVICES-TOURISME-TRANSPORT', 
                            'SRGDP'],
        "concept_keys": ['REF_AREA', 'OBS_STATUS', 'LAST_UPDATE', 'TITLE', 
                         'UNIT_MULT', 'IDBANK', 'PRODUIT', 'UNIT_MEASURE', 
                         'FREQ', 'BASE_PER', 'NATURE', 'DECIMALS', 
                         'TIME_PER_COLLECT', 'EMBARGO_TIME'],
        "codelist_keys": ['REF_AREA', 'OBS_STATUS', 'LAST_UPDATE', 'TITLE', 
                          'UNIT_MULT', 'IDBANK', 'PRODUIT', 'UNIT_MEASURE', 
                          'FREQ', 'BASE_PER', 'NATURE', 'DECIMALS', 'TIME_PER_COLLECT', 
                          'EMBARGO_TIME'],
        "codelist_count": {
            "TITLE": 0,
            "DECIMALS": 0,
            "NATURE": 25,
            "PRODUIT": 30,
            "LAST_UPDATE": 0,
            "FREQ": 7,
            "TIME_PER_COLLECT": 7,
            "IDBANK": 0,
            "EMBARGO_TIME": 0,
            "OBS_STATUS": 10,
            "UNIT_MULT": 0,
            "REF_AREA": 11,
            "UNIT_MEASURE": 123,
            "BASE_PER": 0,                     
        },
    }
}

class FetcherTestCase(BaseFetcherTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_insee:FetcherTestCase

    FETCHER_KLASS = Fetcher
    DATASETS = {
        'IPI-2010-A21': deepcopy(xml_samples.DATA_INSEE_SPECIFIC)
    }
    DATASET_FIRST = "ACT-TRIM-ANC"
    DATASET_LAST = "TXEMP-AN-FR"
    DEBUG_MODE = True
    
    def _load_files(self, dataset_code):
        
        filepaths = self.DATASETS[dataset_code]["DSD"]["filepaths"]
        dsd_content_type = 'application/vnd.sdmx.structure+xml;version=2.1'

        url = "http://www.bdm.insee.fr/series/sdmx/dataflow/INSEE"
        self.register_url(url, 
                          filepaths["dataflow"],
                          content_type=dsd_content_type,
                          match_querystring=True)

        url = "http://www.bdm.insee.fr/series/sdmx/categoryscheme/INSEE"
        self.register_url(url, 
                          filepaths["categoryscheme"],
                          content_type=dsd_content_type,
                          match_querystring=True)

        url = "http://www.bdm.insee.fr/series/sdmx/categorisation/INSEE"
        self.register_url(url, 
                          filepaths["categorisation"],
                          content_type=dsd_content_type,
                          match_querystring=True)

        url = "http://www.bdm.insee.fr/series/sdmx/conceptscheme/INSEE"
        self.register_url(url, 
                          filepaths["conceptscheme"],
                          content_type=dsd_content_type,
                          match_querystring=True)
        
        for cl in ["CL_UNIT", "CL_AREA", "CL_TIME_COLLECT", "CL_OBS_STATUS"]:
            url = "http://www.bdm.insee.fr/series/sdmx/codelist/INSEE/%s" % cl
            self.register_url(url, 
                              filepaths[cl],
                              content_type=dsd_content_type,
                              match_querystring=True)
        
        
        url = "http://www.bdm.insee.fr/series/sdmx/datastructure/INSEE/%s?reference=children" % dataset_code
        self.register_url(url, 
                          filepaths["datastructure"],
                          content_type=dsd_content_type,
                          match_querystring=True)
        
        url = "http://www.bdm.insee.fr/series/sdmx/data/%s" % dataset_code
        self.register_url(url, 
                          self.DATASETS[dataset_code]['filepath'],
                          content_type='application/vnd.sdmx.structurespecificdata+xml;version=2.1',
                          match_querystring=True)

    @httpretty.activate     
    def test_load_datasets_first(self):

        dataset_code = 'IPI-2010-A21'
        self._load_files(dataset_code)
        self.assertLoadDatasetsFirst([dataset_code])

    @httpretty.activate     
    def test_load_datasets_update(self):

        dataset_code = 'IPI-2010-A21'
        self._load_files(dataset_code)
        self.assertLoadDatasetsUpdate([dataset_code])

    @httpretty.activate     
    def test_build_data_tree(self):

        # nosetests -s -v dlstats.tests.fetchers.test_insee:FetcherTestCase.test_build_data_tree

        dataset_code = 'IPI-2010-A21'
        self._load_files(dataset_code)
        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])
        self.assertDataTree(dataset_code)
        
    @httpretty.activate     
    def test_upsert_dataset_ipi_2010_a21(self):

        # nosetests -s -v dlstats.tests.fetchers.test_insee:FetcherTestCase.test_upsert_dataset_ipi_2010_a21

        dataset_code = 'IPI-2010-A21'

        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])

        self._load_files(dataset_code)
        
        self.assertProvider()
        self.assertDataset(dataset_code)
        self.assertSeries(dataset_code)

    @httpretty.activate     
    @unittest.skipIf(True, "TODO")
    def test_is_updated(self):

        # nosetests -s -v dlstats.tests.fetchers.test_insee:FetcherTestCase.test_is_updated

        dataset_code = 'IPI-2010-A21'
        
        self._load_files(dataset_code)
        self.insee.upsert_dataset(dataset_code)

        '''series avec un LAST_UPDATE > au dataset'''
        query = {
            'provider_name': self.insee.provider_name,
            "dataset_code": dataset_code
        }
        new_datetime = datetime(2015, 12, 9)
        result = self.db[constants.COL_DATASETS].update_one(query, {"$set": {'last_update': new_datetime}})
        pprint(result.raw_result)
        self._load_files(dataset_code)
        self.insee.upsert_dataset(dataset_code)
        _series = self.insee.insee_data._series
        #pprint(_series)
        for s in _series:
            print(s['key'])
        d = self.db[constants.COL_DATASETS].find_one(query)
        print("dataset : ", d['last_update'])
        self.assertEqual(len(_series), 11)
        
        
        
