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
from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "insee"))

LOCAL_DATASETS_UPDATE = {
    "IPI-2010-A21": {
        "categories_root": ['COMPTA-NAT', 'CONDITIONS-VIE-SOCIETE', 'DEMO-ENT', 
                            'ECHANGES-EXT', 'ENQ-CONJ', 'MARCHE-TRAVAIL', 
                            'POPULATION', 'PRIX', 'PRODUCTION-ENT', 
                            'SALAIRES-REVENUS', 'SERVICES-TOURISME-TRANSPORT', 
                            'SRGDP'],
        "concept_keys": ['BASE_PER', 'DECIMALS', 'EMBARGO_TIME', 'FREQ', 'IDBANK', 'LAST_UPDATE', 'NATURE', 'OBS_STATUS', 'PRODUIT', 'REF_AREA', 'TIME_PER_COLLECT', 'TITLE', 'UNIT_MEASURE', 'UNIT_MULT'],
        "codelist_keys": ['BASE_PER', 'DECIMALS', 'EMBARGO_TIME', 'FREQ', 'IDBANK', 'LAST_UPDATE', 'NATURE', 'OBS_STATUS', 'PRODUIT', 'REF_AREA', 'TIME_PER_COLLECT', 'TITLE', 'UNIT_MEASURE', 'UNIT_MULT'],
        "codelist_count": {
            "BASE_PER": 0,
            "DECIMALS": 0,
            "EMBARGO_TIME": 0,
            "FREQ": 2,
            "IDBANK": 0,
            "LAST_UPDATE": 0,
            "NATURE": 3,
            "OBS_STATUS": 1,
            "PRODUIT": 5,
            "REF_AREA": 1,
            "TIME_PER_COLLECT": 2,
            "TITLE": 0,
            "UNIT_MEASURE": 2,
            "UNIT_MULT": 0,
        },
        "dimension_keys": ['FREQ', 'PRODUIT', 'NATURE'],
        "dimension_count": {
            "FREQ": 2,
            "PRODUIT": 5,
            "NATURE": 3,
        },
        "attribute_keys": ['IDBANK', 'TITLE', 'LAST_UPDATE', 'UNIT_MEASURE', 'UNIT_MULT', 'REF_AREA', 'DECIMALS', 'BASE_PER', 'TIME_PER_COLLECT', 'OBS_STATUS', 'EMBARGO_TIME'],
        "attribute_count": {
            "IDBANK": 0,
            "TITLE": 0,
            "LAST_UPDATE": 0,
            "UNIT_MEASURE": 2,
            "UNIT_MULT": 0,
            "REF_AREA": 1,
            "DECIMALS": 0,
            "BASE_PER": 0,
            "TIME_PER_COLLECT": 2,
            "OBS_STATUS": 1,
            "EMBARGO_TIME": 0,
        },
    },
}

DSD_INSEE_CHO_AN_AGE = {
    "provider": "INSEE",
    "filepaths": xml_samples.DATA_INSEE_SPECIFIC["DSD"]["filepaths"],
    "dataset_code": "CHO-AN-AGE",
    "dataset_name": "Unemployment according to the ILO standard (annual average) - By gender and age",
    "dsd_id": "CHO-AN-AGE",
    "dsd_ids": ["CHO-AN-AGE"],
    "dataflow_keys": ['CHO-AN-AGE'],
    "is_completed": True,
    "concept_keys": ['AGE', 'BASE_PER', 'DECIMALS', 'EMBARGO_TIME', 'FREQ', 'IDBANK', 'INDICATEUR', 'LAST_UPDATE', 'OBS_STATUS', 'REF_AREA', 'SEXE', 'TIME_PER_COLLECT', 'TITLE', 'UNIT_MEASURE', 'UNIT_MULT'],
    "codelist_keys": ['AGE', 'BASE_PER', 'DECIMALS', 'EMBARGO_TIME', 'FREQ', 'IDBANK', 'INDICATEUR', 'LAST_UPDATE', 'OBS_STATUS', 'REF_AREA', 'SEXE', 'TIME_PER_COLLECT', 'TITLE', 'UNIT_MEASURE', 'UNIT_MULT'],
    "codelist_count": {
        "AGE": 5,
        "BASE_PER": 0,
        "DECIMALS": 0,
        "EMBARGO_TIME": 0,
        "FREQ": 1,
        "IDBANK": 0,
        "INDICATEUR": 2,
        "LAST_UPDATE": 0,
        "OBS_STATUS": 1,
        "REF_AREA": 2,
        "SEXE": 3,
        "TIME_PER_COLLECT": 1,
        "TITLE": 0,
        "UNIT_MEASURE": 2,
        "UNIT_MULT": 0,
    },
    "dimension_keys": ['INDICATEUR', 'SEXE', 'AGE'],
    "dimension_count": {
        "INDICATEUR": 2,
        "SEXE": 3,
        "AGE": 5,
    },
    "attribute_keys": ['FREQ', 'IDBANK', 'TITLE', 'LAST_UPDATE', 'UNIT_MEASURE', 'UNIT_MULT', 'REF_AREA', 'DECIMALS', 'BASE_PER', 'TIME_PER_COLLECT', 'OBS_STATUS', 'EMBARGO_TIME'],      
    "attribute_count": {
        "FREQ": 1,
        "IDBANK": 0,
        "TITLE": 0,
        "LAST_UPDATE": 0,
        "UNIT_MEASURE": 2,
        "UNIT_MULT": 0,
        "REF_AREA": 2,
        "DECIMALS": 0,
        "BASE_PER": 0,
        "TIME_PER_COLLECT": 1,
        "OBS_STATUS": 1,
        "EMBARGO_TIME": 0,
    },
}                        
DSD_INSEE_CHO_AN_AGE["filepaths"]["datastructure"] = os.path.abspath(os.path.join(RESOURCES_DIR, "insee-datastructure-CHO-AN-AGE.xml"))
        
DATA_INSEE_CHO_AN_AGE = {
    "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "insee-data-CHO-AN-AGE.xml")),
    "klass": "XMLSpecificData_2_1_INSEE",
    "DSD": DSD_INSEE_CHO_AN_AGE,
    "kwargs": {
        "provider_name": "INSEE",
        "dataset_code": "CHO-AN-AGE",
        "dsd_filepath": DSD_INSEE_CHO_AN_AGE["filepaths"]["datastructure"],
    },
    "series_accept": 31,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 1219,
    "series_key_first": '001664976',
    "series_key_last": '001665006',
    "series_sample": {
        "provider_name": "INSEE",
        "dataset_code": "CHO-AN-AGE",
        'key': '001664976',
        'name': 'Number - Men - From 15 to 24 years old',
        'frequency': 'A',
        'last_update': None,
        'first_value': {
            'value': '143',
            'ordinal': 5,
            'period': '1975',
            'attributes': {
                "OBS_STATUS": "A"
            },
        },
        'last_value': {
            'value': '359',
            'ordinal': 44,
            'period': '2014',
            'attributes': {
                "OBS_STATUS": "A"
            },
        },
        'dimensions': {
           'INDICATEUR': 'Nbre',
           'SEXE': '1',
           'AGE': '15-24',
        },
        'attributes': {
            'DECIMALS': '0',
            'FREQ': 'A',
            'LAST_UPDATE': '2016-02-10',
            'REF_AREA': 'FM',
            'TIME_PER_COLLECT': 'MOYENNE',
            'TITLE': 'Nombre de chômeurs au sens du BIT (moyenne annuelle) - Hommes de 15 à 24 ans - France métropolitaine',
            'UNIT_MEASURE': 'IND',
            'UNIT_MULT': '3'
        },
    }
}

class FetcherTestCase(BaseFetcherTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_insee:FetcherTestCase

    FETCHER_KLASS = Fetcher
    DATASETS = {
        'IPI-2010-A21': deepcopy(xml_samples.DATA_INSEE_SPECIFIC),
        'CHO-AN-AGE': DATA_INSEE_CHO_AN_AGE
    }
    DATASET_FIRST = "ACT-TRIM-ANC"
    DATASET_LAST = "TXEMP-AN-FR"
    DEBUG_MODE = False
    
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
        
        for cl in ["CL_UNIT", "CL_AREA", "CL_TIME_COLLECT", "CL_OBS_STATUS", "CL_UNIT_MULT", "CL_FREQ"]:
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
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    def test_load_datasets_first(self):

        dataset_code = 'IPI-2010-A21'
        self._load_files(dataset_code)
        self.assertLoadDatasetsFirst([dataset_code])

    @httpretty.activate     
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
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
    @unittest.skipIf(True, "FIXME")     
    def test_upsert_dataset_ipi_2010_a21(self):

        # nosetests -s -v dlstats.tests.fetchers.test_insee:FetcherTestCase.test_upsert_dataset_ipi_2010_a21

        dataset_code = 'IPI-2010-A21'
        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])
        self._load_files(dataset_code)
        self.assertProvider()
        self.assertDataset(dataset_code)
        self.assertSeries(dataset_code)

    @httpretty.activate     
    def test_upsert_dataset_cho_an_age(self):

        # nosetests -s -v dlstats.tests.fetchers.test_insee:FetcherTestCase.test_upsert_dataset_cho_an_age

        dataset_code = 'CHO-AN-AGE'
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
        
        
        
