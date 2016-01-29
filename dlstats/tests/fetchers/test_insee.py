# -*- coding: utf-8 -*-

from copy import deepcopy
from datetime import datetime
import os
from pprint import pprint

from dlstats.fetchers.insee import INSEE as Fetcher
from dlstats import constants

import unittest
import httpretty

from dlstats.tests.fetchers.base import BaseFetcherTestCase, body_generator
from dlstats.tests.resources import xml_samples

LOCAL_DATASETS_UPDATE = {
    "IPI-2010-A21": {
        "concept_keys": ['CARBURANT', 'COMPTE', 'INDICATEUR', 'LOCAL', 'METIER', 
                         'MONNAIE', 'TYPE-MENAGE', 'NATURE', 'DEVISE', 'IDBANK', 
                         'ETAT-CONSTRUCTION', 'DEPARTEMENT', 'TIME_PER_COLLECT', 
                         'QUOTITE-TRAV', 'STATUT', 'GEOGRAPHIE', 'TYPE-RESEAU', 
                         'TIME_PERIOD', 'TYPE-EMP', 'CHEPTEL', 'TYPE-ETAB', 
                         'INSTRUMENT', 'TYPE-SURF-ALIM', 'TYPE-VEHICULE', 'INDEX', 
                         'FORMATION', 'LOGEMENT', 'FACTEUR-INV', 'REVENU', 
                         'CAT-DE', 'DEST-INV', 'PRATIQUE', 'TYPE-CESS-ENT', 
                         'FREQUENCE', 'PERIODE', 'CAT-FP', 'UNITE-URBAINE', 
                         'LAST_UPDATE', 'ANCIENNETE', 'AGE', 'HALO', 
                         'TAILLE-MENAGE', 'EFFOPE', 'UNIT_MEASURE', 'REGIONS', 
                         'SPECIALITE-SANTE', 'OPERATION', 'TOURISME-INDIC', 
                         'TYPE-SAL', 'DIPLOME', 'EXPAGRI', 'UNITE', 'FINANCEMENT', 
                         'REGION', 'PROD-VEG', 'FORME-EMP', 'FORME-VENTE', 
                         'IPC-CALC01', 'BASIND', 'BRANCHE', 'ACTIVITE', 
                         'LOCALISATION', 'CARAC-LOG', 'TYPE-PRIX', 'TYPE-FAMILLE', 
                         'CLIENTELE', 'OCC-SOL', 'DECIMALS', 'DATE-DEF-ENT', 
                         'TYPE-CREAT-ENT', 'TITLE', 'PRIX', 'FONCTION', 'TYPE-EVO', 
                         'TAILLE-ENT', 'CAUSE-DECES', 'MARCHANDISE', 'FREQ', 
                         'ACCUEIL-PERS-AGEES', 'TYPE-TX-CHANGE', 'CHAMP-GEO', 
                         'RESIDENCE', 'DEMOGRAPHIE', 'MIN-FPE', 'POPULATION', 
                         'TYPE-COTIS', 'CORRECTION', 'UNIT_MULT', 'PRODUIT', 
                         'DEPARTEMENTS', 'REPARTITION', 'SECT-INST', 'ETAB-SCOL', 
                         'NATURE-FLUX', 'QUESTION', 'OBS_STATUS', 'ZONE-GEO', 
                         'TYPE-ENT', 'OBS_VALUE', 'REF_AREA', 'COTISATION', 
                         'TYPE-OE', 'SEXE', 'EMBARGO_TIME', 'TYPE-FLUX', 
                         'FEDERATION', 'BASE_PER'],
        "codelist_keys": ['TITLE', 'DECIMALS', 'NATURE', 'PRODUIT', 'LAST_UPDATE', 'FREQ', 
                          'TIME_PER_COLLECT', 'IDBANK', 'EMBARGO_TIME', 'OBS_STATUS', 'UNIT_MULT', 
                          'REF_AREA', 'UNIT_MEASURE', 'BASE_PER'],
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
    def test_upsert_dataset_ipi_2010_a21(self):

        # nosetests -s -v dlstats.tests.fetchers.test_insee:FetcherTestCase.test_upsert_dataset_ipi_2010_a21

        self._collections_is_empty()
        
        dataset_code = 'IPI-2010-A21'

        self.DATASETS[dataset_code]["DSD"]["concept_keys"] = LOCAL_DATASETS_UPDATE[dataset_code]["concept_keys"]
        self.DATASETS[dataset_code]["DSD"]["codelist_keys"] = LOCAL_DATASETS_UPDATE[dataset_code]["codelist_keys"]
        self.DATASETS[dataset_code]["DSD"]["codelist_count"] = LOCAL_DATASETS_UPDATE[dataset_code]["codelist_count"]

        self._load_files(dataset_code)
        
        self.assertProvider()
        self.assertDataTree(dataset_code)
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
        
        
        
