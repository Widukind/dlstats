# -*- coding: utf-8 -*-

import tempfile
import datetime
import os
from copy import deepcopy

from dlstats.fetchers.eurostat import Eurostat as Fetcher, make_url
from dlstats.fetchers._commons import Categories
from dlstats import constants
from widukind_common.errors import RejectUpdatedDataset

import httpretty
import unittest

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR
from dlstats.tests.fetchers.base import BaseFetcherTestCase
from dlstats.tests.resources import xml_samples

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "eurostat"))
TOC_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "table_of_contents.xml"))

def extract_zip_file(zipfilepath):
    import zipfile
    zfile = zipfile.ZipFile(zipfilepath)
    tmpfiledir = tempfile.mkdtemp()
    filepaths = {}
    for filename in zfile.namelist():
        filepaths.update({filename: zfile.extract(filename, 
                                                  os.path.abspath(tmpfiledir))})
    return filepaths

LOCAL_DATASETS_UPDATE = {
    "nama_10_fcs": {
        "last_update": datetime.datetime(2015, 10, 26, 0, 0),
        "frequencies": ['A'],
        "concept_keys": ['freq', 'obs-status', 'time-format', 'geo', 'na-item', 'unit'], 
        "codelist_keys": ['freq', 'obs-status', 'time-format', 'geo', 'na-item', 'unit'], 
        "codelist_count": {
            "freq": 9,
            "geo": 33,
            "na-item": 10,
            "obs-status": 12,
            "time-format": 7,
            "unit": 12,
        },
        "dimension_keys": ['freq', 'unit', 'na-item', 'geo'],
        "dimension_count": {
            "freq": 9,
            "unit": 12,
            "na-item": 10,
            "geo": 33,
        },
        "attribute_keys": ["time-format", "obs-status"],
        "attribute_count": {
            "time-format": 7,
            "obs-status": 12,
        }, 
    }
}

class FetcherTestCase(BaseFetcherTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_eurostat:FetcherTestCase
    
    FETCHER_KLASS = Fetcher
    DATASETS = {
        'nama_10_fcs': deepcopy(xml_samples.DATA_EUROSTAT)
    }
    DATASET_FIRST = "bop_c6_m"
    DATASET_LAST = "nama_10_gdp"
    DEBUG_MODE = False
    
    def _load_files_datatree(self, toc=TOC_FP):
        
        self.fetcher.url_table_of_contents = "http://localhost/toc.xml"
        url = self.fetcher.url_table_of_contents
        self.register_url(url, 
                          toc,
                          match_querystring=True)

    def _load_files_dataset(self, dataset_code=None):

        dataset_zip_filepath = os.path.abspath(os.path.join(RESOURCES_DIR, "%s.sdmx.zip" % dataset_code))
        filepaths = extract_zip_file(dataset_zip_filepath)
        self.DATASETS[dataset_code]["DSD"]["filepaths"]["datastructure"] = filepaths['%s.dsd.xml' % dataset_code]
        self.DATASETS[dataset_code]["filepath"] = filepaths['%s.sdmx.xml' % dataset_code]
        
        url = make_url(dataset_code)
        self.register_url(url, dataset_zip_filepath,
                          content_type='application/zip')

    def _load_files(self, dataset_code=None):
        self._load_files_dataset(dataset_code)
        self._load_files_datatree()
        
    @httpretty.activate     
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    def test_load_datasets_first(self):

        dataset_code = "nama_10_fcs"
        self._load_files(dataset_code)
        self.assertLoadDatasetsFirst([dataset_code])

    @httpretty.activate     
    @unittest.skipUnless('FULL_TEST' in os.environ, "Skip - no full test")
    def test_load_datasets_update(self):

        dataset_code = "nama_10_fcs"
        self._load_files(dataset_code)
        self.assertLoadDatasetsUpdate([dataset_code])

    @httpretty.activate     
    def test_build_data_tree(self):

        dataset_code = "nama_10_fcs"
        self._load_files_datatree()
        self.assertDataTree(dataset_code)
        
    def test_upsert_dataset_nama_10_fcs(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_eurostat:FetcherTestCase.test_upsert_dataset_nama_10_fcs
        
        httpretty.enable()

        dataset_code = "nama_10_fcs"
        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])
        self._load_files_datatree(TOC_FP)
        self._load_files(dataset_code)
        
        self.assertProvider()
        self.assertDataset(dataset_code)
        self.assertSeries(dataset_code)

        '''Reload upsert_dataset for normal fail'''
        with self.assertRaises(RejectUpdatedDataset) as err:
            self.fetcher.upsert_dataset(dataset_code)
        self.assertEqual(err.exception.comments, 
                         "update-date[2015-10-26 00:00:00]")

        '''Verify last_update in category for this dataset'''
        category = Categories.search_category_for_dataset(self.fetcher.provider_name, 
                                                          dataset_code, self.db)
        self.assertIsNotNone(category)
        last_update = None
        for d in category["datasets"]:
            if d["dataset_code"] == dataset_code:
                last_update = d["last_update"]
        self.assertIsNotNone(last_update)
        self.assertEqual(str(last_update), "2015-10-26 00:00:00")
        last_update = None
        
        httpretty.reset()
        httpretty.disable()
        httpretty.enable()
        self._load_files(dataset_code)
            
        '''Change last_update in catalog.xml for force update dataset'''
        toc = open(TOC_FP, 'rb').read()
        toc = toc.replace(b'26.10.2015', b'27.10.2015')
        self.assertTrue(b'27.10.2015' in toc)
        self._load_files_datatree(toc=toc)
        results = self.fetcher.upsert_data_tree(force_update=True)
        self.assertIsNotNone(results)
        self.fetcher.get_selected_datasets(force=True)


        query = {
            'provider_name': self.fetcher.provider_name,
            "dataset_code": dataset_code
        }
        _id = self.db[constants.COL_SERIES].find_one()["_id"]
        deleted = self.db[constants.COL_SERIES].delete_one({"_id": _id})
        self.assertEqual(deleted.deleted_count, 1)

        result = self.fetcher.upsert_dataset(dataset_code)
        self.assertIsNotNone(result) #_id du dataset
        dataset = self.db[constants.COL_DATASETS].find_one(query)
        self.assertIsNotNone(dataset)
        
        self.assertEqual(dataset["last_update"],
                         datetime.datetime(2015, 10, 27, 0, 0))

        #self.assertEqual(dataset["download_last"],
        #                 datetime.datetime(2015, 10, 27, 0, 0))

        httpretty.disable()

    @httpretty.activate
    def test_datasets_list(self):

        # nosetests -s -v dlstats.tests.fetchers.test_eurostat:FetcherTestCase.test_datasets_list

        self._load_files_datatree()

        datasets_list = self.fetcher.datasets_list()

        self.assertEqual(len(datasets_list), 6)

        datasets = [
             {'dataset_code': 'bop_c6_m',
              'name': 'Balance of payments by country - monthly data (BPM6)',
              'last_update': datetime.datetime(2015, 10, 20, 0, 0),
              'metadata': {'data_end': '2015M08',
                            'data_start': '1991M01',
                            'doc_href': 'http://ec.europa.eu/eurostat/cache/metadata/en/bop_6_esms.htm',
                            'values': 4355217}},
             {'dataset_code': 'bop_c6_q',
              'name': 'Balance of payments by country - quarterly data (BPM6)',
              'last_update': datetime.datetime(2015, 10, 23, 0, 0),
              'metadata': {'data_end': '2015Q2',
                            'data_start': '1982',
                            'doc_href': 'http://ec.europa.eu/eurostat/cache/metadata/en/bop_6_esms.htm',
                            'values': 29844073}},
             {'dataset_code': 'dset1',
              'name': 'Dset1',
              'last_update': datetime.datetime(2015, 10, 26, 0, 0),
              'metadata': {'data_end': '2014',
                            'data_start': '1975',
                            'doc_href': 'http://ec.europa.eu/eurostat/cache/metadata/en/nama_10_esms.htm',
                            'values': 417804}},
             {'dataset_code': 'dset2',
              'name': 'Dset2',
              'last_update': datetime.datetime(2015, 10, 26, 0, 0),
              'metadata': {'data_end': '2014',
                            'data_start': '1975',
                            'doc_href': 'http://ec.europa.eu/eurostat/cache/metadata/en/nama_10_esms.htm',
                            'values': 69954}},
             {'dataset_code': 'nama_10_fcs',
              'name': 'Final consumption aggregates by durability',
              'last_update': datetime.datetime(2015, 10, 26, 0, 0),
              'metadata': {'data_end': '2014',
                            'data_start': '1975',
                            'doc_href': 'http://ec.europa.eu/eurostat/cache/metadata/en/nama_10_esms.htm',
                            'values': 69954}},
             {'dataset_code': 'nama_10_gdp',
              'name': 'GDP and main components (output, expenditure and income)',
              'last_update': datetime.datetime(2015, 10, 26, 0, 0),
              'metadata': {'data_end': '2014',
                            'data_start': '1975',
                            'doc_href': 'http://ec.europa.eu/eurostat/cache/metadata/en/nama_10_esms.htm',
                            'values': 417804}}
        ]

        self.assertEqual(datasets_list, datasets)
        
