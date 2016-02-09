# -*- coding: utf-8 -*-

import tempfile
import datetime
import os
from copy import deepcopy

from dlstats.fetchers.eurostat import Eurostat as Fetcher, make_url

import httpretty

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
        "concept_keys": ['FREQ', 'geo', 'unit', 'na_item', 'OBS_STATUS'], #'TIME_FORMAT', 
        "codelist_keys": ['FREQ', 'geo', 'unit', 'na_item', 'OBS_STATUS'], #'TIME_FORMAT', 
        "codelist_count": {
            "FREQ": 9,
            "geo": 33,
            "unit": 12,
            "na_item": 10,
            #"TIME_FORMAT": 7,
            "OBS_STATUS": 12,                           
        },
        "attribute_keys": ["OBS_STATUS"], #"TIME_FORMAT", 
        "attribute_count": {
            #"TIME_FORMAT": 7,
            "OBS_STATUS": 12,
        }, 
    }
}

class FetcherTestCase(BaseFetcherTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_eurostat:FetcherTestCase
    
    FETCHER_KLASS = Fetcher
    DEBUG_MODE = False
    DATASETS = {
        'nama_10_fcs': deepcopy(xml_samples.DATA_EUROSTAT)
    }
    DATASET_FIRST = "bop_c6_m"
    DATASET_LAST = "nama_10_gdp"
    
    def _load_files_datatree(self):
        
        url = self.fetcher.url_table_of_contents
        self.register_url(url, 
                          TOC_FP,
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
    def test_load_datasets_first(self):

        dataset_code = "nama_10_fcs"
        self._load_files(dataset_code)
        self.assertLoadDatasetsFirst([dataset_code])

    @httpretty.activate     
    def test_load_datasets_update(self):

        dataset_code = "nama_10_fcs"
        self._load_files(dataset_code)
        self.assertLoadDatasetsUpdate([dataset_code])

    @httpretty.activate     
    def test_build_data_tree(self):

        dataset_code = "nama_10_fcs"
        self._load_files_datatree()
        self.assertDataTree(dataset_code)
        
    @httpretty.activate
    def test_upsert_dataset_nama_10_fcs(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_eurostat:FetcherTestCase.test_upsert_dataset_nama_10_fcs

        dataset_code = "nama_10_fcs"
        self.DATASETS[dataset_code]["DSD"].update(LOCAL_DATASETS_UPDATE[dataset_code])
        self._load_files(dataset_code)
        
        self.assertProvider()
        self.assertDataset(dataset_code)        
        self.assertSeries(dataset_code)
        
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
        
