# -*- coding: utf-8 -*-

import os
from datetime import datetime
from collections import OrderedDict
from pprint import pprint
import time
import logging
import zipfile

import requests

from dlstats.fetchers._commons import Fetcher, Datasets, Providers
from dlstats.utils import Downloader
from dlstats.xml_utils import XMLData_1_0_FED as XMLData

VERSION = 1

logger = logging.getLogger(__name__)

DATASETS = {
    'G19': {
        "name": "G.19 - Consumer Credit",
        "doc_href": None,
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G19&filetype=zip',
    },
    'Z1': {
        "name": "Flow of Funds Z.1",
        "doc_href": None,
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=Z1&filetype=zip',
    },
}

def extract_zip_file(zipfilepath):
    zfile = zipfile.ZipFile(zipfilepath)
    filepaths = []
    for filename in zfile.namelist():
        if filename.endswith("struct.xml") or filename.endswith("data.xml"):
            filepath = zfile.extract(filename, os.path.dirname(zipfilepath))
            filepaths.append(os.path.abspath(filepath))
            #filepaths.update({filename: zfile.extract(filename, os.path.dirname(zipfilepath))})
    return sorted(filepaths)

class FED(Fetcher):
    
    def __init__(self, db=None, **kwargs):        
        super().__init__(provider_name='FED', db=db, **kwargs)
        
        self.provider = Providers(name=self.provider_name,
                                  long_name='Federal Reserve',
                                  version=VERSION,
                                  region='US',
                                  website='http://www.federalreserve.gov',
                                  fetcher=self)

    def build_data_tree(self, force_update=False):
        
        if self.provider.count_data_tree() > 1 and not force_update:
            return self.provider.data_tree

        for category_code, dataset in DATASETS.items():
            category_key = self.provider.add_category({"name": dataset["name"],
                                                       "category_code": category_code,
                                                       "doc_href": dataset["doc_href"]})
            _dataset = {"name": dataset["name"], "dataset_code": category_code}
            self.provider.add_dataset(_dataset, category_key)
        
        return self.provider.data_tree

    def upsert_dataset(self, dataset_code):
        
        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        
        #TODO: control si existe ou update !!!

        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code,
                           name=DATASETS[dataset_code]['name'],
                           doc_href=DATASETS[dataset_code]['doc_href'],
                           last_update=datetime.now(),
                           fetcher=self)
        
        _data = FED_Data(dataset=dataset, 
                         url=DATASETS[dataset_code]['url'])
        dataset.series.data_iterator = _data
        result = dataset.update_database()
        
        _data = None

        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))
        
        return result

    def load_datasets_first(self):
        start = time.time()        
        logger.info("datasets first load. provider[%s] - START" % (self.provider_name))
        
        self.provider.update_database()
        self.upsert_data_tree()

        datasets_list = [d["dataset_code"] for d in self.datasets_list()]
        for dataset_code in datasets_list:
            try:
                self.upsert_dataset(dataset_code)
            except Exception as err:
                logger.fatal("error for dataset[%s]: %s" % (dataset_code, str(err)))

        end = time.time() - start
        logger.info("datasets first load. provider[%s] - END - time[%.3f seconds]" % (self.provider_name, end))

    def load_datasets_update(self):
        #TODO: 
        self.load_datasets_first()

class FED_Data(object):
    
    def __init__(self, dataset=None, url=None):
        """
        :param Datasets dataset: Datasets instance
        """        
        self.dataset = dataset
        self.url = url
        self.attribute_list = self.dataset.attribute_list
        self.dimension_list = self.dataset.dimension_list
        self.provider_name = self.dataset.provider_name
        self.dataset_code = self.dataset.dataset_code

        #self.xml_dsd = XMLStructure_2_1(provider_name=self.provider_name, 
        #                                dataset_code=self.dataset_code)        
        
        self.rows = None
        #self.dsd_id = None
        
        self._load()
        
        
    def _load(self):

        download = Downloader(url=self.url, 
                              filename="data-%s.xml" % self.dataset_code,
                              #headers=SDMX_DATA_HEADERS        
                              )
        data_fp, dsd_fp = (extract_zip_file(download.get_filepath()))

        self.xml_data = XMLData(provider_name=self.provider_name,
                                dataset_code=self.dataset_code,
                                #dimension_keys=self.xml_dsd.dimension_keys
                                )
        
        self.rows = self.xml_data.process(data_fp)

    def __next__(self):
        _series = next(self.rows)
        if not _series:
            raise StopIteration()
        
        return self.build_series(_series)

    def build_series(self, bson):
        bson["last_update"] = self.dataset.last_update
        
        for key, item in bson['dimensions'].items():
            self.dimension_list.update_entry(key, key, item)

        return bson
        

