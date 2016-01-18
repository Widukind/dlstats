# -*- coding: utf-8 -*-

from datetime import datetime
from collections import OrderedDict
from pprint import pprint
import time
import logging

import requests

from dlstats.fetchers._commons import Fetcher, Datasets, Providers
from dlstats.utils import Downloader
from dlstats.xml_utils import XMLCompactData_2_0_DESTATIS as XMLData

VERSION = 1

logger = logging.getLogger(__name__)

#SDMX_DATA_HEADERS = {'Accept': 'application/vnd.sdmx.genericdata+xml;version=2.1'}
#SDMX_METADATA_HEADERS = {'Accept': 'application/vnd.sdmx.structure+xml;version=2.1'}

"""
https://www.destatis.de/EN/FactsFigures/Indicators/ShortTermIndicators/IMF/IMF_IWF.html
"""
DATASETS = {
    'DCS': {
        "name": "Depository corporations survey",
        "doc_href": "http://www.bundesbank.de/Redaktion/EN/Standardartikel/Statistics/sdds_german_contribution_to_the_consolidated_balance_sheet.html",
        'url': 'https://www.destatis.de/sddsplus/DCS.xml',
        'filename': 'DCS.xml',
        'ns_tag_data': 'ns1', #urn:sdmx:org.sdmx.infomodel.datastructure.DataStructure=IMF:ECOFIN_DSD(1.0):ObsLevelDim:TIME_PERIOD
    },
    'NAG': {
        "name": "National Accounts",
        "doc_href": None,
        'url': 'https://www.destatis.de/sddsplus/NAG.xml',
        'filename': 'NAG.xml',
        'ns_tag_data': 'eco',
    },
    'GGO': {
        "name": "General Government Operations",
        "doc_href": None,
        'url': 'https://www.destatis.de/sddsplus/GGO.xml',
        'filename': 'GGO.xml',
        'ns_tag_data': 'eco',
    }            
}

class DESTATIS(Fetcher):
    
    def __init__(self, db=None, **kwargs):        
        super().__init__(provider_name='DESTATIS', db=db, **kwargs)
        
        self.provider = Providers(name=self.provider_name,
                                  long_name='Statistisches Bundesamt',
                                  version=VERSION,
                                  region='Germany',
                                  website='https://www.destatis.de',
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
        
        _data = DESTATIS_Data(dataset=dataset, ns_tag_data=DATASETS[dataset_code]["ns_tag_data"])
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

class DESTATIS_Data(object):
    
    def __init__(self, dataset=None, ns_tag_data=None):
        """
        :param Datasets dataset: Datasets instance
        """        
        self.dataset = dataset
        self.ns_tag_data = ns_tag_data
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

        url = "https://www.destatis.de/sddsplus/%s.xml" % self.dataset_code
        download = Downloader(url=url, 
                              filename="data-%s.xml" % self.dataset_code,
                              #headers=SDMX_DATA_HEADERS
                              )

        self.xml_data = XMLData(provider_name=self.provider_name,
                                dataset_code=self.dataset_code,
                                ns_tag_data=self.ns_tag_data,
                                #dimension_keys=self.xml_dsd.dimension_keys
                                )
        
        #TODO: response and exception
        try:
            filepath, response = download.get_filepath_and_response()        
        except requests.exceptions.HTTPError as err:
            logger.critical("AUTRE ERREUR HTTP : %s" % err.response.status_code)
            raise
            
        self.rows = self.xml_data.process(filepath)

    def __next__(self):
        _series = next(self.rows)
        if not _series:
            raise StopIteration()
        
        return self.build_series(_series)

    def build_series(self, bson):
        bson["last_update"] = self.dataset.last_update
        
        for key, item in bson['dimensions'].items():
            self.dimension_list.update_entry(key, key, item)

        """
        FIXME: 
        attributes = OrderedDict()
        if 'attributes' in bson and bson['attributes']:
            for key, item in bson['attributes'].items():
                attributes[key] = {key: item}
        pprint(attributes)
        self.attribute_list.set_dict(attributes)
        
        pprint(self.dataset.bson)
        pprint(bson)
        """
        return bson
        

