# -*- coding: utf-8 -*-

import os
import tempfile
import time
from datetime import datetime
import logging
from collections import OrderedDict
import re

import lxml.html
import pytz

from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats.utils import Downloader
from dlstats import errors
from dlstats.xml_utils import (XMLSDMX_2_1 as XMLSDMX,
                               XMLStructure_2_1 as XMLStructure, 
                               XMLSpecificData_2_1_ECB as XMLData,
                               dataset_converter_v2 as dataset_converter)

HTTP_ERROR_LONG_RESPONSE = 413
HTTP_ERROR_NO_RESULT = 404
HTTP_ERROR_BAD_REQUEST = 400
HTTP_ERROR_SERVER_ERROR = 500

VERSION = 4

logger = logging.getLogger(__name__)

SDMX_DATA_HEADERS = {'Accept': 'application/vnd.sdmx.structurespecificdata+xml;version=2.1'}
SDMX_METADATA_HEADERS = {'Accept': 'application/vnd.sdmx.structure+xml;version=2.1'}

CACHE_EXPIRE = 60 * 60 * 4 #4H

class ECB(Fetcher):
    
    def __init__(self, db=None, sdmx=None, **kwargs):        
        super().__init__(provider_name='ECB', db=db, **kwargs)

        if not self.provider:        
            self.provider = Providers(name=self.provider_name,
                                      long_name='European Central Bank',
                                      version=VERSION,
                                      region='Europe',
                                      website='http://www.ecb.europa.eu',
                                      fetcher=self)
        
        if self.provider.version != VERSION:
            self.provider.update_database()

        self.cache_settings = self._get_cache_settings()

        self.xml_sdmx = None
        self.xml_dsd = None
        
        self._dataflows = None
        self._categoryschemes = None
        self._categorisations = None
        self._concepts = None

    def _get_cache_settings(self):

        tmp_filepath = os.path.abspath(os.path.join(tempfile.gettempdir(), 
                                                    self.provider_name))
        
        return {
            "cache_name": tmp_filepath, 
            "backend": 'sqlite', 
            "expire_after": CACHE_EXPIRE
        }

    def _load_structure(self, force=False):
        """Load structure and build data_tree
        """
        
        if self._dataflows and not force:
            return
        
        self.xml_sdmx = XMLSDMX(agencyID=self.provider_name,
                                cache=self.cache_settings)
        
        self.xml_dsd = XMLStructure(provider_name=self.provider_name,
                                    sdmx_client=self.xml_sdmx)       
        
        url = "http://sdw-wsrest.ecb.int/service/dataflow/%s" % self.provider_name
        download = Downloader(url=url, 
                              filename="dataflow.xml",
                              headers=SDMX_METADATA_HEADERS,
                              cache=self.cache_settings)
        self.xml_dsd.process(download.get_filepath())
        self._dataflows = self.xml_dsd.dataflows

        url = "http://sdw-wsrest.ecb.int/service/categoryscheme/%s" % self.provider_name
        download = Downloader(url=url, 
                              filename="categoryscheme.xml",
                              headers=SDMX_METADATA_HEADERS,
                              cache=self.cache_settings)
        self.xml_dsd.process(download.get_filepath())
        self._categoryschemes = self.xml_dsd.categories

        url = "http://sdw-wsrest.ecb.int/service/categorisation/%s" % self.provider_name
        download = Downloader(url=url, 
                              filename="categorisation.xml",
                              headers=SDMX_METADATA_HEADERS,
                              cache=self.cache_settings)
        self.xml_dsd.process(download.get_filepath())
        self._categorisations = self.xml_dsd.categorisations
        
        url = "http://sdw-wsrest.ecb.int/service/conceptscheme/%s" % self.provider_name
        download = Downloader(url=url, 
                              filename="conceptscheme.xml",
                              headers=SDMX_METADATA_HEADERS,
                              cache=self.cache_settings)
        self.xml_dsd.process(download.get_filepath())
        self._concepts = self.xml_dsd.concepts

    def build_data_tree(self, force_update=False):

        self._load_structure()
        
        categories = []
        
        position = 0
        for category_code, category in self.xml_dsd.categories.items():
            parent_ids = self.xml_dsd.iter_parent_category_id(category)

            parent = None
            all_parents = None
            if parent_ids:
                all_parents = parent_ids.copy()
                parent = parent_ids.pop()
            else:
                position += 1
                
            cat = {
                "provider_name": self.provider_name,
                "category_code": category_code,
                "name": category["name"],
                "position": position,
                "parent": parent,
                "all_parents": all_parents, 
                "datasets": [],
                "doc_href": None,
                "metadata": {}
            }
            if category_code in self.xml_dsd.categorisations_categories:
                categorisation_ids = self.xml_dsd.categorisations_categories[category_code]
                
                for categorisation_id in categorisation_ids:
                    categorisation = self.xml_dsd.categorisations[categorisation_id]
                    dataflow_id = categorisation["dataflow"]["id"]
                    if not dataflow_id in self.xml_dsd.dataflows:
                        logger.warning("dataflow[%s] is not in xml_dsd.dataflows" % (dataflow_id))
                        continue
                        
                    dataset = self.xml_dsd.dataflows[dataflow_id]
                    
                    cat["datasets"].append({
                        "dataset_code": dataset['id'], 
                        "name":dataset["name"],
                        "last_update": None,
                        "metadata": {
                            "dsd_id": dataset["dsd_id"]
                        }
                    })
                
            categories.append(cat)
            
        return categories
        
    def parse_agenda(self):
        #TODO: use Downloader
        download = Downloader(url="http://www.ecb.europa.eu/press/calendars/statscal/html/index.en.html",
                              filename="statscall.html")
        with open(download.get_filepath(), 'rb') as fp:
            agenda = lxml.html.parse(fp)
        
        regex_date = re.compile("Reference period: (.*)")
        regex_dataset = re.compile(".*Dataset: (.*)\)")
        entries = agenda.xpath('//div[@class="ecb-faytdd"]/*/dt | '
                               '//div[@class="ecb-faytdd"]/*/dd')[2:]
        entries = zip(entries[::2], entries[1::2])
        for entry in entries:
            item = {}
            match_key = regex_dataset.match(entry[1][0].text_content())
            item['dataflow_key'] = match_key.groups()[0]
            match_date = regex_date.match(entry[1][1].text_content())
            item['reference_period'] = match_date.groups()[0]
            item['scheduled_date'] = entry[0].text_content().replace('\n','')
            yield(item)

    def get_calendar(self):
        datasets = [d["dataset_code"] for d in self.datasets_list()]

        for entry in self.parse_agenda():

            if entry['dataflow_key'] in datasets:

                yield {'action': 'update_node',
                       'kwargs': {'provider_name': self.provider_name,
                                  'dataset_code': entry['dataflow_key']},
                       'period_type': 'date',
                       'period_kwargs': {'run_date': datetime.strptime(
                           entry['scheduled_date'], "%d/%m/%Y %H:%M CET"),
                           'timezone': pytz.timezone('CET')
                       }
                      }

    def upsert_dataset(self, dataset_code):
        
        self._load_structure()
        
        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code,
                           name=None,
                           doc_href=self.provider.website,
                           last_update=datetime.now(),
                           fetcher=self)
        
        _data = ECB_Data(dataset=dataset, fetcher=self)
        dataset.series.data_iterator = _data
        result = dataset.update_database()
        
        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))
        
        return result

    def load_datasets_first(self):
        start = time.time()        
        logger.info("datasets first load. provider[%s] - START" % (self.provider_name))
        
        self._load_structure()

        for dataset in self.datasets_list():
            dataset_code = dataset["dataset_code"]
            try:
                self.upsert_dataset(dataset_code)
            except Exception as err:
                if isinstance(err, errors.MaxErrors):
                    raise
                logger.fatal("error for dataset[%s]: %s" % (dataset_code, str(err)))

        end = time.time() - start
        logger.info("datasets first load. provider[%s] - END - time[%.3f seconds]" % (self.provider_name, end))

    def load_datasets_update(self):
        #TODO: 
        self.load_datasets_first()

class ECB_Data(SeriesIterator):
    
    def __init__(self, dataset=None, fetcher=None):
        """
        :param Datasets dataset: Datasets instance
        """
        super().__init__()
        self.dataset = dataset
        self.fetcher = fetcher
        
        self.attribute_list = self.dataset.attribute_list
        self.dimension_list = self.dataset.dimension_list
        self.provider_name = self.dataset.provider_name
        self.dataset_code = self.dataset.dataset_code

        self.dataset.name = self.fetcher._dataflows[self.dataset_code]["name"]        
        self.dsd_id = self.fetcher._dataflows[self.dataset_code]["dsd_id"]

        self.xml_dsd = XMLStructure(provider_name=self.provider_name)        
        self.xml_dsd.concepts = self.fetcher._concepts
        
        self.rows = None
        self._load()
        
        
    def _load(self):

        url = "http://sdw-wsrest.ecb.int/service/datastructure/ECB/%s?references=children" % self.dsd_id
        download = Downloader(url=url, 
                              filename="dsd-%s.xml" % self.dataset_code,
                              headers=SDMX_METADATA_HEADERS,
                              cache=self.fetcher.cache_settings)
        self.xml_dsd.process(download.get_filepath())
        self._set_dataset()
        
        url = "http://sdw-wsrest.ecb.int/service/data/%s" % self.dataset_code
        download = Downloader(url=url, 
                              filename="data-%s.xml" % self.dataset_code,
                              headers=SDMX_DATA_HEADERS,
                              cache=self.fetcher.cache_settings)

        self.xml_data = XMLData(provider_name=self.provider_name,
                                dataset_code=self.dataset_code,
                                dimension_keys=self.xml_dsd.dimension_keys,
                                dimensions=self.xml_dsd.dimensions)
        
        self.rows = self.xml_data.process(download.get_filepath())

    def _set_dataset(self):

        dimensions = OrderedDict()
        for key, item in self.xml_dsd.dimensions.items():
            dimensions[key] = item["enum"]
        self.dimension_list.set_dict(dimensions)
        
        attributes = OrderedDict()
        for key, item in self.xml_dsd.attributes.items():
            attributes[key] = item["enum"]
        self.attribute_list.set_dict(attributes)
        
        dataset = dataset_converter(self.xml_dsd, self.dataset_code)
        self.dataset.attribute_keys = dataset["attribute_keys"] 
        self.dataset.dimension_keys = dataset["dimension_keys"] 
        self.dataset.concepts = dataset["concepts"] 
        self.dataset.codelists = dataset["codelists"] 

    def build_series(self, bson):
        bson["last_update"] = self.dataset.last_update
        return bson
        

