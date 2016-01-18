# -*- coding: utf-8 -*-

from pprint import pprint
import time
from datetime import datetime
import pytz
import logging
from collections import OrderedDict

import requests

from pandasdmx.api import Request

from dlstats.fetchers._commons import Fetcher, Datasets, Providers
from dlstats.utils import Downloader
from dlstats.xml_utils import (XMLStructure_2_1, 
                               XMLSpecificData_2_1_ECB as XMLData)

import lxml.html
import re

HTTP_ERROR_LONG_RESPONSE = 413
HTTP_ERROR_NO_RESULT = 404
HTTP_ERROR_BAD_REQUEST = 400
HTTP_ERROR_SERVER_ERROR = 500

VERSION = 3

logger = logging.getLogger(__name__)

class ContinueRequest(Exception):
    pass

SDMX_DATA_HEADERS = {'Accept': 'application/vnd.sdmx.structurespecificdata+xml;version=2.1'}
SDMX_METADATA_HEADERS = {'Accept': 'application/vnd.sdmx.structure+xml;version=2.1'}

class ECBRequest(Request):
    Request._agencies['ECB']['resources'] = {
        'data': {
            'headers': SDMX_DATA_HEADERS,
        },
    }
    for r in Request._resources:
        Request._agencies['ECB']['resources'][r] = {'headers': SDMX_METADATA_HEADERS}

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
            self.provider.update_database()
        
        if self.provider.version != VERSION:
            self.provider.update_database()
            
        self.sdmx = sdmx or ECBRequest(agency=self.provider_name)
        self.sdmx.timeout = 90
        
        self._dataflows = None
        self._categoryschemes = None
        self._categorisations = None

    def _load_structure(self, force=False):
        """Load structure and build data_tree
        """
        
        if (self._dataflows and self._categoryschemes and self._categorisations) and not force:
            return
        
        '''Force URL for select only ECB agency'''
        categoryschemes_response = self.sdmx.get(resource_type='categoryscheme', url='http://sdw-wsrest.ecb.int/service/categoryscheme/%s?references=parentsandsiblings' % self.provider_name)
        self._categorisations = categoryschemes_response.msg.categorisations
        self._categoryschemes = categoryschemes_response.msg.categoryschemes
        self._dataflows = categoryschemes_response.msg.dataflows
        
    def build_data_tree(self, force_update=False):
        """Build data_tree from structure datas
        """
        if self.provider.count_data_tree() > 1 and not force_update:
            return self.provider.data_tree
        
        self._load_structure()

        for category in self._categoryschemes.aslist():
            
            _category = dict(name=category.name.en,
                             category_code=category.id)
            category_key = self.provider.add_category(_category)
             
            for subcategory in category.values():
                
                if not subcategory.id in self._categorisations:
                    continue
                
                _subcategory = dict(name=subcategory.name.en,
                                    category_code=subcategory.id)
                _subcategory_key = self.provider.add_category(_subcategory,
                                           parent_code=category_key)
                
                try:
                    _categorisation = self._categorisations[subcategory.id]
                    for i in _categorisation:
                        _d = self._dataflows[i.artefact.id]
                        self.provider.add_dataset(dict(dataset_code=_d.id, name=_d.name.en), _subcategory_key)                        
                except Exception as err:
                    logger.error(err)   
                    raise                             

        return self.provider.data_tree
        
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
        
        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        
        #TODO: control si existe ou update !!!

        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code,
                           name=None,
                           doc_href=self.provider.website,
                           last_update=datetime.now(),
                           fetcher=self)
        
        _data = ECB_Data(dataset=dataset)
        dataset.series.data_iterator = _data
        try:
            result = dataset.update_database()
        except:
            raise
        
        _data = None

        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))
        
        return result

    def load_datasets_first(self):
        start = time.time()        
        logger.info("datasets first load. provider[%s] - START" % (self.provider_name))
        
        self._load_structure()
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

class ECB_Data(object):
    
    def __init__(self, dataset=None):
        """
        :param Datasets dataset: Datasets instance
        """        
        self.dataset = dataset
        self.attribute_list = self.dataset.attribute_list
        self.dimension_list = self.dataset.dimension_list
        self.provider_name = self.dataset.provider_name
        self.dataset_code = self.dataset.dataset_code

        self.xml_dsd = XMLStructure_2_1(provider_name=self.provider_name, 
                                        dataset_code=self.dataset_code)        
        
        self.rows = None
        self.dsd_id = None
        
        self._load()
        
        
    def _load(self):

        url = "http://sdw-wsrest.ecb.int/service/dataflow/ECB/%s" % self.dataset_code
        download = Downloader(url=url, 
                              filename="dataflow-%s.xml" % self.dataset_code,
                              headers=SDMX_METADATA_HEADERS)
        
        self.xml_dsd.process(download.get_filepath())
        self.dsd_id = self.xml_dsd.dsd_id
        
        if not self.dsd_id:
            msg = "DSD ID not found for provider[%s] - dataset[%s]" % (self.provider_name, 
                                                                       self.dataset_code)
            raise Exception(msg)
        
        url = "http://sdw-wsrest.ecb.int/service/datastructure/ECB/%s?references=children" % self.dsd_id
        download = Downloader(url=url, 
                              filename="dsd-%s.xml" % self.dataset_code,
                              headers=SDMX_METADATA_HEADERS)
        self.xml_dsd.process(download.get_filepath())
        
        self.dataset.name = self.xml_dsd.dataset_name
        
        dimensions = OrderedDict()
        for key, item in self.xml_dsd.dimensions.items():
            dimensions[key] = item["dimensions"]
        self.dimension_list.set_dict(dimensions)
        
        attributes = OrderedDict()
        for key, item in self.xml_dsd.attributes.items():
            attributes[key] = item["values"]
        self.attribute_list.set_dict(attributes)
        
        url = "http://sdw-wsrest.ecb.int/service/data/%s" % self.dataset_code
        download = Downloader(url=url, 
                              filename="data-%s.xml" % self.dataset_code,
                              headers=SDMX_DATA_HEADERS)

        self.xml_data = XMLData(provider_name=self.provider_name,
                                dataset_code=self.dataset_code,
                                dimension_keys=self.xml_dsd.dimension_keys)
        
        
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
        return bson
        

