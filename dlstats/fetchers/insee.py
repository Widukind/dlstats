# -*- coding: utf-8 -*-

import time
import urllib
from datetime import datetime
import logging
from collections import OrderedDict

import requests
import pandas
from lxml import etree

from pandasdmx.api import Request

from dlstats.fetchers._commons import Fetcher, Datasets, Providers
from dlstats import constants
from dlstats.utils import Downloader
from dlstats.xml_utils import (XMLStructure_2_1, 
                               XMLSpecificData_2_1_INSEE as XMLData)

HTTP_ERROR_LONG_RESPONSE = 413
HTTP_ERROR_NO_RESULT = 404
HTTP_ERROR_BAD_REQUEST = 400
HTTP_ERROR_SERVER_ERROR = 500

VERSION = 3

SDMX_DATA_HEADERS = {'Accept': 'application/vnd.sdmx.structurespecificdata+xml;version=2.1'}
SDMX_METADATA_HEADERS = {'Accept': 'application/vnd.sdmx.structure+xml;version=2.1'}


logger = logging.getLogger(__name__)

class ContinueRequest(Exception):
    pass

def TODO_parse_agenda(self):
    """Parse agenda of new releases and schedule jobs"""
    
    #TODO: calendrier: RSS 2.0
    
    DATEEXP = re.compile("(January|February|March|April|May|June|July|August|September|October|November|December)[ ]+\d+[ ]*,[ ]+\d+[ ]+\d+:\d+")
    url = 'http://www.insee.fr/en/publics/presse/agenda.asp'
    agenda = BeautifulSoup(urllib.request.urlopen(url))
    ul = agenda.find('div',id='contenu').find('ul','liens')
    for li in ul.find_all('li'):
        href = li.find('a')['href']
        groups = parse_theme(href)
        text = li.find('p','info').string
        date = datetime.datetime.strptime(DATEEXP.match(text).group(),'%B %d, %Y %H:%M')
        print(date)

def TODO_parse_theme(self,url):
    """Find updated code groups"""
    
    #    url = "http://localhost:8800/insee/industrial_production.html"
    theme = BeautifulSoup(urllib.request.urlopen(url))
    p = theme.find('div',id='savoirplus').find('p')
    groups = []
    for a in p.find_all('a'):
        groups += [a.string[1:]]
    return groups

class INSEE(Fetcher):
    
    def __init__(self, db=None, sdmx=None, **kwargs):        
        super().__init__(provider_name='INSEE', db=db, **kwargs)

        if not self.provider:        
            self.provider = Providers(name=self.provider_name,
                                     long_name='National Institute of Statistics and Economic Studies',
                                     version=VERSION,
                                     region='France',
                                     website='http://www.insee.fr',
                                     fetcher=self)
            self.provider.update_database()
        
        if self.provider.version != VERSION:
            self.provider.update_database()
            
        
        self.sdmx = sdmx or Request(agency='INSEE')
        
        self._dataflows = None
        self._categoryschemes = None
        self._categorisations = None
    
    def _load_structure(self, force=False):
        
        if self._dataflows and not force:
            return
        
        """
        #http://www.bdm.insee.fr/series/sdmx/categoryscheme
        categoryscheme_response = self.sdmx.get(resource_type='categoryscheme', params={"references": None})
        logger.debug(categoryscheme_response.url)
        self._categoryschemes = categoryscheme_response.msg.categoryschemes
    
        #http://www.bdm.insee.fr/series/sdmx/categorisation
        categorisation_response = self.sdmx.get(resource_type='categorisation')
        logger.debug(categorisation_response.url)
        self._categorisations = categorisation_response.msg.categorisations
        """
    
        #http://www.bdm.insee.fr/series/sdmx/dataflow
        dataflows_response = self.sdmx.get(resource_type='dataflow')    
        logger.debug(dataflows_response.url)
        self._dataflows = dataflows_response.msg.dataflows

    def load_datasets_first(self):
        start = time.time()        
        logger.info("datasets first load. provider[%s] - START" % (self.provider_name))
        
        for dataset_code in self.datasets_list():
            try:
                self.upsert_dataset(dataset_code)
            except Exception as err:
                logger.fatal("error for dataset[%s]: %s" % (dataset_code, str(err)))

        end = time.time() - start
        logger.info("update fetcher[%s] - END - time[%.3f seconds]" % (self.provider_name, end))

    def load_datasets_update(self):
        #TODO: 
        self.load_datasets_first()

    def build_data_tree(self, force_update=False):
        """Build data_tree from structure datas
        """
        if self.provider.count_data_tree() > 1 and not force_update:
            return self.provider.data_tree
        
        self._load_structure()
        
        for dataset_code, dataset in self._dataflows.items():

            name = dataset.name
            if "en" in dataset.name:
                name = dataset.name.en
            else:
                name = dataset.name.fr
            
            self.provider.add_dataset(dict(dataset_code=dataset_code, name=name), self.provider_name)
            
        return self.provider.data_tree

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
    
    def upsert_dataset(self, dataset_code):

        #self.load_structure(force=False)
        
        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        
        #if not dataset_code in self._dataflows:
        #    raise Exception("This dataset is unknown: %s" % dataset_code)
        
        #dataflow = self._dataflows[dataset_code]
        
        #cat = self.db[constants.COL_CATEGORIES].find_one({'category_code': dataset_code})
        #dataset.name = cat['name']
        #dataset.doc_href = cat['doc_href']
        #dataset.last_update = cat['last_update']

        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code,
                           #name=dataflow.name.en,
                           doc_href=None,
                           last_update=datetime.now(), #TODO:
                           fetcher=self)
        
        dataset_doc = self.db[constants.COL_DATASETS].find_one({'provider_name': self.provider_name,
                                                                "dataset_code": dataset_code})
        
        insee_data = INSEE_Data(dataset=dataset,
                                dataset_doc=dataset_doc, 
                                #dataflow=dataflow, 
                                #sdmx=self.sdmx
                                )
        dataset.series.data_iterator = insee_data
        result = dataset.update_database()
        
        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))
        
        """
        > IDBANK:  A définir dynamiquement sur site ?
        doc_href d'une serie: http://www.bdm.insee.fr/bdm2/affichageSeries?idbank=001694226
        > CODE GROUPE: Balance des Paiements mensuelle - Compte de capital
        http://www.bdm.insee.fr/bdm2/choixCriteres?codeGroupe=1556
        """
        return result

class INSEE_Data(object):
    
    def __init__(self, dataset=None, dataset_doc=None):
        """
        :param Datasets dataset: Datasets instance
        """
        self.dataset = dataset
        self.dataset_doc = dataset_doc
        self.attribute_list = self.dataset.attribute_list
        self.dimension_list = self.dataset.dimension_list
        self.provider_name = self.dataset.provider_name
        self.dataset_code = self.dataset.dataset_code

        if self.dataset_doc:
            self.last_update = self.dataset_doc["last_update"]

        self.xml_dsd = XMLStructure_2_1(provider_name=self.provider_name, 
                                        dataset_code=self.dataset_code)        
        
        self.rows = None
        self._load()
        
    def _load(self):

        self.dsd_id = self.dataset_code

        url = "http://www.bdm.insee.fr/series/sdmx/datastructure/INSEE/%s?references=children" % self.dsd_id
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
        
        url = "http://www.bdm.insee.fr/series/sdmx/data/%s" % self.dataset_code
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

    def is_updated(self, bson):
        """Verify if series changes
        """
        if not self.last_update:
            return True
        
        series_updated = bson['last_update']
        _is_updated = series_updated > self.last_update

        if not _is_updated and logger.isEnabledFor(logging.INFO):
            logger.info("bypass updated dataset_code[%s][%s] - idbank[%s][%s]" % (self.dataset_code,
                                                                                 self.last_update, 
                                                                                 bson['key'],
                                                                                 series_updated))
        
        return _is_updated

    def build_series(self, bson):
        #TODO: last_update : update dataset ?
        #bson["last_update"] = self.last_update
        return bson

