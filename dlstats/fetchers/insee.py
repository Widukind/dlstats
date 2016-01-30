# -*- coding: utf-8 -*-

import tempfile
import os
import time
from datetime import datetime
import logging
from collections import OrderedDict

from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats import constants
from dlstats.utils import Downloader, clean_datetime, remove_file_and_dir
from dlstats import errors
from dlstats.xml_utils import (XMLSDMX_2_1 as XMLSDMX,
                               XMLStructure_2_1 as XMLStructure, 
                               XMLSpecificData_2_1_INSEE as XMLData,
                               dataset_converter_v2 as dataset_converter)

HTTP_ERROR_LONG_RESPONSE = 413
HTTP_ERROR_NO_RESULT = 404
HTTP_ERROR_BAD_REQUEST = 400
HTTP_ERROR_SERVER_ERROR = 500

VERSION = 3

SDMX_DATA_HEADERS = {'Accept': 'application/vnd.sdmx.structurespecificdata+xml;version=2.1'}
SDMX_METADATA_HEADERS = {'Accept': 'application/vnd.sdmx.structure+xml;version=2.1'}

CACHE_EXPIRE = 60 * 60 * 4 #4H

logger = logging.getLogger(__name__)

class INSEE(Fetcher):
    
    def __init__(self, db=None, sdmx=None, **kwargs):
        super().__init__(provider_name='INSEE', db=db, **kwargs)

        self.provider = Providers(name=self.provider_name,
                                 long_name='National Institute of Statistics and Economic Studies',
                                 version=VERSION,
                                 region='France',
                                 website='http://www.insee.fr',
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
        self._codelists = OrderedDict()
        
    def _get_cache_settings(self):

        tmp_filepath = os.path.abspath(os.path.join(tempfile.gettempdir(), 
                                                    self.provider_name))
        
        return {
            "cache_name": tmp_filepath, 
            "backend": 'sqlite', 
            "expire_after": CACHE_EXPIRE
        }
        
    def _add_metadata(self):
        return
        #TODO:
        self.provider.metadata = {
            "web": {
                "remote_series": "http://www.bdm.insee.fr/bdm2/affichageSeries?idbank=%(key)s",
                "remote_datasets": "http://www.bdm.insee.fr/bdm2/affichageSeries?idbank=%(dataset_code)s",
                "remote_category": None,
            }
        }
    
    def _load_structure(self, force=False):
        
        if self._dataflows and not force:
            return

        for_delete = []

        self.xml_sdmx = XMLSDMX(agencyID=self.provider_name,
                                cache=self.cache_settings)
        
        self.xml_dsd = XMLStructure(provider_name=self.provider_name,
                                    sdmx_client=self.xml_sdmx)       
        
        
        url = "http://www.bdm.insee.fr/series/sdmx/dataflow/%s" % self.provider_name
        download = Downloader(url=url, 
                              filename="dataflow.xml",
                              headers=SDMX_METADATA_HEADERS,
                              cache=self.cache_settings)
        filepath = download.get_filepath()
        for_delete.append(filepath)
        self.xml_dsd.process(filepath)
        self._dataflows = self.xml_dsd.dataflows

        url = "http://www.bdm.insee.fr/series/sdmx/categoryscheme/%s" % self.provider_name
        download = Downloader(url=url, 
                              filename="categoryscheme.xml",
                              headers=SDMX_METADATA_HEADERS,
                              cache=self.cache_settings)
        filepath = download.get_filepath()
        for_delete.append(filepath)
        self.xml_dsd.process(filepath)
        self._categoryschemes = self.xml_dsd.categories

        url = "http://www.bdm.insee.fr/series/sdmx/categorisation/%s" % self.provider_name
        download = Downloader(url=url, 
                              filename="categorisation.xml",
                              headers=SDMX_METADATA_HEADERS,
                              cache=self.cache_settings)
        filepath = download.get_filepath()
        for_delete.append(filepath)
        self.xml_dsd.process(filepath)
        self._categorisations = self.xml_dsd.categorisations
        
        url = "http://www.bdm.insee.fr/series/sdmx/conceptscheme/%s" % self.provider_name
        download = Downloader(url=url, 
                              filename="conceptscheme.xml",
                              headers=SDMX_METADATA_HEADERS,
                              cache=self.cache_settings)
        filepath = download.get_filepath()
        for_delete.append(filepath)
        self.xml_dsd.process(filepath)
        self._concepts = self.xml_dsd.concepts

        for fp in for_delete:
            remove_file_and_dir(fp)
        
    def load_datasets_first(self):
                
        self._load_structure()

        start = time.time()        
        logger.info("datasets first load. provider[%s] - START" % (self.provider_name))
        
        for dataset in self.datasets_list():
            dataset_code = dataset.pop("dataset_code")
            try:
                self.upsert_dataset(dataset_code, **dataset)
            except Exception as err:
                if isinstance(err, errors.MaxErrors):
                    raise
                logger.fatal("error for dataset[%s]: %s" % (dataset_code, str(err)))

        end = time.time() - start
        logger.info("update fetcher[%s] - END - time[%.3f seconds]" % (self.provider_name, end))

    def load_datasets_update(self):
        #TODO: 
        self.load_datasets_first()

    def build_data_tree(self, force_update=False):
        """Build data_tree from structure datas
        """
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

    def upsert_dataset(self, dataset_code, **kwargs):

        self._load_structure()

        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code,
                           name=kwargs.get('name', None),
                           doc_href=None,
                           last_update=clean_datetime(),
                           fetcher=self)
        
        dataset_doc = self.db[constants.COL_DATASETS].find_one({'provider_name': self.provider_name,
                                                                "dataset_code": dataset_code})
        
        insee_data = INSEE_Data(dataset=dataset,
                                dataset_doc=dataset_doc, 
                                fetcher=self)
        dataset.series.data_iterator = insee_data
        result = dataset.update_database()
        
        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))
        
        return result

class INSEE_Data(SeriesIterator):
    
    def __init__(self, dataset=None, dataset_doc=None, fetcher=None):
        """
        :param Datasets dataset: Datasets instance
        """
        super().__init__()
        self.dataset = dataset
        self.dataset_doc = dataset_doc
        self.fetcher = fetcher
        
        self.attribute_list = self.dataset.attribute_list
        self.dimension_list = self.dataset.dimension_list
        self.provider_name = self.dataset.provider_name
        self.dataset_code = self.dataset.dataset_code
        
        self.dataset.name = self.fetcher._dataflows[self.dataset_code]["name"]        
        self.dsd_id = self.fetcher._dataflows[self.dataset_code]["dsd_id"]
        
        self.last_update = self.dataset.download_last #self.dataset.last_update
        if self.dataset_doc:
            #self.last_update = self.dataset_doc["last_update"]
            self.last_update = self.dataset_doc["download_last"]

        self.xml_dsd = XMLStructure(provider_name=self.provider_name,
                                    sdmx_client=self.fetcher.xml_sdmx)        
        self.xml_dsd.concepts = self.fetcher._concepts
        self.xml_dsd.codelists = self.fetcher._codelists

        self.rows = None
        self._load()

    def _load(self):
        self._load_dsd()
        self._load_data()
        
    def _load_dsd_by_element(self):
        
        url = "http://www.bdm.insee.fr/series/sdmx/datastructure/INSEE/%s" % self.dsd_id
        download = Downloader(url=url, 
                              filename="datastructure-%s.xml" % self.dsd_id,
                              headers=SDMX_METADATA_HEADERS,
                              cache=self.fetcher.cache_settings)
        filepath = download.get_filepath()
        self.dataset.for_delete.append(filepath)
        self.xml_dsd.process(filepath)
        self._set_dataset()
        
    def _load_dsd(self):
        """
        #TODO: il y a une DSD pour chaque groupe de séries (soit environ 400),
        - download 1 dsd partage par plusieurs dataset
        - 668 datase
        """

        url = "http://www.bdm.insee.fr/series/sdmx/datastructure/INSEE/%s?references=children" % self.dsd_id
        download = Downloader(url=url, 
                              filename="dsd-%s.xml" % self.dsd_id,
                              headers=SDMX_METADATA_HEADERS,
                              cache=self.fetcher.cache_settings)
        filepath, response = download.get_filepath_and_response()
        
        if response.status_code == HTTP_ERROR_LONG_RESPONSE:
            self._load_dsd_by_element()
            return
        elif response.status_code >= 400:
            raise response.raise_for_status()
        
        self.dataset.for_delete.append(filepath)
        self.xml_dsd.process(filepath)
        self._set_dataset()
        
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
        
        self.fetcher._codelists.update(self.xml_dsd.codelists)

    def _load_data(self):
        
        url = "http://www.bdm.insee.fr/series/sdmx/data/%s" % self.dataset_code
        download = Downloader(url=url, 
                              filename="data-%s.xml" % self.dataset_code,
                              headers=SDMX_DATA_HEADERS,
                              cache=self.fetcher.cache_settings)

        #TODO: ?startperiod="2008"
        self.xml_data = XMLData(provider_name=self.provider_name,
                                dataset_code=self.dataset_code,
                                dimension_keys=self.xml_dsd.dimension_keys,
                                dimensions=self.xml_dsd.dimensions)
        
        filepath, response = download.get_filepath_and_response()
        if response.status_code == HTTP_ERROR_LONG_RESPONSE:
            self.rows = self.get_data_by_dimension()
            return
        elif response.status_code >= 400:
            raise response.raise_for_status()

        self.dataset.for_delete.append(filepath)
        self.rows = self.xml_data.process(filepath)

    def select_short_dimension(self):
        """Renvoi le nom de la dimension qui contiens le moins de valeur
        pour servir ensuite de filtre dans le chargement des données (..A.)
        en tenant compte du nombre de dimension et de la position 
        """
        dimension_keys = self.xml_dsd.dimension_keys

        _dimensions = {}        
        for dim_id, dim in self.xml_dsd.dimensions.items():
            _dimensions[dim_id] = len(dim["enum"].keys())
        
        _key = min(_dimensions, key=_dimensions.get)
        
        position = dimension_keys.index(_key)         
        dimension_values = list(self.xml_dsd.dimensions[_key]["enum"].keys())
        return (position, 
                len(self.xml_dsd.dimension_keys), 
                _key, 
                dimension_values)
            
    def get_data_by_dimension(self):

        position, count_dimensions, _key, dimension_values = self.select_short_dimension()
        
        for dimension_value in dimension_values:
            '''Pour chaque valeur de la dimension, generer une key d'url'''
                        
            sdmx_key = []
            for i in range(count_dimensions):
                if i == position:
                    sdmx_key.append(dimension_value)
                else:
                    sdmx_key.append(".")
            key = "".join(sdmx_key)

            url = "http://www.bdm.insee.fr/series/sdmx/data/%s/%s" % (self.dataset_code, key)
            
            download = Downloader(url=url, 
                                  filename="data-%s.xml" % self.dataset_code,
                                  headers=SDMX_DATA_HEADERS,
                                  cache=self.fetcher.cache_settings)
            filepath, response = download.get_filepath_and_response()
            
            if response.status_code == HTTP_ERROR_NO_RESULT:
                continue
            elif response.status_code >= 400:
                raise response.raise_for_status()

            self.dataset.for_delete.append(filepath)
            
            for row in self.xml_data.process(filepath):
                yield row
    
    def is_updated(self, bson):
        """Verify if series changes
        
        Return True si la series doit etre mise a jour et False si elle est a jour  
        """
        if not self.last_update:
            return True
        
        series_updated = bson.get('last_update', None)
        if not series_updated:
            return True
        
        if series_updated > self.last_update:
            return True

        return False

    def build_series(self, bson):
        if not self.is_updated(bson):
            raise errors.RejectUpdatedSeries(provider_name=self.provider_name,
                                             dataset_code=self.dataset_code,
                                             key=bson.get('key'))
        return bson

