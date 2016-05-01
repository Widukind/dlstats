# -*- coding: utf-8 -*-

import os
import logging
import re
from datetime import datetime
from collections import OrderedDict

from pyquery import PyQuery as pq
import requests

from widukind_common import errors

from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats import constants
from dlstats.utils import Downloader, clean_datetime
from dlstats.xml_utils import (XMLSDMX_2_1 as XMLSDMX,
                               XMLStructure_2_1 as XMLStructure, 
                               XMLSpecificData_2_1_INSEE as XMLData,
                               dataset_converter,
                               select_dimension,
                               get_dimensions_from_dsd)

HTTP_ERROR_LONG_RESPONSE = 413
HTTP_ERROR_NO_RESULT = 404
HTTP_ERROR_BAD_REQUEST = 400
HTTP_ERROR_SERVER_ERROR = 500

VERSION = 5

SDMX_DATA_HEADERS = {'Accept': 'application/vnd.sdmx.structurespecificdata+xml;version=2.1'}
SDMX_METADATA_HEADERS = {'Accept': 'application/vnd.sdmx.structure+xml;version=2.1'}

FREQUENCIES_SUPPORTED = ["A", "M", "Q", "W", "D"]
FREQUENCIES_REJECTED = ["S", "B", "I"]

logger = logging.getLogger(__name__)

def download_page(url):
        try:
            response = requests.get(url)

            if not response.ok:
                msg = "download url[%s] - status_code[%s] - reason[%s]" % (url, 
                                                                           response.status_code, 
                                                                           response.reason)
                logger.error(msg)
                raise Exception(msg)
            
            return response.content
                
            #TODO: response.close() ?
            
        except requests.exceptions.ConnectionError as err:
            raise Exception("Connection Error")
        except requests.exceptions.ConnectTimeout as err:
            raise Exception("Connect Timeout")
        except requests.exceptions.ReadTimeout as err:
            raise Exception("Read Timeout")
        except Exception as err:
            raise Exception("Not captured exception : %s" % str(err))            


class INSEE(Fetcher):
    
    def __init__(self, **kwargs):
        super().__init__(provider_name='INSEE', version=VERSION, **kwargs)

        self.provider = Providers(name=self.provider_name,
                                 long_name='National Institute of Statistics and Economic Studies',
                                 version=VERSION,
                                 region='France',
                                 website='http://www.insee.fr',
                                 fetcher=self)
        
        self.xml_sdmx = None
        self.xml_dsd = None
        
        self._dataflows = None
        self._categoryschemes = None
        self._categorisations = None
        self._concepts = None
        self._codelists = OrderedDict()
        
        self.requests_client = requests.Session()
                
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

        self.xml_sdmx = XMLSDMX(agencyID=self.provider_name)
        
        self.xml_dsd = XMLStructure(provider_name=self.provider_name,
                                    sdmx_client=self.xml_sdmx)       
        
        url = "http://www.bdm.insee.fr/series/sdmx/dataflow/%s" % self.provider_name
        download = Downloader(url=url, 
                              filename="dataflow.xml",
                              store_filepath=self.store_path,
                              headers=SDMX_METADATA_HEADERS,
                              use_existing_file=self.use_existing_file)
        filepath = download.get_filepath()
        self.for_delete.append(filepath)
        self.xml_dsd.process(filepath)
        self._dataflows = self.xml_dsd.dataflows

        url = "http://www.bdm.insee.fr/series/sdmx/categoryscheme/%s" % self.provider_name
        download = Downloader(url=url, 
                              filename="categoryscheme.xml",
                              store_filepath=self.store_path,
                              headers=SDMX_METADATA_HEADERS,
                              use_existing_file=self.use_existing_file)
        filepath = download.get_filepath()
        self.for_delete.append(filepath)
        self.xml_dsd.process(filepath)
        self._categoryschemes = self.xml_dsd.categories

        url = "http://www.bdm.insee.fr/series/sdmx/categorisation/%s" % self.provider_name
        download = Downloader(url=url, 
                              filename="categorisation.xml",
                              store_filepath=self.store_path,
                              headers=SDMX_METADATA_HEADERS,
                              use_existing_file=self.use_existing_file)
        filepath = download.get_filepath()
        self.for_delete.append(filepath)
        self.xml_dsd.process(filepath)
        self._categorisations = self.xml_dsd.categorisations
        
        url = "http://www.bdm.insee.fr/series/sdmx/conceptscheme/%s" % self.provider_name
        download = Downloader(url=url, 
                              filename="conceptscheme.xml",
                              store_filepath=self.store_path,
                              headers=SDMX_METADATA_HEADERS,
                              use_existing_file=self.use_existing_file)
        filepath = download.get_filepath()
        self.for_delete.append(filepath)
        self.xml_dsd.process(filepath)
        self._concepts = self.xml_dsd.concepts
        
    def load_datasets_first(self):
        self._load_structure()
        return super().load_datasets_first()

    def build_data_tree(self):
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

    def upsert_dataset(self, dataset_code):

        self._load_structure()

        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code,
                           name=None,
                           doc_href=None,
                           last_update=clean_datetime(),
                           fetcher=self)
        
        query = {'provider_name': self.provider_name, 
                 "dataset_code": dataset_code}        
        dataset_doc = self.db[constants.COL_DATASETS].find_one(query)
        
        insee_data = INSEE_Data(dataset,
                                dataset_doc=dataset_doc)
        dataset.series.data_iterator = insee_data
        
        return dataset.update_database()

    def get_calendar(self):

        datasets = {d['name']: d['dataset_code'] for d in self.datasets_list()}
        
        DATEEXP = re.compile("(January|February|March|April|May|June|July|August|September|October|November|December)[ ]+\d+[ ]*,[ ]+\d+[ ]+\d+:\d+")
        url = 'http://www.insee.fr/en/service/agendas/agenda.asp'
        
        d = pq(url=url, parser='html')
        
        for li in d('div#contenu')('ul.liens')("li.princ-ind"):
            try:
                
                # April 21, 2016  08:45 - INSEE
                text = pq(li)("p.info")[0].text
                
                _date = datetime.strptime(DATEEXP.match(text).group(),'%B %d, %Y %H:%M')
                
                #/en/themes/indicateur.asp?id=105
                url1 = "http://www.insee.fr%s" % pq(li)("a")[0].get("href")
                page2 = pq(url=url1, parser='html')
                
                # 'http://www.bdm.insee.fr/bdm2/choixCriteres.action?request_locale=en&codeGroupe=1007'
                url2 = page2("div#savoirplus")('p')('a')[0].get("href")
                page3 = pq(url=url2, parser='html')
                
                #telechargeSDMX-ML?lien=CLIMAT-AFFAIRES&groupeLibc=CLIMAT-AFFAIRES
                dataset_code = page3("a#exportSDMX")[0].get("href").split("=")[-1]
                
                #print("dataset_code : ", dataset_code)
                
                if dataset_code in datasets:

                    yield {'action': "update-dataset",
                           "kwargs": {"provider_name": self.provider_name,
                                      "dataset_code": dataset_code},
                           "period_type": "date",
                           "period_kwargs": {"run_date": datetime(_date.year,
                                                                  _date.month,
                                                                  _date.day,
                                                                  _date.hour,
                                                                  _date.minute+2,
                                                                  0),
                                             "timezone": 'Europe/Paris'}
                         }
                
            except Exception as err:
                logger.exception(err)

class INSEE_Data(SeriesIterator):
    
    def __init__(self, dataset, dataset_doc=None):
        """
        :param Datasets dataset: Datasets instance
        """
        super().__init__(dataset)

        self.dataset_doc = dataset_doc
        self.store_path = self.get_store_path()
        
        #TODO: prendre cette info dans la DSD sans utiliser dataflows
        self.dataset.name = self.fetcher._dataflows[self.dataset_code]["name"]        
        self.dsd_id = self.fetcher._dataflows[self.dataset_code]["dsd_id"]
        
        if self.dataset_doc and self.dataset_doc["enable"]:
            #self.last_update = self.dataset_doc["last_update"]
            self.last_update = self.dataset_doc["download_last"]
        else:
            self.last_update = self.dataset.download_last #self.dataset.last_update

        self.xml_dsd = XMLStructure(provider_name=self.provider_name,
                                    sdmx_client=self.fetcher.xml_sdmx)        
        self.xml_dsd.concepts = self.fetcher._concepts
        self.xml_dsd.codelists = self.fetcher._codelists

        self._load_dsd()
        
        self.xml_data = XMLData(provider_name=self.provider_name,
                                dataset_code=self.dataset_code,
                                xml_dsd=self.xml_dsd,
                                dsd_id=self.dsd_id,
                                frequencies_supported=FREQUENCIES_SUPPORTED)
        
        self.rows = self._get_data_by_dimension()

    def _load_dsd_by_element(self):
        
        #FIXME: Manque codelist et concepts ?
        
        url = "http://www.bdm.insee.fr/series/sdmx/datastructure/INSEE/%s" % self.dsd_id
        download = Downloader(url=url, 
                              filename="datastructure-%s.xml" % self.dsd_id,
                              headers=SDMX_METADATA_HEADERS,
                              store_filepath=self.store_path,
                              use_existing_file=self.fetcher.use_existing_file)
        
        filepath = download.get_filepath()
        self.fetcher.for_delete.append(filepath)
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
                              store_filepath=self.store_path,
                              use_existing_file=self.fetcher.use_existing_file)
        
        filepath, response = download.get_filepath_and_response()
        
        if response:
            if response.status_code == HTTP_ERROR_LONG_RESPONSE:
                self._load_dsd_by_element()
                return
            elif response.status_code >= 400:
                raise response.raise_for_status()

        if not os.path.exists(filepath):
            self._load_dsd_by_element()
            return
        
        self.fetcher.for_delete.append(filepath)
        self.xml_dsd.process(filepath)
        self._set_dataset()
        
    def _set_dataset(self):

        dataset = dataset_converter(self.xml_dsd, self.dataset_code, dsd_id=self.dsd_id)
        self.dataset.dimension_keys = dataset["dimension_keys"] 
        self.dataset.attribute_keys = dataset["attribute_keys"] 
        self.dataset.concepts = dataset["concepts"] 
        self.dataset.codelists = dataset["codelists"]

    def _get_dimensions_from_dsd(self):
        return get_dimensions_from_dsd(self.xml_dsd, self.provider_name, self.dataset_code)
        
    def _get_data_by_dimension(self):
        
        dimension_keys, dimensions = self._get_dimensions_from_dsd()
        
        choice = "avg" 
        if self.dataset_code in ["IPC-2015-COICOP"]:
            choice = "max"        
        
        position, _key, dimension_values = select_dimension(dimension_keys, 
                                                            dimensions, 
                                                            choice=choice)
        
        count_dimensions = len(dimension_keys)
        
        logger.info("choice[%s] - filterkey[%s] - count[%s] - provider[%s] - dataset[%s]" % (choice, _key, len(dimension_values), self.provider_name, self.dataset_code))
        
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
            filename = "data-%s-%s.xml" % (self.dataset_code, key.replace(".", "_"))
            download = Downloader(url=url, 
                                  filename=filename,
                                  store_filepath=self.store_path,
                                  #client=self.fetcher.requests_client
                                  )
            filepath, response = download.get_filepath_and_response()

            if filepath:
                self.fetcher.for_delete.append(filepath)

            if response.status_code == HTTP_ERROR_NO_RESULT:
                continue
            elif response.status_code >= 400:
                raise response.raise_for_status()
            
            for row, err in self.xml_data.process(filepath):
                yield row, err

            #self.dataset.update_database(save_only=True)
        
        yield None, None
    
    def _is_updated(self, bson):
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

    def clean_field(self, bson):
        bson["attributes"].pop("IDBANK", None)
        bson = super().clean_field(bson)
        return bson

    def build_series(self, bson):
        self.dataset.add_frequency(bson["frequency"])
        
        if not self._is_updated(bson):
            raise errors.RejectUpdatedSeries(provider_name=self.provider_name,
                                             dataset_code=self.dataset_code,
                                             key=bson.get('key'))
            
        series_updated = bson.get('last_update', None)
        if series_updated and series_updated > self.dataset.last_update:
            self.dataset.last_update = series_updated
            
        return bson
