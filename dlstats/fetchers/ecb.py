# -*- coding: utf-8 -*-

import os
from datetime import datetime
import logging
import re

import lxml.html
import requests

from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats import utils
from dlstats.utils import Downloader
from dlstats.xml_utils import (XMLStructure_2_1 as XMLStructure, 
                               XMLSpecificData_2_1_ECB as XMLData,
                               dataset_converter,
                               select_dimension,
                               get_key_for_dimension,
                               get_dimensions_from_dsd)

HTTP_ERROR_NOT_MODIFIED = 304
HTTP_ERROR_LONG_RESPONSE = 413
HTTP_ERROR_NO_RESULT = 404
HTTP_ERROR_BAD_REQUEST = 400
HTTP_ERROR_SERVER_ERROR = 500

VERSION = 5

logger = logging.getLogger(__name__)

SDMX_DATA_HEADERS = {'Accept': 'application/vnd.sdmx.structurespecificdata+xml;version=2.1'}
SDMX_METADATA_HEADERS = {'Accept': 'application/vnd.sdmx.structure+xml;version=2.1'}

#TODO: intégré à metadata du provider ou du dataset ?
FREQUENCIES_SUPPORTED = ["A", "M", "Q", "W", "D", "S"]
FREQUENCIES_REJECTED = ["E", "B", "H", "N"] 

"""
https://sdw-wsrest.ecb.europa.eu/service/codelist/ECB/CL_FREQ
https://sdw-wsrest.ecb.europa.eu/service/dataflow/ECB/EXR?references=all
https://sdw-wsrest.ecb.europa.eu/service/data/EXR/.ARS...
A: Annual (2015)
B: Business (Pas d'exemple)
D: Daily (2000-01-13)
E: Event (not supported)
H: Half-yearly (2000-S2)
M: Monthly (2000-02)
N: Minutely (Pas d'exemple)
Q: Quarterly (2000-Q2)
S: Half Yearly, semester (value H exists but change to S in 2009, move from H to this new value to be agreed in ESCB context) (Pas d'exemple)
W: Weekly (Pas d'exemple)

frequencies_supported = [
    "A", #Annual
    "D", #Daily
    "M", #Monthly
    "Q", #Quarterly
    "W"  #Weekly
]
frequencies_rejected = [
    "E", #Event
    "B", #Business
    "H", #Half-yearly
    "N", #Minutely
    "S", #Half Yearly, semester 
]
"""

class ECB(Fetcher):
    
    def __init__(self, **kwargs):        
        super().__init__(provider_name='ECB', version=VERSION, **kwargs)

        self.provider = Providers(name=self.provider_name,
                                  long_name='European Central Bank',
                                  version=VERSION,
                                  region='Europe',
                                  website='http://www.ecb.europa.eu',
                                  terms_of_use='https://www.ecb.europa.eu/home/disclaimer/html/index.en.html',
                                  fetcher=self)
    
        self.xml_sdmx = None
        self.xml_dsd = None
        
        self._dataflows = None
        self._categoryschemes = None
        self._categorisations = None
        self._concepts = None
        
        #self.requests_client = requests.Session()

    def _load_structure(self, force=False):
        """Load structure and build data_tree
        """
        
        if self._dataflows and not force:
            return
        
        self.xml_dsd = XMLStructure(provider_name=self.provider_name)       
        
        url = "http://sdw-wsrest.ecb.int/service/dataflow/%s" % self.provider_name
        download = utils.Downloader(store_filepath=self.store_path,
                                    url=url, 
                                    filename="dataflow.xml",
                                    headers=SDMX_METADATA_HEADERS,
                                    use_existing_file=self.use_existing_file)
        filepath = download.get_filepath()
        self.for_delete.append(filepath)
        self.xml_dsd.process(filepath)
        self._dataflows = self.xml_dsd.dataflows

        url = "http://sdw-wsrest.ecb.int/service/categoryscheme/%s" % self.provider_name
        download = utils.Downloader(store_filepath=self.store_path,
                                    url=url, 
                                    filename="categoryscheme.xml",
                                    headers=SDMX_METADATA_HEADERS,
                                    use_existing_file=self.use_existing_file)
        filepath = download.get_filepath()
        self.for_delete.append(filepath)
        self.xml_dsd.process(filepath)
        self._categoryschemes = self.xml_dsd.categories

        url = "http://sdw-wsrest.ecb.int/service/categorisation/%s" % self.provider_name
        download = utils.Downloader(store_filepath=self.store_path,
                                    url=url, 
                                    filename="categorisation.xml",
                                    headers=SDMX_METADATA_HEADERS,
                                    use_existing_file=self.use_existing_file)
        filepath = download.get_filepath()
        self.for_delete.append(filepath)
        self.xml_dsd.process(filepath)
        self._categorisations = self.xml_dsd.categorisations
        
        url = "http://sdw-wsrest.ecb.int/service/conceptscheme/%s" % self.provider_name
        download = utils.Downloader(store_filepath=self.store_path,
                                    url=url, 
                                    filename="conceptscheme.xml",
                                    headers=SDMX_METADATA_HEADERS,
                                    use_existing_file=self.use_existing_file)
        filepath = download.get_filepath()
        self.for_delete.append(filepath)
        
        self.xml_dsd.process(filepath)
        self._concepts = self.xml_dsd.concepts
        
    def build_data_tree(self):

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
            
            if len(cat["datasets"]) > 0:
                categories.append(cat)
            
        return categories
        
    def _parse_agenda(self):
        download = utils.Downloader(store_filepath=self.store_path,
                              url="http://www.ecb.europa.eu/press/calendars/statscal/html/index.en.html",
                              filename="statscall.html")
        filepath = download.get_filepath()
        with open(filepath, 'rb') as fp:
            agenda = lxml.html.parse(fp)
        self.for_delete.append(filepath)
        
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

        for entry in self._parse_agenda():

            if entry['dataflow_key'] in datasets:
                
                scheduled_date = entry.pop("scheduled_date")
                run_date = datetime.strptime(scheduled_date, "%d/%m/%Y %H:%M CET")

                yield {'action': 'update-dataset',
                       'kwargs': {'provider_name': self.provider_name,
                                  'dataset_code': entry['dataflow_key']},
                       'period_type': 'date',
                       'period_kwargs': {'run_date': run_date,
                                         'timezone': 'CET'}
                      }

    def upsert_dataset(self, dataset_code):
        
        self._load_structure()
        
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code,
                           name=None,
                           doc_href=self.provider.website,
                           fetcher=self)
        dataset.last_update = utils.clean_datetime()
        
        _data = ECB_Data(dataset=dataset)
        dataset.series.data_iterator = _data
        return dataset.update_database()

    def load_datasets_first(self):
        self._load_structure()
        return super().load_datasets_first()

class ECB_Data(SeriesIterator):
    
    def __init__(self, dataset):
        """
        :param Datasets dataset: Datasets instance
        """
        super().__init__(dataset)
        self.store_path = self.get_store_path()
        self.last_modified = None        
                
        self.dataset.name = self.fetcher._dataflows[self.dataset_code]["name"]        
        self.dsd_id = self.fetcher._dataflows[self.dataset_code]["dsd_id"]
        self.agency_id = self.fetcher._dataflows[self.dataset_code]["attrs"].get("agencyID")

        self.xml_dsd = XMLStructure(provider_name=self.provider_name)        
        #self.xml_dsd.concepts = self.fetcher._concepts
        
        self._load()
        
        self.rows = self._get_data_by_dimension()
        
    def _load(self):

        url = "http://sdw-wsrest.ecb.int/service/datastructure/%s/%s?references=all" % (self.agency_id, self.dsd_id)
        download = utils.Downloader(store_filepath=self.store_path,
                                    url=url, 
                                    filename="dsd-%s.xml" % self.dataset_code,
                                    headers=SDMX_METADATA_HEADERS,
                                    use_existing_file=self.fetcher.use_existing_file)
        filepath = download.get_filepath()
        self.fetcher.for_delete.append(filepath)
        self.xml_dsd.process(filepath)
        self._set_dataset()
        
    def _get_dimensions_from_dsd(self):
        return get_dimensions_from_dsd(self.xml_dsd, self.provider_name, self.dataset_code)
    
    def _get_data_by_dimension(self):
        
        self.xml_data = XMLData(provider_name=self.provider_name,
                                dataset_code=self.dataset_code,
                                xml_dsd=self.xml_dsd,
                                dsd_id=self.dsd_id,
                                frequencies_supported=FREQUENCIES_SUPPORTED)
        
        dimension_keys, dimensions = self._get_dimensions_from_dsd()
        
        position, _key, dimension_values = select_dimension(dimension_keys, dimensions)
        
        count_dimensions = len(dimension_keys)
        
        for dimension_value in dimension_values:
            
            key = get_key_for_dimension(count_dimensions, position, dimension_value)

            #http://sdw-wsrest.ecb.int/service/data/IEAQ/A............
            url = "http://sdw-wsrest.ecb.int/service/data/%s/%s" % (self.dataset_code, key)
            if not self._is_good_url(url, good_codes=[200, HTTP_ERROR_NOT_MODIFIED]):
                print("bypass url[%s]" % url)
                continue
            
            headers = SDMX_DATA_HEADERS
            
            filename = "data-%s-%s.xml" % (self.dataset_code, key.replace(".", "_"))               
            download = Downloader(url=url, 
                                  filename=filename,
                                  store_filepath=self.store_path,
                                  headers=headers,
                                  use_existing_file=self.fetcher.use_existing_file,
                                  #client=self.fetcher.requests_client
                                  )
            filepath, response = download.get_filepath_and_response()

            if filepath and os.path.exists(filepath):
                self.fetcher.for_delete.append(filepath)
            elif not filepath or not os.path.exists(filepath):
                continue

            if response:
                self._add_url_cache(url, response.status_code)
            elif response and response.status_code == HTTP_ERROR_NO_RESULT:
                continue
            elif response and response.status_code >= 400:
                raise response.raise_for_status()
    
            for row, err in self.xml_data.process(filepath):
                yield row, err

        yield None, None
                        
    def _set_dataset(self):
        dataset = dataset_converter(self.xml_dsd, self.dataset_code)
        self.dataset.dimension_keys = dataset["dimension_keys"] 
        self.dataset.attribute_keys = dataset["attribute_keys"] 
        self.dataset.concepts = dataset["concepts"] 
        self.dataset.codelists = dataset["codelists"]
        
    def clean_field(self, bson):
        bson["attributes"].pop("TITLE", None)
        bson["attributes"].pop("TITLE_COMPL", None)
        bson = super().clean_field(bson)
        return bson

    def build_series(self, bson):
        self.dataset.add_frequency(bson["frequency"])
        bson["last_update"] = self.dataset.last_update
        
        return bson
        

