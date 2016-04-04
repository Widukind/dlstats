# -*- coding: utf-8 -*-
"""
.. module:: eurostat
    :platform: Unix, Windows
    :synopsis: Populate a MongoDB database with data from Eurostat

.. :moduleauthor :: Widukind team <widukind-dev@cepremap.org>
"""

from collections import OrderedDict
from datetime import datetime
import logging
import zipfile
import os

from lxml import etree

from dlstats import errors
from dlstats import constants
from dlstats.utils import Downloader
from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats.xml_utils import (XMLStructure_2_0 as XMLStructure, 
                               XMLCompactData_2_0_EUROSTAT as XMLData,
                               dataset_converter)

TABLE_OF_CONTENT_NSMAP = {'nt': 'urn:eu.europa.ec.eurostat.navtree',
                          'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}

xpath_title = etree.XPath("nt:title[@language='en']/text()", namespaces=TABLE_OF_CONTENT_NSMAP)
xpath_code = etree.XPath("nt:code/text()", namespaces=TABLE_OF_CONTENT_NSMAP)
xpath_ds_last_update = etree.XPath("nt:lastUpdate/text()", namespaces=TABLE_OF_CONTENT_NSMAP)
xpath_ds_last_modified = etree.XPath("nt:lastModified/text()", namespaces=TABLE_OF_CONTENT_NSMAP)
xpath_ds_metadata_html = etree.XPath("nt:metadata[@format='html']/text()", namespaces=TABLE_OF_CONTENT_NSMAP)

xpath_ds_data_start = etree.XPath("nt:dataStart/text()", namespaces=TABLE_OF_CONTENT_NSMAP)
xpath_ds_data_end = etree.XPath("nt:dataEnd/text()", namespaces=TABLE_OF_CONTENT_NSMAP)
xpath_ds_values = etree.XPath("nt:values/text()", namespaces=TABLE_OF_CONTENT_NSMAP)

VERSION = 3

logger = logging.getLogger(__name__)

def fixtag_toc(ns, tag, nsmap=TABLE_OF_CONTENT_NSMAP):
    return '{' + nsmap[ns] + '}' + tag

def first_element_xpath(values, default=None):
    """Return first element of array or default value
    """
    if values and isinstance(values, list) and len(values) > 0:
        return values[0]
    
    return default
    
def extract_zip_file(zipfilepath):
    """Extract first file in zip file and return absolute path for the file extracted
    
    :param str filepath: Absolute file path of zip file
    
    Example: 
        file1.zip contains one file: file1.csv
    
    >>> extract_zip_file('/tmp/file1.zip')
    '/tmp/file1.csv'
        
    """
    zfile = zipfile.ZipFile(zipfilepath)
    filepaths = {}
    for filename in zfile.namelist():
        filepaths.update({filename: zfile.extract(filename, 
                                                  os.path.dirname(zipfilepath))})
    return filepaths

def make_url(dataset_code):
    return("http://ec.europa.eu/eurostat/" +
           "estat-navtree-portlet-prod/" +
           "BulkDownloadListing?sort=1&file=data/" +
           dataset_code + ".sdmx.zip")

class Eurostat(Fetcher):
    """Class for managing the SDMX endpoint from eurostat in dlstats."""
    
    def __init__(self, **kwargs):
        super().__init__(provider_name='EUROSTAT', version=VERSION, **kwargs)
        
        self.provider = Providers(name=self.provider_name,
                                  long_name='Eurostat',
                                  version=VERSION,
                                  region='Europe',
                                  website='http://ec.europa.eu/eurostat',
                                  fetcher=self)
        
        self.categories_filter = [
            'nama_10', 
            'namq_10', 
            'nasa_10', 
            'nasq_10', 
            'naid_10',
            'nama', 
            'namq', 
            'nasa', 
            'nasq', 
            'gov', 
            'ert', 
            'irt', 
            'prc', 
            'bop', 
            'bop_6',
            'demo_pjanbroad', 
            'lfsi_act_q',
            'euroind',
            'pop',
            'labour',
        ]

        self.url_table_of_contents = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=table_of_contents.xml"
        self.dataset_url = None
        
        self._concepts = OrderedDict()
        self._codelists = OrderedDict()        

    def build_data_tree(self):
        """Builds the data tree
        """
        
        download = Downloader(url=self.url_table_of_contents, 
                              filename="table_of_contents.xml",
                              store_filepath=self.store_path,
                              use_existing_file=self.use_existing_file)
        filepath = download.get_filepath()
        
        categories = []
        categories_keys = []
        
        it = etree.iterparse(filepath, events=['end'], tag="{urn:eu.europa.ec.eurostat.navtree}leaf")

        def is_selected(parent_codes):
            """parent_codes is array of category_code
            """
            for _select in self.categories_filter: 
                if _select in parent_codes:
                    return True
            return False

        def get_category(category_code):
            for c in categories:
                if c["category_code"] == category_code:
                    return c

        #TODO: date TOC à stocker dans provider !!!
        
        def create_categories(parent_codes, parent_titles, position):
            
            position += 1
            
            for i in range(len(parent_codes)):
                category_code = parent_codes.pop()                
                name = parent_titles.pop()                
                all_parents = parent_codes.copy()
                parent = None
                if all_parents:
                    parent = all_parents[-1]
                if not category_code in categories_keys:
                    _category = {
                        "provider_name": self.provider_name,
                        "category_code": category_code,
                        "name": name,
                        "position": position + i,
                        "parent": parent,
                        'all_parents': all_parents,
                        "datasets": [],
                        "doc_href": None,
                        "metadata": None
                    }
                    categories_keys.append(category_code)
                    categories.append(_category)
        
        #    .getroottree().creationDate="20160225T1102"

        position = 0
        
        for event, dataset in it:
            
            parent_codes = dataset.xpath("ancestor::nt:branch/nt:code/text()", namespaces=TABLE_OF_CONTENT_NSMAP)
            
            if not is_selected(parent_codes):
                continue
            
            parent_titles = dataset.xpath("ancestor::nt:branch/nt:title[attribute::language='en']/text()", namespaces=TABLE_OF_CONTENT_NSMAP)
            category_code = parent_codes[-1]

            create_categories(parent_codes, parent_titles, position)
            
            category = get_category(category_code)

            name = xpath_title(dataset)[0]
            last_update = xpath_ds_last_update(dataset)
            last_modified = xpath_ds_last_modified(dataset)
            doc_href = xpath_ds_metadata_html(dataset)
            data_start = xpath_ds_data_start(dataset)
            data_end = xpath_ds_data_end(dataset)
            values = xpath_ds_values(dataset)

            last_update = datetime.strptime(last_update[0], '%d.%m.%Y')
            if last_modified:
                last_modified = datetime.strptime(last_modified[0], '%d.%m.%Y')
                last_update = max(last_update, last_modified)

            dataset_code = xpath_code(dataset)[0]
            _dataset = {
                "dataset_code": dataset_code, 
                "name": name,
                "last_update": last_update,
                "metadata": {
                    "doc_href": first_element_xpath(doc_href),
                    "data_start": first_element_xpath(data_start),
                    "data_end": first_element_xpath(data_end),
                    "values": int(first_element_xpath(values, default="0")),
                }
            }             
            category["datasets"].append(_dataset)

        self.for_delete.append(filepath)
        
        return categories
        
    def upsert_dataset(self, dataset_code):
        """Updates data in Database for selected datasets
        """
        self.get_selected_datasets()

        doc = self.db[constants.COL_DATASETS].find_one(
            {'provider_name': self.provider_name, 'dataset_code': dataset_code},
            {'dataset_code': 1, 'last_update': 1})

        dataset_settings = self.selected_datasets[dataset_code]
        
        if doc and  doc['last_update'] >= dataset_settings['last_update']:
            comments = "update-date[%s]" % doc['last_update']
            raise errors.RejectUpdatedDataset(provider_name=self.provider_name,
                                              dataset_code=dataset_code,
                                              comments=comments)            

        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code, 
                           name=dataset_settings["name"], 
                           doc_href=dataset_settings["metadata"].get("doc_href"), 
                           last_update=dataset_settings["last_update"], 
                           fetcher=self)

        dataset.series.data_iterator = EurostatData(dataset)
        
        return dataset.update_database()
    
    def load_datasets_update(self):

        datasets_list = self.datasets_list()
        dataset_codes = [d["dataset_code"] for d in self.datasets_list()]
        
        #TODO: enable ?
        cursor = self.db[constants.COL_DATASETS].find(
            {'provider_name': self.provider_name, 
             'dataset_code': {'$in': dataset_codes}},
            {'dataset_code': 1, 'last_update': 1})

        selected_datasets = {s['dataset_code'] : s for s in cursor}

        for dataset in datasets_list:
            dataset_code = dataset["dataset_code"]
            
            if (dataset_code not in selected_datasets) or (selected_datasets[dataset_code]['last_update'] < dataset['last_update']):
                try:
                    self.wrap_upsert_dataset(dataset_code)
                except Exception as err:
                    if isinstance(err, errors.MaxErrors):
                        raise
                    msg = "error for provider[%s] - dataset[%s]: %s"
                    logger.critical(msg % (self.provider_name, 
                                           dataset_code, 
                                           str(err)))


class EurostatData(SeriesIterator):

    def __init__(self, dataset):
        super().__init__(dataset)

        self.dataset_url = make_url(self.dataset_code)
        
        self.xml_dsd = XMLStructure(provider_name=self.provider_name)
        self.xml_dsd.concepts = self.fetcher._concepts        
        self.xml_dsd.codelists = self.fetcher._codelists
        
        self.store_path = self.get_store_path()
        
        self._load()

    def _load(self):
        
        download = Downloader(url=self.dataset_url, 
                              filename="data-%s.zip" % self.dataset_code,
                              store_filepath=self.store_path,
                              use_existing_file=self.fetcher.use_existing_file)
        
        filepaths = (extract_zip_file(download.get_filepath()))
        dsd_fp = filepaths[self.dataset_code + ".dsd.xml"]        
        data_fp = filepaths[self.dataset_code + ".sdmx.xml"]
        
        self.fetcher.for_delete.append(dsd_fp)
        self.fetcher.for_delete.append(data_fp)        
        
        self.xml_dsd.process(dsd_fp)
        self._set_dataset()

        self.xml_data = XMLData(provider_name=self.provider_name,
                                dataset_code=self.dataset_code,
                                xml_dsd=self.xml_dsd,
                                #TODO: frequencies_supported=FREQUENCIES_SUPPORTED
                                )        
        self.rows = self.xml_data.process(data_fp)

    def _set_dataset(self):

        dataset = dataset_converter(self.xml_dsd, self.dataset_code)
        self.dataset.dimension_keys = dataset["dimension_keys"] 
        self.dataset.attribute_keys = dataset["attribute_keys"] 
        self.dataset.concepts = dataset["concepts"] 
        self.dataset.codelists = dataset["codelists"]

    def build_series(self, bson):
        self.dataset.add_frequency(bson["frequency"])
        bson["last_update"] = self.dataset.last_update
        return bson

