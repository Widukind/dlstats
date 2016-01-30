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
import time

from lxml import etree

from dlstats import constants
from dlstats.utils import Downloader, remove_file_and_dir
from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator, Categories
from dlstats.xml_utils import (XMLStructure_2_0 as XMLStructure, 
                               XMLCompactData_2_0_EUROSTAT as XMLData,
                               dataset_converter_v2 as dataset_converter)

TABLE_OF_CONTENT_NSMAP = {'nt': 'urn:eu.europa.ec.eurostat.navtree',
                          'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}

xpath_datasets = etree.XPath("descendant::nt:leaf[@type='dataset']", namespaces=TABLE_OF_CONTENT_NSMAP)
xpath_parent_codes = etree.XPath("ancestor::nt:branch/nt:code/text()", namespaces=TABLE_OF_CONTENT_NSMAP)
xpath_ancestor_branch = etree.XPath("ancestor::nt:branch", namespaces=TABLE_OF_CONTENT_NSMAP)
xpath_title = etree.XPath("nt:title[@language='en']/text()", namespaces=TABLE_OF_CONTENT_NSMAP)
xpath_code = etree.XPath("nt:code/text()", namespaces=TABLE_OF_CONTENT_NSMAP)
xpath_ds_last_update = etree.XPath("nt:lastUpdate/text()", namespaces=TABLE_OF_CONTENT_NSMAP)
xpath_ds_last_modified = etree.XPath("nt:lastModified/text()", namespaces=TABLE_OF_CONTENT_NSMAP)
xpath_ds_metadata_html = etree.XPath("nt:metadata[@format='html']/text()", namespaces=TABLE_OF_CONTENT_NSMAP)

xpath_ds_data_start = etree.XPath("nt:dataStart/text()", namespaces=TABLE_OF_CONTENT_NSMAP)
xpath_ds_data_end = etree.XPath("nt:dataEnd/text()", namespaces=TABLE_OF_CONTENT_NSMAP)
xpath_ds_values = etree.XPath("nt:values/text()", namespaces=TABLE_OF_CONTENT_NSMAP)

__all__ = ['Eurostat']

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

class Eurostat(Fetcher):
    """Class for managing the SDMX endpoint from eurostat in dlstats."""
    
    def __init__(self, db=None):
        super().__init__(provider_name='Eurostat', db=db)
        
        if not self.provider:
            self.provider = Providers(name=self.provider_name,
                                      long_name='Eurostat',
                                      version=VERSION,
                                      region='Europe',
                                      website='http://ec.europa.eu/eurostat',
                                      fetcher=self)
        
        if self.provider.version != VERSION:
            self.provider.update_database()
        
        self.selected_codes = [
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
            'lfsi_act_q'
        ]
        self.selected_datasets = {}
        self.url_table_of_contents = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=table_of_contents.xml"
        self.dataset_url = None
        
        self._concepts = OrderedDict()
        self._codelists = OrderedDict()        
        
    def build_data_tree(self, force_update=False):
        """Builds the data tree
        
        Pour créer les categories, ne prend que les branch dont l'un des <code> 
        de la branch se trouvent dans selected_codes
        
        Même chose pour les datasets. Prend le category_code du parent
        et verifie si il est dans selected_codes
        """
        
        start = time.time()
        logger.info("build_data_tree provider[%s] - START" % self.provider_name)
        
        filepath = self.get_table_of_contents()
        
        categories = []
        categories_keys = []

        it = etree.iterparse(filepath, events=['end'])

        def is_selected(parent_codes):
            """parent_codes is array of category_code
            """
            for _select in self.selected_codes: 
                if _select in parent_codes:
                    return True
            return False

        def get_category(category_code):
            for c in categories:
                if c["category_code"] == category_code:
                    return c

        position = 0
        for event, element in it:
            if event == 'end':

                if element.tag == fixtag_toc('nt', 'branch'):

                    for child in element.iterchildren(tag=fixtag_toc('nt', 'children')):
                        _parent_codes = xpath_parent_codes(child)
                        _parents = xpath_ancestor_branch(child)

                        if not is_selected(_parent_codes):
                            continue

                        for parent in _parents:
                            _parent_code = xpath_code(parent)[0]
                            _parent_title =xpath_title(parent)[0]

                            '''Extrait la partie gauche des categories parents'''
                            _parent_categories = _parent_codes[:_parent_codes.index(_parent_code)]
                            _parent = None

                            _category = {
                                "provider_name": self.provider_name,
                                "category_code": _parent_code,
                                "name": _parent_title,
                                "position": 0,
                                "parent": None,
                                'all_parents': None,
                                "datasets": [],
                                "doc_href": None,
                                "metadata": None
                            }

                            if _parent_categories and len(_parent_categories) >= 1:
                                _category["parent"] = _parent_categories[-1]
                                _category["all_parents"] = _parent_categories
                            else:
                                position += 1
                                _category["position"] = position
                            
                            if not _category["category_code"] in categories_keys:
                                categories_keys.append(_category["category_code"])
                                categories.append(_category)    

                        datasets = xpath_datasets(child)

                        for dataset in datasets:
                            parent_codes = xpath_parent_codes(dataset)
                            dataset_code = xpath_code(dataset)[0]
                            _category = get_category(parent_codes[-1]) 

                            '''Verifie si au moins un des category_code est dans selected_codes'''
                            if not is_selected(parent_codes):
                                continue

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
                            _category["datasets"].append(_dataset)
                            dataset.clear()
                        child.clear()
                    element.clear()

        end = time.time() - start
        logger.info("build_data_tree load provider[%s] - END - time[%.3f seconds]" % (self.provider_name, end))
        
        remove_file_and_dir(filepath)

        return categories
        
    def get_table_of_contents(self):
        return Downloader(url=self.url_table_of_contents, 
                              filename="table_of_contents.xml").get_filepath()

    def get_selected_datasets(self, force=False):
        """Collects the dataset codes that are in table of contents
        below the ones indicated in "selected_codes" provided in configuration
        :returns: list of dict of dataset settings"""
        
        if self.selected_datasets and not force:
            return self.selected_datasets  

        if Categories.count(self.provider_name, db=self.db) == 0:
            self.upsert_data_tree()
        
        query = {
            "$or": [
                 {"category_code": {"$in": self.selected_codes}},
                 {"all_parents": {"$in": self.selected_codes}},
            ],
            "datasets.0": {"$exists": True}
        }
        
        categories = Categories.categories(self.provider_name, db=self.db, **query)
        for category in categories.values():
            for d in category["datasets"]:
                self.selected_datasets[d['dataset_code']] = d
        
        return self.selected_datasets

    def upsert_dataset(self, dataset_code):
        """Updates data in Database for selected datasets
        """
        
        self.get_selected_datasets()

        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))

        doc = self.db[constants.COL_DATASETS].find_one(
            {'provider_name': self.provider_name, 'dataset_code': dataset_code},
            {'dataset_code': 1, 'last_update': 1})

        dataset_settings = self.selected_datasets[dataset_code]
        
        if doc and  doc['last_update'] >= dataset_settings['last_update']:
            end = time.time() - start
            msg = "bypass updated dataset[%s] - last_update[%s] - END - time[%.3f seconds]"
            logger.info(msg % (dataset_code, doc['last_update'], end))
            return

        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code, 
                           name=dataset_settings["name"], 
                           doc_href=dataset_settings["metadata"].get("doc_href"), 
                           last_update=dataset_settings["last_update"], 
                           fetcher=self)

        data_iterator = EurostatData(dataset, 
                                     filename=dataset_code,
                                     fetcher=self)
        dataset.series.data_iterator = data_iterator
        result = dataset.update_database()

        end = time.time() - start
        msg = "upsert dataset[%s] - BULK[%s] - END - time[%.3f seconds]"
        logger.info(msg % (dataset_code, dataset.bulk_size, end))
        
        return result

    def load_datasets_first(self):
        self.get_selected_datasets()

        start = time.time()
        logger.info("first load provider[%s] - START" % (self.provider_name))

        for dataset_code in self.selected_datasets.keys():
            try:
                self.upsert_dataset(dataset_code)
            except Exception as err:
                logger.fatal("error for dataset[%s]: %s" % (dataset_code, str(err)))

        end = time.time() - start
        logger.info("first load provider[%s] - END - time[%.3f seconds]" % (self.provider_name, end))
        
    def load_datasets_update(self):
        self.get_selected_datasets()
        
        start = time.time()
        logger.info("update provider[%s] - START" % (self.provider_name))

        selected_datasets = self.db[constants.COL_DATASETS].find(
            {'provider_name': self.provider_name, 'dataset_code': {'$in': list(self.selected_datasets.keys())}},
            {'dataset_code': 1, 'last_update': 1})

        selected_datasets = {s['dataset_code'] : s for s in selected_datasets}

        for dataset_code, dataset in self.selected_datasets.items():
            if (dataset_code not in selected_datasets) or (selected_datasets[dataset_code]['last_update'] < dataset['last_update']):
                try:
                    self.upsert_dataset(dataset_code)
                except Exception as err:
                    logger.fatal("error for dataset[%s]: %s" % (dataset_code, str(err)))

        end = time.time() - start
        logger.info("update provider[%s] - END - time[%.3f seconds]" % (self.provider_name, end))


class EurostatData(SeriesIterator):

    def __init__(self, dataset=None, filename=None, fetcher=None):
        super().__init__()        
        self.dataset = dataset
        self.filename = filename
        self.fetcher = fetcher

        self.attribute_list = self.dataset.attribute_list
        self.dimension_list = self.dataset.dimension_list
        self.provider_name = self.dataset.provider_name
        self.dataset_code = self.dataset.dataset_code
        self.dataset_url = self.make_url()
        
        self.xml_dsd = XMLStructure(provider_name=self.provider_name)
        self.xml_dsd.concepts = self.fetcher._concepts        
        self.xml_dsd.codelists = self.fetcher._codelists        
        
        self.rows = None

        self._load()

    def _load(self):
        
        download = Downloader(url=self.dataset_url, 
                              filename="data-%s.zip" % self.dataset_code)
        
        filepaths = (extract_zip_file(download.get_filepath()))
        dsd_fp = filepaths[self.dataset_code + ".dsd.xml"]        
        data_fp = filepaths[self.dataset_code + ".sdmx.xml"]
        
        self.dataset.for_delete.append(dsd_fp)
        self.dataset.for_delete.append(data_fp)        
        
        self.xml_dsd.process(dsd_fp)
        self._set_dataset()

        self.xml_data = XMLData(provider_name=self.provider_name,
                                dataset_code=self.dataset_code,
                                dimension_keys=self.xml_dsd.dimension_keys,
                                dimensions=self.xml_dsd.dimensions)
        
        self.rows = self.xml_data.process(data_fp)

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

    def make_url(self):
        return("http://ec.europa.eu/eurostat/" +
               "estat-navtree-portlet-prod/" +
               "BulkDownloadListing?sort=1&file=data/" +
               self.dataset_code + ".sdmx.zip")

    def build_series(self, bson):
        bson["last_update"] = self.dataset.last_update
        return bson

