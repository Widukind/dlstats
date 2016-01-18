# -*- coding: utf-8 -*-
"""
.. module:: eurostat
    :platform: Unix, Windows
    :synopsis: Populate a MongoDB database with data from Eurostat

.. :moduleauthor :: Widukind team <widukind-dev@cepremap.org>
"""

from collections import OrderedDict, defaultdict
from datetime import datetime
import re
import logging
import zipfile
import os
import tempfile
import time

import pandas
import bson
from lxml import etree

from dlstats import constants
from dlstats.utils import Downloader
from dlstats.fetchers._commons import Fetcher, Datasets, Providers

REGEX_DATE_P3M = re.compile(r"(\d+)-Q(\d)")
REGEX_DATE_P1D = re.compile(r"(\d\d\d\d)(\d\d)(\d\d)")

TABLE_OF_CONTENT_NSMAP = {'nt': 'urn:eu.europa.ec.eurostat.navtree',
                          'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}

DSD_NSMAP = {'common': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/common',
             'compact': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/compact',
             'cross': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/cross',
             'generic': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic',
             'query': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/query',
             'structure': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure',
             'utility': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/utility',
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
    
def get_nsmap(xml_iterator):
    """Extract namespaces from XML document 
    parsed with etree.iterparse
    
    Ex:     
    xml_iterator = etree.iterparse(filepath, events=['end','start-ns'])
    nsmap = get_nsmap(xml_iterator)
    """
    nsmap = {}
    for event, element in xml_iterator:
        if event == 'start-ns':
            ns, url = element
            if len(ns) > 0:
                nsmap[ns] = url
        else:
            break
    return nsmap

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
        filepaths.update({filename: zfile.extract(filename, os.path.dirname(zipfilepath))})
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
            self.provider.update_database()
        
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
        
    def build_data_tree(self, force_update=False):
        """Builds the data tree
        
        Pour créer les categories, ne prend que les branch dont l'un des <code> 
        de la branch se trouvent dans selected_codes
        
        Même chose pour les datasets. Prend le category_code du parent
        et verifie si il est dans selected_codes
        """
        
        start = time.time()
        logger.info("build_data_tree provider[%s] - START" % self.provider_name)
        
        if self.provider.count_data_tree() > 1 and not force_update:
            logger.info("use existing data-tree for provider[%s]" % self.provider_name)
            return self.provider.data_tree

        filepath = self.get_table_of_contents()

        it = etree.iterparse(filepath, events=['end'])

        def is_selected(parent_codes):
            """parent_codes is array of category_code
            """
            for _select in self.selected_codes: 
                if _select in parent_codes:
                    return True
            return False

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
                            _parent_categories = ".".join(_parent_codes[:_parent_codes.index(_parent_code)])
                            _category = None
                            _parent = None

                            if not _parent_categories or len(_parent_categories) == 0:
                                _category = {"category_code": _parent_code, "name": _parent_title}
                            else:
                                _parent = self.provider._category_key(_parent_categories)
                                _category = {"category_code": _parent_code, "name": _parent_title}

                            try:
                                _key = self.provider.add_category(_category, _parent)
                            except:
                                #Pas de capture car verifie seulement si existe
                                pass

                        datasets = xpath_datasets(child)

                        for dataset in datasets:
                            parent_codes = xpath_parent_codes(dataset)
                            dataset_code = xpath_code(dataset)[0]
                            category_code = self.provider._category_key(".".join(parent_codes))

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

                            dataset = {
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
                            self.provider.add_dataset(dataset, category_code)
                            dataset.clear()
                        child.clear()
                    element.clear()

        end = time.time() - start
        logger.info("build_data_tree load provider[%s] - END - time[%.3f seconds]" % (self.provider_name, end))

        return self.provider.data_tree
    
    def get_table_of_contents(self):
        return Downloader(url=self.url_table_of_contents, 
                              filename="table_of_contents.xml").get_filepath()

    def get_selected_datasets(self):
        """Collects the dataset codes that are in table of contents
        below the ones indicated in "selected_codes" provided in configuration
        :returns: list of dict of dataset settings"""
        category_filter = [".*%s.*" % d for d in self.selected_codes]
        category_filter = "|".join(category_filter)
        self.selected_datasets = {d['dataset_code']: d for d in self.datasets_list(category_filter=category_filter)}
        return self.selected_datasets

    def upsert_dataset(self, dataset_code):
        """Updates data in Database for selected datasets
        :dset: dataset_code
        :returns: None"""
        self.get_selected_datasets()

        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))

        dataset_settings = self.selected_datasets[dataset_code]

        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code, 
                           name=dataset_settings["name"], 
                           doc_href=dataset_settings["metadata"].get("doc_href"), 
                           last_update=dataset_settings["last_update"], 
                           fetcher=self)

        data_iterator = EurostatData(dataset, filename=dataset_code)
        dataset.series.data_iterator = data_iterator
        dataset.update_database()

        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))

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

class EurostatData:
    
    def __init__(self,dataset, filename=None, store_filepath=None):
        self.provider_name = dataset.provider_name
        self.dataset_code = dataset.dataset_code
        self.last_update = dataset.last_update
        self.attribute_list = dataset.attribute_list
        self.dimension_list = dataset.dimension_list
        self.filename = filename
        self.store_filepath = store_filepath
        self.dataset_url = self.make_url()
        self.sdmx_tree_iterator = None
        # parse DSD, prepare and set sdmx_tree_iterator
        self.process_data()

    def __next__(self):
        for event, element in self.sdmx_tree_iterator:
            if event == 'end':
                if element.tag == self.fixtag('data','Series'):
                    bson = self.one_series(element)
                    element.clear()
                    return bson
        raise StopIteration()        

    def get_store_path(self):
        return self.store_filepath or os.path.abspath(os.path.join(
            tempfile.gettempdir(), 
            self.provider_name, 
            self.dataset_code))

    def _load_datas(self):

        store_filepath = self.get_store_path()
        download = Downloader(url=self.dataset_url, 
                              filename=self.filename, 
                              store_filepath=store_filepath)

        '''Return 2 filepath (dsd and data)'''    
        return (extract_zip_file(download.get_filepath()))

    def process_data(self):
        filepaths = self._load_datas()

        # parse dsd
        with open(filepaths[self.dataset_code + ".dsd.xml"],"rb") as f:
            dsd_file = f.read()

        [attributes,dimensions] = self.parse_dsd(dsd_file,self.dataset_code)
        self.attribute_list.set_dict(attributes)
        self.dimension_list.set_dict(dimensions)

        # prepare sdmx iterator for datas
        sdmx_filename = filepaths[self.dataset_code + ".sdmx.xml"]
        self.sdmx_tree_iterator = etree.iterparse(sdmx_filename, 
                                                  events=['end','start-ns'])
        self.nsmap = get_nsmap(self.sdmx_tree_iterator)

    def fixtag(self, ns, tag):
        return '{' + self.nsmap[ns] + '}' + tag

    def one_series(self,series):
        attributes = defaultdict(list)
        values = []
        raw_dates = []
        # drop TIME FORMAT that isn't informative

        dimensions = OrderedDict([(key.lower(), value) for key,value in series.attrib.items() if key != 'TIME_FORMAT'])
        time_format = series.attrib['TIME_FORMAT']

        nobs = 1
        for observation in series.iterchildren():
            for k in attributes:
                attributes[k] += [""]
            attrib = observation.attrib
            value_field = False
            for a in attrib:
                if a == "TIME_PERIOD":
                    raw_dates.append(attrib[a])
                elif a == "OBS_VALUE":
                    values.append(attrib[a])
                    value_field = True
                else:
                    if not a in attributes.keys():
                        attributes[a] = ["" for i in range(nobs)]
                    attributes[a][-1] = attrib[a]

            # OBS_VALUE may be missing in the attributes
            # this indicates a missing value
            if not value_field:
                values.append('')
            nobs += 1

        # force attributes' key to be lower case
        attributes = {k.lower() : attributes[k] for k in attributes} 

        bson = {}
        bson['provider_name'] = self.provider_name
        bson['dataset_code'] = self.dataset_code
        bson['name'] =  "-".join([self.dimension_list.get_dict()[n][v]
                             for n,v in dimensions.items()])
        bson['key'] = ".".join(dimensions.values())
        bson['values'] = values
        bson['last_update'] = self.last_update
        bson['attributes'] = attributes
        bson['dimensions'] = dimensions
        (start_string, freq) = self.parse_date(raw_dates[0], time_format)        
        (end_string, freq) = self.parse_date(raw_dates[-1], time_format)
        bson['start_date'] = pandas.Period(start_string, freq=freq).ordinal
        bson['end_date'] = pandas.Period(end_string, freq=freq).ordinal
        bson['frequency'] = freq

        return(bson)
    
    def parse_dsd(self, file, dataset_code):
        parser = etree.XMLParser(ns_clean=True, recover=True,
                                      encoding='utf-8') 
        tree = etree.fromstring(file, parser)

        nsmap = DSD_NSMAP
        code_desc = OrderedDict()

        for code_lists in tree.iterfind("{*}CodeLists", namespaces=nsmap):

            for code_list in code_lists.iterfind(
                ".//structure:CodeList", namespaces=nsmap):
                name = code_list.get('id')
                # truncate intial "CL_" in name
                name = name[3:]
                # a dot "." can't be part of a JSON field name
                name = re.sub(r"\.","",name)
                dimension = OrderedDict()
                
                for code in code_list.iterfind(".//structure:Code",
                                                     namespaces=nsmap):
                    key = code.get("value")
                    for desc in code:
                        if desc.attrib.items()[0][1] == "en":
                            dimension[key] = desc.text
                code_desc[name] = dimension

        # Splitting codeList in dimensions and attributes
        for concept_list in tree.iterfind(".//structure:Components",
                                          namespaces=nsmap):

            dl = [d.get("codelist")[3:] for d in concept_list.iterfind(
                ".//structure:Dimension",namespaces=nsmap)]
            al = [d.get("codelist")[3:] for d in concept_list.iterfind(
                ".//structure:Attribute",namespaces=nsmap)]

        # force key name tp lowercase
        attributes = OrderedDict([(key.lower(), code_desc[key]) for key in al])
        dimensions = OrderedDict([(key.lower(), code_desc[key]) for key in dl])

        return (attributes, dimensions)
    
    def parse_date(self,_str,fmt):
        if (fmt == 'P1Y'):
            return (_str,'A')
        elif (fmt == 'P3M'):
            m = re.match(REGEX_DATE_P3M,_str)
            return (m.groups()[0]+'Q'+m.groups()[1],'Q')
        elif (fmt == 'P1M'):
            return (_str,'M')
        elif (fmt == 'P1D'):
            m = re.match(REGEX_DATE_P1D,_str)
            return ('-'.join(m.groups()),'D')
        else:
            msg = 'eurostat, '+self.datase_code+', TIME FORMAT not recognized'
            logger.critical(msg)
            raise Exception(msg)

    def make_url(self):
        return("http://ec.europa.eu/eurostat/" +
               "estat-navtree-portlet-prod/" +
               "BulkDownloadListing?sort=1&file=data/" +
               self.dataset_code + ".sdmx.zip")
    
