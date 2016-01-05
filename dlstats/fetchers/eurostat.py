#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
.. module:: eurostat
    :platform: Unix, Windows
    :synopsis: Populate a MongoDB database with data from Eurostat

.. :moduleauthor :: Widukind team <widukind-dev@cepremap.org>
"""

from collections import OrderedDict, defaultdict
import lxml.etree
#from pandas.tseries.offsets import *
import pandas
import datetime
from io import BytesIO, StringIO, TextIOWrapper
import requests
import re
import logging
import zipfile
import bson
import os
import tempfile
import time
import pymongo

from dlstats import constants
from dlstats.fetchers._commons import Fetcher, Datasets, Providers

REGEX_DATE_P3M = re.compile(r"(\d+)-Q(\d)")
REGEX_DATE_P1D = re.compile(r"(\d\d\d\d)(\d\d)(\d\d)")

__all__ = ['Eurostat']

logger = logging.getLogger(__name__)

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

class Downloader():
    
    headers = {
        'user-agent': 'dlstats - https://github.com/Widukind/dlstats'
    }
    
    def __init__(self, url=None, filename=None, store_filepath=None, 
                 timeout=None, max_retries=0, replace=True):
        self.url = url
        self.filename = filename
        self.store_filepath = store_filepath
        self.timeout = timeout
        self.max_retries = max_retries
        
        if not self.store_filepath:
            self.store_filepath = tempfile.mkdtemp()
        else:
            if not os.path.exists(self.store_filepath):
                os.makedirs(self.store_filepath, exist_ok=True)
        self.filepath = os.path.abspath(os.path.join(self.store_filepath, self.filename))
        
        #TODO: force_replace ?
        
        if os.path.exists(self.filepath) and not replace:
            raise Exception("filepath is already exist : %s" % self.filepath)
        
    def _download(self):
        
        #TODO: timeout
        #TODO: max_retries (self.max_retries)
        #TODO: analyse rate limit dans headers
        
        start = time.time()
        try:
            #TODO: Session ?
            response = requests.get(self.url, 
                                    timeout=self.timeout, 
                                    stream=True, 
                                    allow_redirects=True,
                                    verify=False, #ssl
                                    headers=self.headers)

            if not response.ok:
                msg = "download url[%s] - status_code[%s] - reason[%s]" % (self.url, 
                                                                           response.status_code, 
                                                                           response.reason)
                logger.error(msg)
                raise Exception(msg)
            
            with open(self.filepath,'wb') as f:
                for chunk in response.iter_content():
                    f.write(chunk)
                    #TODO: flush ?            
                
            #TODO: response.close() ?
            
        except requests.exceptions.ConnectionError as err:
            raise Exception("Connection Error")
        except requests.exceptions.ConnectTimeout as err:
            raise Exception("Connect Timeout")
        except requests.exceptions.ReadTimeout as err:
            raise Exception("Read Timeout")
        except Exception as err:
            raise Exception("Not captured exception : %s" % str(err))            

        end = time.time() - start
        logger.info("download file[%s] - END - time[%.3f seconds]" % (self.url, end))
    
    def get_filepath(self, force_replace=False):
        
        if os.path.exists(self.filepath) and force_replace:
            os.remove(self.filepath)
        
        if not os.path.exists(self.filepath):
            logger.info("not found file[%s] - download dataset url[%s]" % (self.filepath, self.url))
            self._download()
        else:
            logger.info("use local dataset file [%s]" % self.filepath)
        
        return self.filepath

class Eurostat(Fetcher):
    """Class for managing the SDMX endpoint from eurostat in dlstats."""
    def __init__(self, db=None):
        super().__init__(provider_name='Eurostat', 
                         db=db)
        self.provider_name = 'Eurostat'
        self.provider = Providers(name=self.provider_name,
                                  long_name='Eurostat',
                                  region='Europe',
                                  website='http://ec.europa.eu/eurostat',
                                  fetcher=self)
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
        
    def build_data_tree(self):
        """Builds the data tree 
        """
        def walktree(child,ns):
            """Recursive function for parsing table_of_contents.xml

            :param branch: The current branch explored. The function
            is going top to bottom.
            :type branch: ElementTree
            :param parent_id: The id of the previous branch
            :type parent_id: MongoObject(Id)
            """
            children = []
            for branch in child.iterchildren():
                _tree = {}
                last_update = None
                last_modified = None
                _tree['provider_name'] = self.provider_name
                _tree['doc_href'] = None
                _tree['children'] = []
                for element in branch.iterchildren():
                    if element.tag == '{' + ns['nt'] + '}' + 'title':
                        if element.attrib.values()[0] == 'en':
                            _tree['name'] = element.text
                    elif element.tag == '{' + ns['nt'] + '}' + 'metadata':
                        if element.attrib.values()[0] == 'html':
                            _tree['doc_href'] = element.text
                    elif element.tag == '{' + ns['nt'] + '}' + 'code':
                        _tree['category_code'] = element.text
                    elif element.tag == '{' + ns['nt'] + '}' + 'last_update':
                        if not (element.text is None):
                            last_update = datetime.datetime.strptime(
                                element.text,'%d.%m.%Y')
                    elif element.tag == '{' + ns['nt'] + '}' + 'lastModified':
                        if not (element.text is None):
                            last_modified = datetime.datetime.strptime(
                                element.text,'%d.%m.%Y')
                    elif element.tag == '{' + ns['nt'] + '}' + 'children':
                        _tree['children'] = walktree(element,ns)
                if not ((last_update is None) | (last_modified is None)):
                    last_update = max(last_update,last_modified)
                if last_update is not None and not isinstance(last_update,
                                                             datetime.datetime):
                    _tree['last_update'] = datetime.datetime(last_update)
                else:
                    _tree['last_update'] = last_update
                _tree['exposed'] = False
                children.append(_tree)
            return children

        table_of_contents = lxml.etree.fromstring(self.get_table_of_contents())
        ns = table_of_contents.nsmap
        branch = table_of_contents.find('nt:branch',namespaces=ns)
        children = walktree(branch.find('nt:children',namespaces=ns),ns)
        data_tree = {'provider_name': self.provider_name,
                     'name': 'Eurostat',
                     'doc_href': None,
                     'children': children,
                     'category_code': 'eurostat_root',
                     'exposed': False,
                     'last_update': None}
        return data_tree
    
    def get_table_of_contents(self):
        webpage = requests.get(self.url_table_of_contents,
                               timeout=7)
        return webpage.content

    def upsert_categories(self):
        data_tree = self.build_data_tree()

        self.provider.add_data_tree(data_tree)
        
        
    def get_selected_datasets(self):
        """Collects the dataset codes that are in table of contents
        below the ones indicated in "selected_codes" provided in configuration
        :returns: list of codes"""

        def walktree1(node,selected):

            if selected or (node['category_code'] in self.selected_codes):
                selected = True
                if len(node['children']) == 0:
                    # this is a leaf
                    node['exposed'] = True
                    self.selected_datasets.update({node['category_code']: node})

            for child in node['children']:
                walktree1(child,selected)

        provider = self.db[constants.COL_PROVIDERS].find_one({'name': self.provider_name},{'data_tree': 1})
        if 'data_tree' not in provider:
            self.upsert_categories()
            provider = self.db[constants.COL_PROVIDERS].find_one({'name': self.provider_name},{'data_tree': 1})
            
        walktree1(provider['data_tree'],False)
        
    def upsert_selected_datasets(self):
        if not self.selected_datasets:
            self.get_selected_datasets()
        for d in self.selected_datasets:
            self.upsert_dataset(d)

    def datasets_list(self):
        if not self.selected_datasets:
            self.get_selected_datasets()
        return [d for d in self.selected_datasets]

    def datasets_long_list(self):
        if not self.selected_datasets:
            self.get_selected_datasets()
        return [(d[0],d[1]['name']) for d in self.selected_datasets.items()]

    def upsert_dataset(self,dataset_code):
        """Updates data in Database for selected datasets
        :dset: dataset_code
        :returns: None"""
        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        if not self.selected_datasets:
            self.get_selected_datasets()
        cat = self.selected_datasets[dataset_code]
        dataset = Datasets(self.provider_name,
                           dataset_code,
                           last_update=cat['last_update'],
                           fetcher=self)
        dataset.name = cat['name']
        dataset.doc_href = cat['doc_href']
        data_iterator = EurostatData(dataset,filename = dataset_code)
        dataset.series.data_iterator = data_iterator
        dataset.update_database()
        self.update_metas(dataset_code)
        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))

    def upsert_all_datasets(self):
        start = time.time()
        logger.info("update fetcher[%s] - START" % (self.provider_name))
        self.upsert_categories();
        self.get_selected_datasets()
        selected_datasets = self.db[constants.COL_DATASETS].find(
            {'provider_name': self.provider_name, 'dataset_code': {'$in': list(self.selected_datasets.keys())}},
            {'dataset_code': 1, 'last_update': 1})
        selected_datasets = {s['dataset_code'] : s for s in selected_datasets}
        for d in self.selected_datasets:
            if (d not in selected_datasets) or (selected_datasets[d]['last_update'] < self.selected_datasets[d]['last_update']):
                self.upsert_dataset(d)
        end = time.time() - start
        logger.info("update fetcher[%s] - END - time[%.3f seconds]" % (self.provider_name, end))

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

    def __iter__(self):
        return self

    def __next__(self):
        for event, element in self.sdmx_tree_iterator:
            if event == 'end':
                if element.tag == self.fixtag('data','Series'):
                    bson = self.one_series(element)
                    element.clear()
                    return bson
        # sdmx_tree_iterator is exhaused        
        raise StopIteration()        
        
    def get_store_path(self):
        return self.store_filepath or os.path.abspath(os.path.join(
            tempfile.gettempdir(), 
            self.provider_name, 
            self.dataset_code))
    
    def _load_datas(self):
        
        store_filepath = self.get_store_path()
        # TODO: timeout, replace
        download = Downloader(url=self.dataset_url, filename=self.filename, store_filepath=store_filepath)
            
        return(extract_zip_file(download.get_filepath(force_replace=True)))

    def process_data(self):
        filepaths = self._load_datas()
        # parse dsd
        with open(filepaths[self.dataset_code + ".dsd.xml"],"rb") as f:
            dsd_file = f.read()
        [attributes,dimensions] = self.parse_dsd(dsd_file,self.dataset_code)
        self.attribute_list.set_dict(attributes)
        self.dimension_list.set_dict(dimensions)
        
        # prepare sdmx iterator
        sdmx_filename = filepaths[self.dataset_code + ".sdmx.xml"]
        self.sdmx_tree_iterator = lxml.etree.iterparse(sdmx_filename, events=['end','start-ns'])
        self.nsmap = {}
        # store document name space
        for event, element in self.sdmx_tree_iterator:
            if event == 'start-ns':
                ns, url = element
                if len(ns) > 0:
                    self.nsmap[ns] = url
            else:
                break

    def fixtag(self,ns,tag):
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
        (start_string, freq) = self.parse_date(
            raw_dates[0], time_format)
        (end_string, freq) = self.parse_date(
            raw_dates[-1], time_format)
        bson['start_date'] = pandas.Period(start_string,freq=freq).ordinal
        bson['end_date'] = pandas.Period(end_string,freq=freq).ordinal
        bson['frequency'] = freq
        return(bson)
    
    def parse_dsd(self,file,dataset_code):
        parser = lxml.etree.XMLParser(ns_clean=True, recover=True,
                                      encoding='utf-8') 
        tree = lxml.etree.fromstring(file, parser)
        # Anonymous namespace is not supported by lxml
        nsmap = {}
        for t in tree.nsmap:
            if t != None:
                nsmap[t] = tree.nsmap[t]
        code_desc = OrderedDict()
        for code_lists in tree.iterfind("{*}CodeLists",
                                       namespaces=nsmap):
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
        return (attributes,dimensions)
    
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
    
