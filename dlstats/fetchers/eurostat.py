#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
.. module:: eurostat
    :platform: Unix, Windows
    :synopsis: Populate a MongoDB database with data from Eurostat

.. :moduleauthor :: Widukind team <widukind-dev@cepremap.org>
"""

from dlstats.fetchers._skeleton import (Skeleton, Category, Series,
                                        Dataset, Provider, CodeDict)
from dlstats.fetchers.make_elastic_index import ElasticIndex

import threading
from collections import OrderedDict, defaultdict
import lxml.etree
import urllib
from pandas.tseries.offsets import *
import pandas
import datetime
from io import BytesIO, StringIO, TextIOWrapper
import urllib.request
import gzip
import re
import pymongo
import logging
from multiprocessing import Pool
import sdmx
import datetime
import time
import math
import requests
import zipfile
import pprint


class Eurostat(Skeleton):
    """Class for managing the SDMX endpoint from eurostat in dlstats."""
    def __init__(self):
        super().__init__(provider_name='eurostat')
        self.lgr = logging.getLogger('Eurostat')
        self.lgr.setLevel(logging.INFO)
        self.fh = logging.FileHandler('Eurostat.log')
        self.fh.setLevel(logging.INFO)
        self.frmt = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.fh.setFormatter(self.frmt)
        self.lgr.addHandler(self.fh)
        #        self.lgr.info('Retrieving %s',
        #                      self.configuration['Fetchers']['Eurostat']['url_table_of_contents'])
        #        webpage = urllib.request.urlopen(
        #            self.configuration['Fetchers']['Eurostat']['url_table_of_contents'],
        #            timeout=7)
        #        table_of_contents = webpage.read()
        #        self.table_of_contents = lxml.etree.fromstring(table_of_contents)
        self.provider_name = 'Eurostat'
        self.provider = Provider(name=self.provider_name,website='http://ec.europa.eu/eurostat')
        self.selected_codes = ['ei_bcs_cs']

    def upsert_provider_db(self):
        self.provider.update_database();

    def update_categories_db(self):
        """Update the categories in MongoDB

        If a category doesn't exit, it is created
        """
        def walktree(child):
            """Recursive function for parsing table_of_contents.xml

            :param branch: The current branch explored. The function
            is going top to bottom.
            :type branch: ElementTree
            :param parent_id: The id of the previous branch
            :type parent_id: MongoObject(Id)
            """
            children_ids = []
            for branch in child.iterchildren():
                title = None
                docHref = None
                lastUpdate = None
                lastModified = None
                code = None
                children = None
                for element in branch.iterchildren():
                    if element.tag[-5:] == 'title':
                        if element.attrib.values()[0] == 'en':
                            title = element.text
                    elif element.tag[-5:] == 'metadata':
                        if element.attrib.values()[0] == 'html':
                            docHref = element.text
                    elif element.tag[-4:] == 'code':
                        code = element.text
                    elif element.tag[-10:] == 'lastUpdate':
                        if not (element.text is None):
                            lastUpdate = datetime.datetime.strptime(
                                element.text,'%d.%m.%Y')
                    elif element.tag[-12:] == 'lastModified':
                        if not (element.text is None):
                            lastModified = datetime.datetime.strptime(
                                element.text,'%d.%m.%Y')
                    elif element.tag[-8:] == 'children':
                        children = walktree(element)
                if not ((lastUpdate is None) | (lastModified is None)):
                    lastUpdate = max(lastUpdate,lastModified)
                if lastUpdate is not None and not isinstance(lastUpdate,
                                                             datetime.datetime):
                    lastUpdate = datetime.datetime(lastUpdate)
                if docHref is not None:
                    document = Category(provider=self.provider_name,name=title,
                                        docHref=doc_href,children=children,
                                        categoryCode=code,lastUpdate=lastUpdate)
                    self.lgr.debug('Instantiating Category: %s',code) 
                else:
                    document = Category(provider=self.provider_name,name=title,
                                        children=children,categoryCode=code,
                                        lastUpdate=lastUpdate)
                    self.lgr.debug('Instantiating Category: %s',code) 
                _id = document.update_database()
                children_ids += [_id]
            return children_ids

        branch = self.table_of_contents.find('{urn:eu.europa.ec.eurostat.navtree}branch')
        _id = walktree(branch.find('{urn:eu.europa.ec.eurostat.navtree}children'))
        document = Category(provider=self.provider_name,name='root',children=_id,
                            lastUpdate=None,categoryCode='eurostat_root')
        document.update_database()


    def get_selected_datasets(self):
        """Collects the dataset codes that are in table of contents,
        below the ones indicated in "selected_codes" provided in configuration
        :returns: list of codes"""
        def walktree1(id):
            datasets1 = []
            c = self.db.categories.find_one({'_id': id})
            if 'children' in c:
                for child in  c['children']:
                    datasets1 += walktree1(child)
                return datasets1
            else:
                return [c['categoryCode']]
        datasets = []
        for code in self.selected_codes:
            cc = self.db.categories.find_one({'provider': self.provider_name,
                                              'categoryCode': code})
            datasets += walktree1(cc['_id'])
        return datasets

    def update_eurostat(self):
        return self.update_categories_db()

    def update_selected_dataset(self,datasetCode,testing_mode=False):
        """Updates data in Database for selected datasets
        :dset: datasetCode
        :returns: None"""
        dataset = Dataset(self.provider_name,datasetCode)
        if testing_mode:
            dataset.testing_mode = True
        cat = self.db.categories.find_one({'categoryCode': datasetCode})
        dataset.name = cat['name']
        dataset.doc_href = cat['docHref']
        dataset.last_update = cat['lastUpdate']
        data = EurostatData(self.provider_name,datasetCode,dataset.last_update)
        dataset.dimension_list = data.dimension_list
        dataset.attribute_list = data.attribute_list
        dataset.series.data_iterator = data
        dataset.update_database()
        es = ElasticIndex()
        es.make_index(self.provider_name,datasetCode)

class EurostatData:
    def __init__(self,provider_name,datasetCode,lastUpdate):
        self.provider_name = provider_name
        self.datasetCode = datasetCode
        self.lastUpdate = lastUpdate
        request = requests.get(self.make_url(datasetCode))
        buffer = BytesIO(request.content)
        files = zipfile.ZipFile(buffer)
        dsd_file = files.read(datasetCode + ".dsd.xml")
        data_file = files.read(datasetCode + ".sdmx.xml")
        [attributes,dimensions] = self.parse_dsd(dsd_file,datasetCode)
        self.attribute_list = CodeDict(attributes)
        self.dimension_list = CodeDict(dimensions)

        parser = lxml.etree.XMLParser(ns_clean=True, recover=True,
                                      encoding='utf-8') 
        tree = lxml.etree.fromstring(data_file, parser)
        # Anonymous namespace is not supported by lxml
        nsmap = {}
        for t in tree.nsmap:
            if t != None:
                nsmap[t] = tree.nsmap[t]
        self.series_iterator = tree.iterfind(".//data:Series",
                                             namespaces=nsmap)

    def __iter__(self):
        return self

    def __next__(self):
        return(self.one_series())
        
    def one_series(self):
        attributes = defaultdict(list)
        values = []
        raw_dates = []
        series = next(self.series_iterator)
        # drop TIME FORMAT that isn't informative
        dimensions = OrderedDict({key.lower(): value for key,value in series.attrib.items() if key != 'TIME_FORMAT'})
        nobs = 1
        for observation in series.iterchildren():
            for k in attributes:
                attributes[k] += [""]
            attrib = observation.attrib
            for a in attrib:
                if a == "TIME_PERIOD":
                    raw_dates.append(attrib[a])
                elif a == "OBS_VALUE":
                    values.append(attrib[a])
                else:
                    if not a in attributes.keys():
                        attributes[a] = ["" for i in range(nobs)]
                    attributes[a][-1] = attrib[a]
            nobs += 1
        bson = {}
        bson['provider'] = self.provider_name
        bson['datasetCode'] = self.datasetCode
        bson['name'] =  "-".join([self.dimension_list.get_dict()[n][v]
                             for n,v in dimensions.items()])
        bson['key'] = ".".join(dimensions.values())
        bson['values'] = values
        bson['attributes'] = attributes
        bson['dimensions'] = dimensions
        bson['releaseDates'] = [self.lastUpdate for v in values]
        (start_year, start_subperiod,freq) = self.parse_date(
            raw_dates[0])
        (end_year,end_subperiod,freq) = self.parse_date(
            raw_dates[-1])
        if freq == "A":
            bson['startDate'] = pandas.Period(start_year,freq='annual').ordinal
            bson['endDate'] = pandas.Period(end_year,freq='annual').ordinal
        else:
            bson['startDate'] = pandas.Period(start_year+freq+start_subperiod,freq=freq).ordinal
            bson['endDate'] = pandas.Period(end_year+freq+end_subperiod,freq=freq).ordinal
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
        code_desc = {}
        for code_lists in tree.iterfind("{*}CodeLists",
                                       namespaces=nsmap):
            for code_list in code_lists.iterfind(
                ".//structure:CodeList", namespaces=nsmap):
                name = code_list.get('id')
                # truncate intial "CL_" in name
                name = name[3:]
                # a dot "." can't be part of a JSON field name
                name = re.sub(r"\.","",name)
                dimension = {}
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
        attributes = {key.lower(): code_desc[key] for key in al}
        dimensions = {key.lower(): code_desc[key] for key in dl}
        return (attributes,dimensions)
    
    def parse_sdmx(self,file,dataset_code):
        parser = lxml.etree.XMLParser(ns_clean=True, recover=True,
                                      encoding='utf-8') 
        tree = lxml.etree.fromstring(file, parser)
        # Anonymous namespace is not supported by lxml
        nsmap = {}
        for t in tree.nsmap:
            if t != None:
                nsmap[t] = tree.nsmap[t]

        raw_dimensions = {}
        raw_dates = {}
        raw_values = {}
        raw_attributes = {}
        DATA = '{'+tree.nsmap['data']+'}'

        for series in tree.iterfind(".//data:Series",
                                    namespaces=nsmap):

            attributes = defaultdict(list)
            values = []
            dimensions = []

            dimensions_ = OrderedDict(series.attrib)
            nobs = 1
            for observation in series.iterchildren():
                for k in attributes:
                    attributes[k] += [""]
                attrib = observation.attrib
                for a in attrib:
                    if a == "TIME_PERIOD":
                        dimensions.append(attrib[a])
                    elif a == "OBS_VALUE":
                        values.append(attrib[a])
                    else:
                        if not a in attributes.keys():
                            attributes[a] = ["" for i in range(nobs)]
                        attributes[a][-1] = attrib[a]
                nobs += 1
            key = ".".join(dimensions_.values())
            raw_dimensions[key] = dimensions_
            raw_dates[key] = dimensions
            raw_values[key] = values
            raw_attributes[key] = attributes
        return (raw_values, raw_dates, raw_attributes, raw_dimensions)

    def parse_date(self,str):
        m = re.match(re.compile(r"(\d+)-([DWMQH])(\d+)|(\d+)"),str)
        if m.groups()[3]:
            return (m.groups()[3],None,'A')
        else:
            return (m.groups()[0],m.groups()[2],m.groups()[1])

    def make_url(self,datasetCode):
        return("http://ec.europa.eu/eurostat/" +
               "estat-navtree-portlet-prod/" +
               "BulkDownloadListing?sort=1&file=data/" +
               datasetCode + ".sdmx.zip")
    
if __name__ == "__main__":
    e = Eurostat()
    l = logging.getLogger('_skeleton')
    l.setLevel(logging.INFO)
    #    e.upsert_provider_db()
    #    e.update_categories_db()
    #    e.update_selected_dataset('nama_gdp_c')
    e.update_selected_dataset('nama_gdp_k')
