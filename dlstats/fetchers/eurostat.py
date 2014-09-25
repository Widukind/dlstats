#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#TODO Refactor the code so that every method looks like create_series_db. This is *the right way*.
"""
.. module:: eurostat
    :platform: Unix, Windows
    :synopsis: Populate a MongoDB database with data from Eurostat

.. :moduleauthor :: Widukind team <widukind-dev@cepremap.org>
"""

from dlstats.fetchers._skeleton import Skeleton
#from _skeleton import Skeleton
import threading
from collections import OrderedDict
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


class Eurostat(Skeleton):
    """Class for managing the SDMX endpoint from eurostat in dlstats."""
    def __init__(self):
        super().__init__()
        self.lgr = logging.getLogger('Eurostat')
        self.lgr.setLevel(logging.DEBUG)
        self.fh = logging.FileHandler('Eurostat.log')
        self.fh.setLevel(logging.DEBUG)
        self.frmt = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.fh.setFormatter(self.frmt)
        self.lgr.addHandler(self.fh)
        self.db = self.client.widukind
        webpage = urllib.request.urlopen(
            self.configuration['Fetchers']['Eurostat']['url_table_of_contents'],
            timeout=7)
        table_of_contents = webpage.read()
        self.table_of_contents = lxml.etree.fromstring(table_of_contents)
#        parser = lxml.etree.XMLParser(recover=True) 
#        self.table_of_contents = lxml.etree.parse("http://localhost:8800/eurostat/table_of_contents.xml", parser)
        self.selected_codes = ['ei_bcs_cs']

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
                doc_href = None
                last_update = None
                last_modified = None
                code = None
                children = None
                for element in branch.iterchildren():
                    if element.tag[-5:] == 'title':
                        if element.attrib.values()[0] == 'en':
                            title = element.text
                    elif element.tag[-5:] == 'metadata':
                        if element.attrib.values()[0] == 'html':
                            doc_href = element.text
                    elif element.tag[-4:] == 'code':
                        code = element.text
                    elif element.tag[-10:] == 'lastUpdate':
                        if not (element.text is None):
                            last_update = datetime.datetime.strptime(element.text,'%d.%m.%Y')
                    elif element.tag[-12:] == 'lastModified':
                        if not (element.text is None):
                            last_modified = datetime.datetime.strptime(element.text,'%d.%m.%Y')
                    elif element.tag[-8:] == 'children':
                        children = walktree(element)
                if not ((last_update is None) | (last_modified is None)):
                    last_update = max(last_update,last_modified)
                document = self._Category(provider='eurostat',name=title,doc_href=doc_href,children=children,category_code=code,last_update=last_update)
                _id = document.store(self.db.categories)
                children_ids += [_id]
            return children_ids

        branch = self.table_of_contents.find('{urn:eu.europa.ec.eurostat.navtree}branch')
        _id = walktree(branch.find('{urn:eu.europa.ec.eurostat.navtree}children'))
        document = self._Category(provider='eurostat',name='root',children=[_id],last_update=None)
        document.store(self.db.categories)


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
                return [c['code']]
        datasets = []
        for code in self.selected_codes:
            cc = self.db.categories.find_one({'provider': 'eurostat','categoryCode': code})
            datasets += walktree1(cc['_id'])
        return datasets

    def parse_dsd(self,file,dataset_code):
        parser = lxml.etree.XMLParser(ns_clean=True, recover=True, encoding='utf-8') 
        tree = lxml.etree.fromstring(file, parser)
        # Anonymous namespace is not supported by lxml
        nsmap = {}
        for t in tree.nsmap:
            if t != None:
                nsmap[t] = tree.nsmap[t]
        codes = {}
        for codelist_ in  tree.iterfind("{*}CodeLists",namespaces=nsmap):
            for codelist in codelist_.iterfind(".//structure:CodeList",
                                                namespaces=nsmap):
                name = codelist.get('id')
                # truncate intial "CL_" in name
                name = name[3:]
                # a dot "." can't be part of a JSON field name
                name = re.sub(r"\.","",name)
                code = []
                for code_ in codelist.iterfind(".//structure:Code",
                                               namespaces=nsmap):
                    code_key = code_.get("value")
                    for desc in code_:
                        if desc.attrib.items()[0][1] == "en":
                            code.append([code_key, desc.text])
                codes[name] = code
        return codes

    def parse_sdmx(self,file,dataset_code):
        parser = lxml.etree.XMLParser(ns_clean=True, recover=True, encoding='utf-8') 
        tree = lxml.etree.fromstring(file, parser)
        # Anonymous namespace is not supported by lxml
        nsmap = {}
        for t in tree.nsmap:
            if t != None:
                nsmap[t] = tree.nsmap[t]

        raw_codes = {}
        raw_dates = {}
        raw_values = {}
        raw_attributes = {}
        DATA = '{'+tree.nsmap['data']+'}'

        for series in tree.iterfind(".//data:Series",
                                    namespaces=nsmap):

            attributes = {}
            values = []
            dimensions = []

            codes = OrderedDict(series.attrib)
            for observation in series.iterchildren():
                attrib = observation.attrib
                for a in attrib:
                    if a == "TIME_PERIOD":
                        dimensions.append(attrib[a])
                    elif a == "OBS_VALUE":
                        values.append(attrib[a])
                    else:
                        attributes[a] = attrib[a]
            key = ".".join(codes.values())
            raw_codes[key] = codes
            raw_dates[key] = dimensions
            raw_values[key] = values
            raw_attributes[key] = attributes
        return (raw_values, raw_dates, raw_attributes, raw_codes)



    def update_selected_dataset(self,dataset_code):
        """Updates data in Database for selected datasets
        :dset: dataset code
        :returns: None"""
#        request = requests.get("http://localhost:8800/eurostat/" + dataset_code + ".sdmx.zip")
        request = requests.get("http://epp.eurostat.ec.europa.eu/NavTree_prod/everybody/BulkDownloadListing?sort=1&file=data/" + dataset_code + ".sdmx.zip")
        buffer = BytesIO(request.content)
        files = zipfile.ZipFile(buffer)
        dsd_file = files.read(dataset_code + ".dsd.xml")
        data_file = files.read(dataset_code + ".sdmx.xml")
        dsd = self.parse_dsd(dsd_file,dataset_code)
        cat = self.db.categories.find_one({'categoryCode': dataset_code})
        document = self._Dataset(provider='eurostat',
                                 dataset_code=dataset_code,
                                 codes_list=dsd,
                                 name = cat['name'],
                                 doc_href = cat['docHref'],
                                 last_update=cat['lastUpdate'])
        id = document.store(self.db.datasets)
        self.update_a_series(data_file,dataset_code,id,document.bson['lastUpdate'],dsd)    

    def parse_date(self,str):
        m = re.match(re.compile(r"(\d+)-([DWMQH])(\d+)|(\d+)"),str)
        if m.groups()[3]:
            return (m.groups()[3],None,'A')
        else:
            return (m.groups()[0],m.groups()[2],m.groups()[1])

    def update_a_series(self,data_file,dataset_code,code_list,lastUpdate,codes_list):
        (raw_values, raw_dates, raw_attributes, raw_codes) = self.parse_sdmx(data_file,dataset_code)
        for key in raw_values:
            series_key = (dataset_code+'.'+ key).upper()
            # Eurostat lists data in reversed chronological order
            values = raw_values[key][::-1]
            (start_year, start_subperiod,freq) = self.parse_date(raw_dates[key][0])
            (end_year,end_subperiod,freq) = self.parse_date(raw_dates[key][-1])
            for a in raw_attributes[key]:
                raw_attributes[key][a] = raw_attributes[key][a][::-1]
            release_dates = [lastUpdate for v in values]
            codes_ = raw_codes[key]
            # make all codes uppercase
            codes = {name.upper(): value.upper() 
                     for name, value in codes_.items()}
            name = "-".join([d[1] for name,value in codes.items() for d in codes_list[name] if d[0] == value])
            document = self._Series(provider='eurostat',
                                    key= series_key,
                                    name=name,
                                    dataset_code= dataset_code,
                                    start_date=[start_year,start_subperiod],
                                    end_date=[end_year,end_subperiod],
                                    values=raw_values[key],
                                    attributes=raw_attributes[key],
                                    release_dates=release_dates,
                                    frequency=freq,
                                    dimensions=codes
                                )
            document.store(self.db.series)


    def update_eurostat(self):
        categories = self.create_categories_db()
#        print(self.get_selected_datasets())
#        for d in self.get_selected_datasets():
#            self.update_selected_dataset(d)



if __name__ == "__main__":
    import eurostat
    e = eurostat.Eurostat()
    e.update_selected_dataset('namq_gdp_c')
