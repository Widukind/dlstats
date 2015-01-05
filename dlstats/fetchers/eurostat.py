#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#TODO Refactor the code so that every method looks like create_series_db. This is *the right way*.
"""
.. module:: eurostat
    :platform: Unix, Windows
    :synopsis: Populate a MongoDB database with data from Eurostat

.. :moduleauthor :: Widukind team <widukind-dev@cepremap.org>
"""

from dlstats.fetchers._skeleton import Skeleton, Category, Series, Dataset
#from _skeleton import Skeleton
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
        super().__init__()
        self.lgr = logging.getLogger('Eurostat')
        self.lgr.setLevel(logging.DEBUG)
        self.fh = logging.FileHandler('Eurostat.log')
        self.fh.setLevel(logging.DEBUG)
        self.frmt = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.fh.setFormatter(self.frmt)
        self.lgr.addHandler(self.fh)
        self.lgr.info('Retrieving %s', self.configuration['Fetchers']['Eurostat']['url_table_of_contents'])
        webpage = urllib.request.urlopen(
            self.configuration['Fetchers']['Eurostat']['url_table_of_contents'],
            #            "http://localhost:8800/eurostat/table_of_contents.xml",
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
                            lastUpdate = datetime.datetime.strptime(element.text,'%d.%m.%Y')
                    elif element.tag[-12:] == 'lastModified':
                        if not (element.text is None):
                            lastModified = datetime.datetime.strptime(element.text,'%d.%m.%Y')
                    elif element.tag[-8:] == 'children':
                        children = walktree(element)
                if not ((lastUpdate is None) | (lastModified is None)):
                    lastUpdate = max(lastUpdate,lastModified)
                self.lgr.debug("docHref : %s", doc_href)
                if docHref is not None:
                    document = Category(provider='eurostat',name=title,docHref=doc_href,children=children,categoryCode=code,lastUpdate=lastUpdate)
                else:
                    document = Category(provider='eurostat',name=title,children=children,categoryCode=code,last_update=lastUpdate)
                _id = document.update_dabase()
                children_ids += [_id]
            return children_ids

        branch = self.table_of_contents.find('{urn:eu.europa.ec.eurostat.navtree}branch')
        _id = walktree(branch.find('{urn:eu.europa.ec.eurostat.navtree}children'))
        document = Category(provider='eurostat',name='root',children=[_id],lastUpdate=None)
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
        codes = []
        for dimensions_list_ in  tree.iterfind("{*}CodeLists",namespaces=nsmap):
            for dimensions_list in dimensions_list_.iterfind(".//structure:CodeList",
                                                namespaces=nsmap):
                name = dimensions_list.get('id')
                # truncate intial "CL_" in name
                name = name[3:]
                # a dot "." can't be part of a JSON field name
                name = re.sub(r"\.","",name)
                dimension = []
                for dimension_ in dimensions_list.iterfind(".//structure:Code",
                                               namespaces=nsmap):
                    dimension_key = dimension_.get("value")
                    for desc in dimension_:
                        if desc.attrib.items()[0][1] == "en":
                            dimension.append((dimension_key, desc.text))
                codes.append({'name':name, 'values': dimension})
        self.lgr.debug('Parsed codes %s', codes)
        # Splitting codeList in dimensions and attributes
        for concept_list in tree.iterfind(".//structure:Components",namespaces=nsmap):
            dl = [d.get("codelist")[3:] for d in concept_list.iterfind(".//structure:Dimension",namespaces=nsmap)]
            al = [d.get("codelist")[3:] for d in concept_list.iterfind(".//structure:Attribute",namespaces=nsmap)]
        attributes = [d for d in codes if d['name'] in al]
        dimensions = [d for d in codes if d['name'] in dl]
        return (attributes,dimensions)

    def parse_sdmx(self,file,dataset_code):
        parser = lxml.etree.XMLParser(ns_clean=True, recover=True, encoding='utf-8') 
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



    def update_selected_dataset(self,datasetCode):
        """Updates data in Database for selected datasets
        :dset: datasetCode
        :returns: None"""
#        request = requests.get("http://localhost:8800/eurostat/" + datasetCode + ".sdmx.zip")
        request = requests.get("http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=data/" + datasetCode + ".sdmx.zip")
        buffer = BytesIO(request.content)
        files = zipfile.ZipFile(buffer)
        dsd_file = files.read(datasetCode + ".dsd.xml")
        data_file = files.read(datasetCode + ".sdmx.xml")
        [attributes,dimensions] = self.parse_dsd(dsd_file,datasetCode)
        cat = self.db.categories.find_one({'categoryCode': datasetCode})
        self.lgr.debug("docHref : %s", cat['docHref'])
        self.lgr.debug("attributeList : %s", pprint.pformat(attributes))
        self.lgr.debug("dimensionList : %s", pprint.pformat(dimensions))
        document = Dataset(provider='eurostat',
                                 datasetCode=datasetCode,
                                 attributeList=attributes,
                                 dimensionList=dimensions,
                                 name = cat['name'],
                                 docHref = cat['docHref'],
                                 lastUpdate=cat['lastUpdate'])
        id = document.update_database()
        self.update_a_series(data_file,datasetCode,dimensions,document.bson['lastUpdate'])

    def parse_date(self,str):
        m = re.match(re.compile(r"(\d+)-([DWMQH])(\d+)|(\d+)"),str)
        if m.groups()[3]:
            return (m.groups()[3],None,'A')
        else:
            return (m.groups()[0],m.groups()[2],m.groups()[1])

    def update_a_series(self,data_file,datasetCode,dimensionList,lastUpdate):
        (raw_values, raw_dates, raw_attributes, raw_dimensions) = self.parse_sdmx(data_file,datasetCode)
        for key in raw_values:
            series_key = (datasetCode+'.'+ key).upper()
            # Eurostat lists data in reversed chronological order
            values = raw_values[key][::-1]
            (start_year, start_subperiod,freq) = self.parse_date(raw_dates[key][0])
            (end_year,end_subperiod,freq) = self.parse_date(raw_dates[key][-1])
            period_index = pandas.period_range(start=start_year+freq+start_subperiod,end=end_year+freq+end_subperiod,freq=freq)
            for a in raw_attributes[key]:
                raw_attributes[key][a] = raw_attributes[key][a][::-1]
            releaseDates = [lastUpdate for v in values]
            dimensions_ = raw_dimensions[key]
            # make all codes uppercase
            dimensions = {name.upper(): value.upper() 
                     for name, value in dimensions_.items()}
            dimensions_dict = {d['name']: d['values'] for d in dimensionList}
            name = "-".join([v[1] for name,value in dimensions.items() for d in dimensionList if d['name'] == name for v in d['values'] if v[0] == value])
            document = Series(provider='eurostat',
                                    key= series_key,
                                    name=name,
                                    datasetCode= datasetCode,
                                    period_index=period_index,
                                    values=raw_values[key],
                                    attributes=raw_attributes[key],
                                    releaseDates=releaseDates,
                                    frequency=freq,
                                    dimensions=dimensions
                                )
            document.update_database(key=key)


    def update_eurostat(self):
        categories = self.create_categories_db()
#        print(self.get_selected_datasets())
#        for d in self.get_selected_datasets():
#            self.update_selected_dataset(d)



if __name__ == "__main__":
    import eurostat
    e = eurostat.Eurostat()
    e.update_selected_dataset('namq_gdp_c')
