#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#TODO Refactor the code so that every method looks like create_series_db. This is *the right way*.
"""
.. module:: eurostat
    :platform: Unix, Windows
    :synopsis: Populate a MongoDB database with data from Eurostat

.. :moduleauthor :: Widukind team <widukind-dev@cepremap.org>
"""

#from dlstats.fetchers._skeleton import Skeleton
from _skeleton import Skeleton
import threading
import collections
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
        self.db = self.client.eurostat
#        webpage = urllib.request.urlopen(
#            self.configuration['Fetchers']['Eurostat']['url_table_of_contents'],
#            timeout=7)
#        table_of_contents = webpage.read()
#        self.table_of_contents = lxml.etree.fromstring(table_of_contents)
        parser = lxml.etree.XMLParser(recover=True) 
        self.table_of_contents = lxml.etree.parse("http://localhost:8800/eurostat/table_of_contents.xml", parser)
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
                document = self._Category(name=title,doc_href=doc_href,children=children,category_code=code,last_update=last_update)
                _id = document.store(self.db.categories)
                children_ids += [_id]
            return children_ids

        branch = self.table_of_contents.find('{urn:eu.europa.ec.eurostat.navtree}branch')
        _id = walktree(branch.find('{urn:eu.europa.ec.eurostat.navtree}children'))
        document = self._Category(name='Eurostat',children=[_id],last_update=None)
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
            cc = self.db.categories.find_one({'code': code})
            datasets += walktree1(cc['_id'])
        return datasets

    def update_selected_dataset(self,dataset_code):
        """Updates data in Database for selected datasets
        :dset: dataset code
        :returns: None"""
        dsd = sdmx.eurostat_test.codes(dataset_code)
        cat = self.db.categories.find_one({'categoryCode': dataset_code})
        document = self._Dataset(dataset_code=dataset_code,
                                 codes_list=dsd,
                                 name = cat['name'],
                                 doc_href = cat['docHref'],
                                 last_update=cat['lastUpdate'])
        id = document.store(self.db.datasets)
        self.update_a_series(dataset_code,id,document.bson['lastUpdate'],dsd)    

    def parse_date(self,str):
        m = re.match(re.compile(r"(\d+)-([DWMQH])(\d+)|(\d+)"),str)
        if m.groups()[3]:
            return (m.groups()[3],None,'A')
        else:
            return (m.groups()[0],m.groups()[2],m.groups()[1])

    def update_a_series(self,dataset_code,code_list,lastUpdate,codes_list):
        (raw_values, raw_dates, raw_attributes, raw_codes) = sdmx.eurostat_test.raw_data(dataset_code,'....')
        for key in raw_values:
            series_key = (dataset_code+'.'+ key).upper()
            # Eurostat lists data in reversed chronological order
            values = raw_values[key][::-1]
            (start_year, start_subperiod,freq) = self.parse_date(raw_dates[key][-1])
            (end_year,end_subperiod,freq) = self.parse_date(raw_dates[key][0])
            for a in raw_attributes[key]:
                raw_attributes[key][a] = raw_attributes[key][a][::-1]
            release_dates = [lastUpdate for v in values]
            codes_ = raw_codes[key]
            codes = [{name: value} 
                     for name, value in codes_.items()]
            name = "-".join([codes_list[name][value] for c in codes for name,value in c.items()])
            print(name)
            document = self._Series(key= series_key,
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
    e.update_selected_dataset('ei_bsco_q')
