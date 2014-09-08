#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#TODO Refactor the code so that every method looks like create_series_db. This is *the right way*.
"""
.. module:: eurostat
    :platform: Unix, Windows
    :synopsis: Populate a MongoDB database with data from Eurostat

.. :moduleauthor :: Widukind team <widukind-dev@cepremap.org>
"""

#from dlstats.fetchers.skeleton import Skeleton
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
                lastUpdate = None
                lastModified = None
                code = None
                children = None
                for element in branch.iterchildren():
                    if element.tag[-5:] == 'title':
                        if element.attrib.values()[0] == 'en':
                            title = element.text
                    elif element.tag[-4:] == 'code':
                        code = element.text
                    elif element.tag[-10:] == 'lastUpdate':
                        if not (element.text is None):
                            lastUpdate = datetime.datetime.strptime(element.text,'%d.%m.%Y')
                    elif element.tag[-8:] == 'children':
                        children = walktree(element)
                if not ((lastUpdate is None) | (lastModified is None)):
                    lastUpdate = max(lastUpdate,lastModified)
                document = self._Category(name=title,children=children,category_code=code)
                _id = document.store(self.db.categories)
                children_ids += [_id]
            return children_ids

        branch = self.table_of_contents.find('{urn:eu.europa.ec.eurostat.navtree}branch')
        _id = walktree(branch.find('{urn:eu.europa.ec.eurostat.navtree}children'))
        document = self._Category(name='Eurostat',children=[_id])
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
        dsd = sdmx.eurostat.data_definition(d)
        cat = self.db.categories.find_one({'code': d})
        document = self._Dataset(dataset_code=d,
                                 dimension_list=dsd.codes,
# To be fixed !!!!
                                 attributes_list=None,
                                 lastUpdate=cat['lastUpdate'])
        id = document.store(self.db.datasets)
# To be tested
        self.update_a_series(dataset_code,id,document.lastUpdate)    

    def update_a_series(self,dataset_code,dataset_id,lastUpdate):
        key = '....'
        series__ = sdmx.eurostat.data_extraction(dataset_code,key)
        for series_ in [series__.time_series[key]
                        for key
                        in series__.time_series.keys()]:
            series_key = (dataset_code+'.'+'.'.join(series_[0].values())).upper()
            values = series_[1].values.tolist()
            release_dates = [lastUpdate for i in range(len(values))]
            codes_ = series_[0]
            codes = [{'name': name, 'value': value} 
                     for name, value in codes_.items()]
            document = self._Series(key= series_key,
                                datasetCode= dataset_code,
                                startDate=series_[1].index[0], 
                                endDate=series_[1].index[-1], 
                                values=values,
# to be fixed !!!!
                                attributes=None,
                                releaseDates=release_dates,
                                frequency=series_[0]['FREQ'], 
                                categoriesId=dataset_id,
                                dimensions=codes)
            document.store(self.db.series)


    def update_eurostat(self):
        categories = self.create_categories_db()
#        print(self.get_selected_datasets())
#        for d in self.get_selected_datasets():
#            self.update_selected_dataset(d)



