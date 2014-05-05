#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#TODO Refactor the code so that every method looks like create_series_db. This is *the right way*.
"""
.. module:: eurostat
    :platform: Unix, Windows
    :synopsis: Populate a MongoDB database with data from Eurostat

.. :moduleauthor :: Widukind team <widukind-dev@cepremap.org>
"""

from dlstats.fetchers.skeleton import Skeleton
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
import pysdmx
import datetime
import time
import math


def update_series(leaf):
    """Update the time series documents in MongoDB. This function is defined outside of Eurostat() so that the multiprocessing module can use :mod:`pickle`.
    :param: leaf (): A leaf from http://epp.eurostat.ec.europa.eu/NavTree_prod/everybody/BulkDownloadListing/table_of_contents.xml
    :type: lxml.etree.ElementTree
    :returns: A tuple providing additional info. The first member is True if the insertion succeeded. The second member is the flowRef identifying the DataFlow that was pulled."""
    try:
        id_journal = self.db.journal.insert({'method': 'update_series'})
        client = pymongo.MongoClient()
        db = client.eurostat
        key = '....'
        series_ = pysdmx.eurostat.data_extraction(leaf['flowRef'][0],key)
        for series in [series_.time_series[key]
                       for key
                       in series_.time_series.keys()]:
            name = leaf['name']
            start_date = series[1].index[0]
            end_date = series[1].index[-1]
            values = series[1].values.tolist()
            frequency = series[0]['FREQ']
            codes = series[0]
            categories_id = leaf['_id']
            previous_series = db.series.find({'flowRef':leaf['flowRef'][0],
                                              'name':name},
                                             fields={'values':1})
            series_id = db.series.update({'flowRef':leaf['flowRef'][0],
                                          'name':name},
                                         {'name':name,
                                          'start_date':start_date,
                                          'end_date':end_date,
                                          'values':values,
                                          'frequency':frequency,
                                          'categories_id':categories_id},
                                         {'$push':{'_id_journal':id_journal}},
                                        upsert=True)
            i = 0
            for old_value in previous_series[0]['values']:
                if old_value != values[i]:
                    db.series.update(
                        {'flowRef':leaf['flowRef'][0],'name':name},
                        {'revisions': {'value': old_value,
                                       'position':i}},
                        upsert=True)
                    i += 1

            if previous_series != []:
                for code_name, code_value in codes.items():
                    code_id = db.codes.insert(
                        {'name':code_name,
                         'values':{'value':code_value}},
                        {'$push': {'series_id': series_id}})
                    db.series.update({'_id':series_id},
                                     {'$push':{'codes_id':code_id}},
                                     upsert=True)
        return (True,'flowRef : '+leaf['flowRef'][0])
    except:
        return (False,'flowRef : '+leaf['flowRef'][0])

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
        webpage = urllib.request.urlopen(
            'http://www.epp.eurostat.ec.europa.eu/'
            'NavTree_prod/everybody/BulkDownloadListing'
            '?sort=1&file=table_of_contents.xml', timeout=7)
        table_of_contents = webpage.read()
        self.table_of_contents = lxml.etree.fromstring(table_of_contents)
        self.store_path = '/mnt/data/dlstats/'
    def create_categories_db(self):
        """Create the categories in MongoDB
        """

        id_journal = self.db.journal.insert({'method': 'insert_categories_db'})
        def walktree(branch, parent_id):
            """Recursive function for parsing table_of_contents.xml

            :param branch: The current branch explored. The function
            is going top to bottom.
            :type branch: ElementTree
            :param parent_id: The id of the previous branch
            :type parent_id: MongoObject(Id)
            """
            for children in branch.iterchildren(
                '{urn:eu.europa.ec.eurostat.navtree}children'):
                for branch in children.iterchildren(
                    '{urn:eu.europa.ec.eurostat.navtree}branch'):
                    for title in branch.iterchildren(
                        '{urn:eu.europa.ec.eurostat.navtree}title'):
                        if title.attrib.values()[0] == 'en':
                            node = {'name': title.text,
                                    'id_journal': id_journal}
                            _id = self.db.categories.insert(node)
                            self.db.categories.update(
                                {'_id': parent_id},
                                {'$push': {'children': _id}})
                    walktree(branch, _id)
                for leaf in children.iterchildren(
                    '{urn:eu.europa.ec.eurostat.navtree}leaf'):
                    for title in leaf.iterchildren(
                        '{urn:eu.europa.ec.eurostat.navtree}title'):
                        if title.attrib.values()[0] == 'en':
                            node = {'name': title.text,
                                    'id_journal': id_journal}
                            _id = self.db.categories.insert(node)
                            self.db.categories.update(
                                {'_id': parent_id},
                                {'$push': {'children': _id}})
                    for key in leaf.iterchildren(
                        '{urn:eu.europa.ec.eurostat.navtree}code'):
                            self.db.categories.update(
                                {'_id': _id},
                                {'$push': {'flowRef': key.text}})
                    for download_link in leaf.iterchildren(
                        '{urn:eu.europa.ec.eurostat.navtree}downloadLink'):
                            format =  download_link.attrib.values()[0]
                            _url = download_link.text
                            self.db.categories.update(
                                {'_id': _id},
                                {'$push': {'url_'+format: _url }})

        root = self.table_of_contents.iterfind(
            '{urn:eu.europa.ec.eurostat.navtree}branch')
        for branch in root:
            for title in branch.iterchildren(
                '{urn:eu.europa.ec.eurostat.navtree}title'):
                if title.attrib.values()[0] == 'en':
                    node = {'name': title.text, 'id_journal': id_journal}
                    _id = self.db.categories.insert(node)
            walktree(branch, _id)


    def update_categories_db(self):
        """Update the categories in MongoDB
        """
        id_journal = self.db.journal.insert({'method': 'update_categories_db'})
        self.create_categories_db()
        id_and_names = [name['name']
                        for name 
                        in self.db.categories.find({},
                                                   fields = {'_id':0,'name':1})]
        dupes = [dupe for dupe, number_of_occurences 
                 in collections.Counter(id_and_names).items() 
                 if number_of_occurences > 1]
        for dupe in dupes:
            candidates = self.db.categories.find({'name':dupe}).sort('_id')
            candidates = [candidate for candidate in candidates]
            self.db.categories.remove({'_id':candidates[1]['_id']})

    def create_series_db(self):
        """Create time series documents in MongoDB. 
        :param leaf: A leaf from http://epp.eurostat.ec.europa.eu/NavTree_prod/everybody/BulkDownloadListing/table_of_contents.xml
        :type lxml.etree.ElementTree 
        :returns: None"""
        def create_a_series(lgr,db,leaf):
            try:
                id_journal = db.journal.insert({'method': '_create_series'})
                webpage = urllib.request.urlopen(leaf['url_dft'][0], timeout=7)
                buffer = BytesIO(webpage.read())
                dft_file = gzip.GzipFile(fileobj=buffer, mode="rb")
                dft_file = TextIOWrapper(dft_file, encoding='utf-8', newline='')
                split_dft = [line.strip() for line in dft_file]
                i=0
                lastup_regex = re.compile('LASTUP')
                for line in split_dft:
                    if i == 1:
                        last_update = line.title()
                        last_update = datetime.datetime.strptime(last_update,
                                                                 '%a %d %b %Y %X')
                        break
                    if re.search(lastup_regex, line):
                        i=1
                key = '....'
                series__ = pysdmx.eurostat.data_extraction(leaf['flowRef'][0],
                                                          key)
                series = []
                for series_ in [series__.time_series[key]
                               for key
                               in series__.time_series.keys()]:
                    values = series_[1].values.tolist()
                    release_dates = [last_update for i in range(len(values))]
                    codes_ = series_[0]
                    codes = [{'name': name, 'value': value} 
                             for name, value in codes_.items()]
                    series.append({'name': leaf['name'],
                                   'start_date':series_[1].index[0], 
                                   'end_date':series_[1].index[-1], 
                                   'values':values,
                                   'release_dates':release_dates,
                                   'frequency':series_[0]['FREQ'], 
                                   'categories_id':leaf['_id'],
                                   'id_journal':id_journal,
                                   'codes': codes})
                ids_series = db.series.insert(series)
                lgr.info('Successfully inserted '+leaf['flowRef'][0])
            except:
                lgr.info('Insertion failed '+leaf['flowRef'][0])

        id_journal = self.db.journal.insert({'method': 'create_series_db'})
        last_update_categories = list(self.db.journal.find(
            {'method': 'insert_categories_db'}).sort([('_id',-1)]).limit(1))
        leaves = list(self.db.categories.find({
            'id_journal': last_update_categories[0]['_id'],
            'flowRef': {'$exists': 'true'}}))
#The next line is for testing purposes.
        leaves = leaves[0:9]
        threads = []
        for leaf in leaves:
            threads.append(threading.Thread(target=create_a_series,
                                            args=(self.lgr,self.db,leaf)))
        i=1
        for thread in threads:
            thread.start()
            time.sleep(math.log(i)+4)
            i += 1
            if i > 70:
                thread[i-70].join()
        for thread in threads[i-70:]:
            thread.join()
        self.lgr.info('create_series_db() done')

    def update_series_db(self):
        """Update the series in MongoDB
        """
        id_journal = self.db.journal.insert({'method': 'update_series_db'})
        last_update_categories = list(self.db.journal.find(
            {'name': 'categories'}).sort([('_id',-1)]).limit(1))
        leaves = list(self.db.categories.find({
            'id_journal': last_update_categories[0]['_id'],
            'flowRef': {'$exists': 'true'}}))
#The next line is for testing purposes.
        leaves = leaves[1:10]
        series = []
        pool = Pool(8)
        for exit_status in pool.map(update_series,leaves):
            if exit_status[0] is True:
                self.lgr.info('Successfully inserted '+exit_status[1])
            else:
                self.lgr.error('Insertion failed '+exit_status[1])
        pool.close()
        pool.join()
