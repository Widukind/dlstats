#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""Retrieving data from Eurostat"""
from dlstats.fetchers.skeleton import Skeleton
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


def insert_series(leaf):
    try:
        client = pymongo.MongoClient()
        db = client.eurostat
        key = '....'
        series_ = pysdmx.eurostat.data_extraction(leaf['flowRef'][0],key)
        for series in [series_.time_series[key] for key in series_.time_series.keys()]:
            name = leaf['name']
            dates = leaf
            start_date = series[1].index[0]
            end_date = series[1].index[-1]
            values = series[1].values.tolist()
            frequency = series[0]['FREQ']
            codes = series[0]
            categories_id = leaf['_id']
            series_id = db.series.insert({'name':name,
                                          'start_date':start_date, 'end_date':end_date, 'values':values,
                                          'frequency':frequency, 'categories_id':categories_id})
            for code_name, code_value in codes.items():
                db.codes.insert({'name':code_name,'values':{'value':code_value}},
                                 {'$push': {'series_id': series_id}})
        return (True,'flowRef : '+leaf['flowRef'][0])
    except:
        return (False,'flowRef : '+leaf['flowRef'][0])

class Eurostat(Skeleton):
    """Eurostat statistical provider"""
    def __init__(self):
        super().__init__()
        self.client = pymongo.MongoClient()
        self.lgr = logging.getLogger('Eurostat')
        self.lgr.setLevel(logging.DEBUG)
        self.fh = logging.FileHandler('Eurostat.log')
        self.fh.setLevel(logging.DEBUG)
        self.frmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
    def update_categories_db(self):
        """Update the categories in MongoDB
        """

        id_journal = self.db.journal.insert({'name': 'categories'})
        def walktree(branch, parent_id):
            """Recursive function for parsing table_of_contents.xml

            :param branch: The current branch explored. The function
            is going top to bottom.
            :type branch ElementTree
            :param parent_id: The id of the previous branch
            :type branch MongoObject(Id)
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


    def update_series_db(self):
        """Update the series in MongoDB
        """
        id_journal = self.db.journal.insert({'name': 'series'})
        last_update_categories = list(self.db.journal.find(
            {'name': 'categories'}).sort([('_id',-1)]).limit(1))
        leaves = list(self.db.categories.find({
            'id_journal': last_update_categories[0]['_id'],
            'flowRef': {'$exists': 'true'}}))
#The next line is for testing purposes.
        leaves = leaves[1:10]
        series = []
        pool = Pool(8)
        for exit_status in pool.map(insert_series,leaves):
            if exit_status[0] is True:
                self.lgr.info('Successfully inserted '+exit_status[1])
            else:
                self.lgr.error('Insertion failed '+exit_status[1])
        pool.close()
        pool.join()
