"""Retrieving data from Eurostat"""
from skeleton import Skeleton
import lxml.etree
import urllib
from pandas.tseries.offsets import *
import pandas
import datetime
from io import BytesIO, StringIO, TextIOWrapper
import urllib.request
import gzip
import uuid
import re
import pymongo
import logging

class Eurostat(Skeleton):
    """Eurostat statistical provider"""
    def __init__(self):
        super(Eurostat, self).__init__()
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
                    for download_link in leaf.iterchildren(
                        '{urn:eu.europa.ec.eurostat.navtree}downloadLink'):
                        if download_link.attrib.values()[0] == 'tsv':
                            _url = download_link.text
                            self.db.categories.update(
                                {'_id': _id},
                                {'$push': {'url': _url }})

        root = self.table_of_contents.iterfind(
            '{urn:eu.europa.ec.eurostat.navtree}branch')
        for branch in root:
            for title in branch.iterchildren(
                '{urn:eu.europa.ec.eurostat.navtree}title'):
                if title.attrib.values()[0] == 'en':
                    node = {'name': title.text, 'id_journal': id_journal}
                    _id = self.db.categories.insert(node)
            walktree(branch, _id)

    def leaf_to_pandas(self, url_leaf):
        """Download series contained in a leaf and returns a list of pandas
        DataFrame"""
        def _date_parser(date):
            regex_quarterly = re.compile('[0-9]{4}Q[1-4]')
            regex_monthly = re.compile('[0-9]{4}M[0-9]{2}')
            if re.search(regex_quarterly, date):
                date = date.split('Q')
                date = str(int(date[1])*3) + date[0]
                return datetime.datetime.strptime(date, '%m%Y')
            if re.search(regex_monthly, date):
                return datetime.datetime.strptime(date, '%YM%m')
        response = urllib.request.urlopen(url_leaf)
        memzip = BytesIO(response.read())
        archive = gzip.open(memzip,'rb')
        tsv = TextIOWrapper(archive, encoding='utf-8', newline='')
        split_tsv = [line.strip() for line in tsv]
        header = split_tsv[0].split()
        cols_labels = header[0].split(',')
        dates = [_date_parser(date_string) 
                 for date_string in header[1:]]
        series_from_leaf = []
        for line in split_tsv[1:]:
            metadata = {}
            i = 0
            for label in cols_labels:
                value = line.split(',')[i].split('\t')[0]
                label = label.split('\\')
                metadata[label[0]] = value
                i += 1
            identifier = 'id'+str(uuid.uuid4()).replace('-','')
            values = line.split('\t')[1:]
            values = [re.sub(r'[a-z]','',value).strip() for value in values]
            series = pandas.DataFrame(
                {identifier: values}, index=dates).replace(
                    ':', pandas.np.nan).replace(
                        ': ', pandas.np.nan).astype('float')
            series_from_leaf.append([identifier, metadata, series])
        return series_from_leaf

    def update_series_db(self):
        """Update the series in MongoDB
        """
        id_journal = self.db.journal.insert({'name': 'series'})
        file_identifier = id_journal.__repr__().split('\'')[1]
        store = pandas.HDFStore(
            self.store_path + 'eurostat' + file_identifier + '.h5',
            complevel=9, complib='zlib')
        last_update_categories = list(self.db.journal.find(
            {'name': 'categories'}).sort([('_id',-1)]).limit(1))
        leaves = list(self.db.categories.find({
            'id_journal': last_update_categories[0]['_id'],
            'url': {'$exists': 'true'}}))
        series = []
        for leaf in leaves:
            series = self.leaf_to_pandas(leaf['url'][0])
            for a_series in series:
                a_series[1]['hdfpath'] = 'id'+str(uuid.uuid4()).replace('-','')
                self.db.series.insert({'uuid': a_series[0],'metadata': a_series[1]})
                store[a_series[1]['hdfpath']+'/'+a_series[0]] = a_series[2]
        store.close()

