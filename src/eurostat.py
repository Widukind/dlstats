"""Retrieving data from Eurostat"""
from skeleton import Skeleton
import lxml
import urllib
from pandas.tseries.offsets import *
import pandas
from io import BytesIO, StringIO, TextIOWrapper
import urllib.request
import gzip

class Eurostat(Skeleton):
    """Eurostat statistical provider"""
    def __init__(self):
        super(Eurostat, self).__init__()
        self.db = self.client.eurostat
        webpage = urllib.request.urlopen(
            'http://www.epp.eurostat.ec.europa.eu/'
            'NavTree_prod/everybody/BulkDownloadListing'
            '?sort=1&file=table_of_contents.xml', timeout=7)
        table_of_contents = webpage.read()
        self.table_of_contents = lxml.etree.fromstring(table_of_contents)
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



    def download_data(self,url)
        """Returns a pandas DataFrame
        """
        response = urllib.request.urlopen(url)
        memzip = BytesIO(response.read())
        archive= gzip.open(memzip,'rb')
        tsv = TextIOWrapper(archive)
        data = pandas.read_csv(tsv,encoding='utf-8', delimiter="[,\t]", header=0, index_col=[0,1,2,3], na_values= ':', decimal='.')
        return data


    def update_series_db(self):
        """Update the series in MongoDB
        """
        raise NotImplementedError("All the methods from the Skeleton class must"
                                  "be implemented.")
