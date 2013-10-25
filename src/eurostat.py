from skeleton import Skeleton
import lxml
import urllib
import misc_func

class eurostat(skeleton):
    def __init__(self):
        self.db = self.client.eurostat
        table_of_contents = urllib.request.urlopen('http://www.epp.eurostat.ec.europa.eu/NavTree_prod/everybody/BulkDownloadListing?sort=1&file=table_of_contents.xml', timeout=7)
        table_of_contents = webpage.read()
        self.table_of_contents = lxml.etree.fromstring(table_of_contents)
    def update_categories_db(self):
        """Update the categories in MongoDB
        """
        def walktree(branch,category_name,parent_id ): 
            for children in branch.iterchildren('{urn:eu.europa.ec.eurostat.navtree}children'):
                for branch in children.iterchildren('{urn:eu.europa.ec.eurostat.navtree}branch'):
                    for title in branch.iterchildren('{urn:eu.europa.ec.eurostat.navtree}title'):
                        if title.attrib.values()[0] == 'en':
                            _categories=[title.text]
                            node={'name': title.text}
                            _id = collection.insert(node)
                            collection.update({'_id': parent_id}, {'$push': {'children': _id}})
                    walktree(branch, title.text,_id)
                for leaf in children.iterchildren('{urn:eu.europa.ec.eurostat.navtree}leaf'):
                    for title in leaf.iterchildren('{urn:eu.europa.ec.eurostat.navtree}title'):
                        if title.attrib.values()[0] == 'en':
                            _categories=[title.text]
                            node={'name': title.text}
                            _id = collection.insert(node)
                            collection.update({'_id': parent_id}, {'$push': {'children': _id}})

        root=self.table_of_contents.iterfind('{urn:eu.europa.ec.eurostat.navtree}branch')
        for branch in root:
            for title in branch.iterchildren('{urn:eu.europa.ec.eurostat.navtree}title'):
                if title.attrib.values()[0] == 'en':
                    node={'name': title.text}
                    _id = collection.insert(node)
            walktree(branch, title.text,_id)
    def update_series_db(self):
        """Update the series in MongoDB
        """
        raise NotImplementedError("All the methods from the Skeleton class must"
                                  "be implemented.")
