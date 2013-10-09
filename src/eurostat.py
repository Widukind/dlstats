from skeleton import Skeleton
import lxml.html
import urllib
import misc_func
import xml.etree.ElementTree as ET

class eurostat(skeleton):
    def __init__(self):
        self.bulk_dl = urllib.request.urlopen('http://www.epp.eurostat.ec.europa.eu/NavTree_prod/everybody/BulkDownloadListing?sort=1&file=table_of_contents.xml', timeout=7)
    def update_categories_db(self):
        """Update the categories in MongoDB
        """
        for elem in tree.iterfind('.//ns:leaf/ns:title/[@language="en"]',
                                 {'ns':'urn:eu.europa.ec.eurostat.navtree'}):
            print (elem.text)
    def update_series_db(self):
        """Update the series in MongoDB
        """
        raise NotImplementedError("All the methods from the Skeleton class must"
                                  "be implemented.")
