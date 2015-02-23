from dlstats.fetchers._skeleton import Skeleton, Category, Series, Dataset
import io
import sdmx
import os
import datetime
import pandas
import random

class Ecb(Skeleton):
    def __init__(self):
        super().__init__()         
        self.lgr = logging.getLogger('Ecb')
        self.lgr.setLevel(logging.DEBUG)
        self.fh = logging.FileHandler('Ecb.log')
        self.fh.setLevel(logging.DEBUG)
        self.frmt = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.fh.setFormatter(self.frmt)
        self.lgr.addHandler(self.fh)
        self.sdmx = sdmx.ecb
                           
    def upsert_categories(self):
        def walk_nested_dictionnary(category):
            if 'subcategories' in category:
                chldren_ids = []
                for subcategory in category['subcategories']:
                    children_ids.append(walk_nested_dictionnary(subcategory))
            if 'flowrefs' in category:
                flowrefs = category['flowrefs']
            else:
                flowrefs = None
            document = Category(provider='ECB',name=category['name'],docHref=None,children=children_ids,categoryCode=flowrefs,lastUpdate=None)
            return document.update_database()
        walk_nested_dictionnary(self.sdmx.categories)
