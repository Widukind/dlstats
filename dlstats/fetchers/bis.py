# -*- coding: utf-8 -*-

from .. import constants
from ._commons import Fetcher, Category, Series, Dataset, Provider, CodeDict, ElasticIndex

class BIS(Fetcher):
    
    def __init__(self):
        super().__init__(provider_name='BIS')
        self.provider = Provider(name=self.provider_name, website='http://www.bis.org')

    def upsert_dataset(self, datasetCode):
        if datasetCode=='BIS':
            pass
            #for u in self.urls:
            #    self.upsert_weo_issue(u, datasetCode)
            #es = ElasticIndex()
            #es.make_index(self.provider_name, datasetCode)
        else:
            raise Exception("This dataset is unknown" + dataCode)
        
    def upsert_categories(self):
        document = Category(provider=self.provider_name, 
                            name='BIS' , 
                            categoryCode='BIS',
                            exposed=True)
        
        return document.update_database()
    
    def _extract_sources(self):
        pass
        #TODO: unzip constants.BIS_URLS


def main():
    w = BIS()
    #w.provider.update_database()
    w.upsert_categories()
    w.upsert_dataset('BIS') 

if __name__ == "__main__":
    main()
