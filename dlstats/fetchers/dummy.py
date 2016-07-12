# -*- coding: utf-8 -*-

from dlstats import constants
from dlstats.utils import Downloader, clean_datetime
from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator

VERSION = 1

class DUMMY(Fetcher):
    
    def __init__(self, **kwargs):
        super().__init__(provider_name='DUMMY', version=VERSION, **kwargs)
        
        self.provider = Providers(name=self.provider_name,
                                  long_name='Dummy Fetcher',
                                  version=VERSION,
                                  region='World',
                                  website='http://www.example.org', 
                                  fetcher=self)
        
    def upsert_dataset(self, dataset_code):
        
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code, 
                           name="My Dataset Name",
                           last_update=clean_datetime(), 
                           fetcher=self)
        dataset.codelists = {
            'COUNTRY': {'FRA': 'France'},
            'OBS_STATUS': {'A': "A"}
        }
        fetcher_data = DUMMY_Data(dataset)
        dataset.series.data_iterator = fetcher_data

        return dataset.update_database()
            
    def build_data_tree(self):
        
        categories = []

        categories.append({
            "category_code": "c1",
            "name": "category 1",
            "doc_href": "http://www.example.org/c1",
            "datasets": [{
                "name": "My Dataset Name", 
                "dataset_code": "ds1",
                "last_update": None, 
                "metadata": None
            }]
        })
        
        return categories
    
DUMMY_SAMPLE_SERIES = [
    {
        'provider_name': "DUMMY", 
        'dataset_code': "ds1",
        'name': "name1", 
        'key': "key1", 
        "slug": "dummy-ds1-key1",             
        'attributes': None,
        'dimensions': {"COUNTRY": "FRA"},
        'start_date': 30, 
        'end_date': 31,
        'frequency': "A",
        'values': [
            {
             "period": "2000", 
             "value": "1", 
             "attributes": {"OBS_STATUS": "A"}
            },
            {
             "period": "2001", 
             "value": "10", 
             "attributes": None
            },
        ],                
    }
] 

class DUMMY_Data(SeriesIterator):
    
    def __init__(self, dataset):
        super().__init__(dataset)

        self.rows = self._process()
        
    def _process(self):
        for series in DUMMY_SAMPLE_SERIES:
            yield series, None
        
    def build_series(self, bson):
        bson["last_update"] = self.dataset.last_update
        return bson
        
    