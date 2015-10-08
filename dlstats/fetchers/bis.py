# -*- coding: utf-8 -*-

import io
from zipfile import ZipFile
import zipfile
import urllib.request
import csv
import datetime

import pandas

from .. import constants
from ._commons import Fetcher, Category, Dataset, Provider, ElasticIndex

def load_zip_file(url):
    response = urllib.request.urlopen(url)
    zfile = zipfile.ZipFile(io.BytesIO(response.read()))
    filename = zfile.namelist()[0]
    return zfile.read(filename)
    
class BIS(Fetcher):
    
    def __init__(self, db=None, es_client=None, settings=None):
        super().__init__(provider_name='BIS', db=db)
        self.provider = Provider(name=self.provider_name, website='http://www.bis.org', db=db)
        self.es_client = es_client
        self.settings = settings or {}

    def upsert_dataset(self, id):
        if id == 'LBS-DISS':
            url = self.settings.get("BIS_LBS_DISS_URL", constants.BIS_LBS_DISS_URL)
            self.upsert_lbs_diss(url, id)
            es = ElasticIndex(db=self.db, es_client=self.es_client)
            es.make_index(self.provider_name, id)
        else:
            raise Exception("This dataset is unknown" + id)
        
    def upsert_categories(self):
        document = Category(provider=self.provider_name, 
                            name='Locational Banking Statistics - disseminated data', 
                            categoryCode='LBS-DISS',
                            exposed=True,
                            db=self.db)
        
        return document.update_database()
            
    def upsert_lbs_diss(self, url, dataset_code):
        
        dataset = Dataset(self.provider_name, dataset_code, db=self.db)
        
        fetcher_data = LBS_DISS_Data(dataset, url)
        
        dataset.name = 'Locational Banking Statistics - disseminated data'
        dataset.doc_href = 'http://www.bis.org/statistics/bankstats.htm'
        dataset.last_update = fetcher_data.release_date
        
        dataset.series.data_iterator = fetcher_data
        dataset.update_database()
        

class LBS_DISS_Data():
    """
    @see: http://www.bis.org/statistics/dsd_lbs.pdf
    """
    
    def __init__(self, dataset, url):
        
        self.provider_name = dataset.provider_name
        self.dataset_code = dataset.dataset_code
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        
        csv_text = load_zip_file(url)
        csv_file = io.TextIOWrapper(io.BytesIO(csv_text), newline="\n")
        next(csv_file)
        txt_date = csv_file.readline().replace("Retrieved on,", "").strip()
        self.release_date = datetime.datetime.strptime(txt_date, "%a %b %d %H:%M:%S %Z %Y")
        next(csv_file)
        next(csv_file)
        self.sheet = csv.DictReader(csv_file)
        
        self.dimension_keys = self.sheet.fieldnames[:13]
        
        self.periods = self.sheet.fieldnames[13:]
        self.start_date = pandas.Period(self.periods[0], freq='quarter')
        self.end_date = pandas.Period(self.periods[-1], freq='quarter')

    def __next__(self):
        row = next(self.sheet) 
        series = self.build_series(row)
        if series is None:
            raise StopIteration()            
        return(series)
        
    def build_series(self,row):
        if row['Measure']:               
            series = {}
            values = [row[period] for period in self.periods]
            dimensions = {}
            
            for d in self.dimension_keys:
                dim_short_id = row[d].split(":")[0]
                dim_long_id = row[d].split(":")[1]
                dimensions[d] = self.dimension_list.update_entry(d, dim_short_id, dim_long_id)

            series_name = "-".join([row[d] for d in self.dimension_keys if d != 'Time Period'])
            series_key = row['Time Period']
            
            release_dates = [self.release_date for v in values]
            #print("---------------------------------------------------------")
            #print(len(self.periods), len(values), len(release_dates))
            #print("---------------------------------------------------------")
            
            series['provider'] = self.provider_name
            series['datasetCode'] = self.dataset_code
            series['name'] = series_name
            series['key'] = series_key
            series['values'] = values
            series['attributes'] = {}
            series['dimensions'] = dimensions
            series['releaseDates'] = release_dates
            series['startDate'] = self.start_date.ordinal
            series['endDate'] = self.end_date.ordinal
            series['frequency'] = 'Q'
            return(series)
        else:
            return None
        

def main():
    w = BIS()
    w.provider.update_database()
    w.upsert_categories()
    w.upsert_dataset('LBS-DISS') 

if __name__ == "__main__":
    main()
