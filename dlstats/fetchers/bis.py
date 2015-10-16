# -*- coding: utf-8 -*-

import io
from zipfile import ZipFile
import zipfile
import urllib.request
import csv
import datetime

import pandas

from dlstats import constants
from dlstats.fetchers._commons import Fetcher, Category, Dataset, Provider

__all__ = ['BIS']

def load_zip_file(url):
    response = urllib.request.urlopen(url)
    zfile = zipfile.ZipFile(io.BytesIO(response.read()))
    filename = zfile.namelist()[0]
    return zfile.read(filename)
    
class BIS(Fetcher):
    
    def __init__(self, db=None, es_client=None):
        
        super().__init__(provider_name='BIS', 
                         db=db, 
                         es_client=es_client)
        
        self.provider = Provider(name=self.provider_name, 
                                 website='http://www.bis.org', 
                                 fetcher=self)

    def upsert_dataset(self, dataset_code):
        
        if dataset_code == 'LBS-DISS':
            self.upsert_lbs_diss(dataset_code)
            self.update_metas(dataset_code)
        else:
            raise Exception("This dataset is unknown" + dataset_code)
        
    def upsert_categories(self):
        
        document = Category(provider=self.provider_name, 
                            name='Locational Banking Statistics - disseminated data', 
                            categoryCode='LBS-DISS',
                            exposed=True,
                            fetcher=self)
        
        return document.update_database()
            
    def upsert_lbs_diss(self, dataset_code):
        
        dataset = Dataset(provider_name=self.provider_name, 
                          dataset_code=dataset_code, 
                          name='Locational Banking Statistics - disseminated data', 
                          doc_href='http://www.bis.org/statistics/bankstats.htm', 
                          #last_update=fetcher_data.release_date, 
                          fetcher=self)
        
        fetcher_data = LBS_DISS_Data(dataset)
        #TODO: dataset.last_update a faire dans LBS_DISS_Data
        dataset.last_update = fetcher_data.release_date        
        dataset.series.data_iterator = fetcher_data
        dataset.update_database()
        
class LBS_DISS_Data():
    """
    
    @see: http://www.bis.org/statistics/dsd_lbs.pdf
    """
    
    def __init__(self, dataset):
        
        self.provider_name = dataset.provider_name
        self.dataset_code = dataset.dataset_code
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        
        self.frequency = 'Q'
        
        csv_text = load_zip_file(self.url)
        csv_file = io.TextIOWrapper(io.BytesIO(csv_text), newline="\n")
        
        next(csv_file)
        
        txt_date = csv_file.readline().replace("Retrieved on,", "").strip()
        
        self.release_date = datetime.datetime.strptime(txt_date, "%a %b %d %H:%M:%S %Z %Y")
        
        next(csv_file)
        next(csv_file)
        
        self.sheet = csv.DictReader(csv_file)
        
        period_position = 0
        for e in enumerate(self.sheet.fieldnames):
            if e[1] == "Time Period":
                period_position = e[0]
        
        self.dimension_keys = self.sheet.fieldnames[:period_position]
        self.periods = self.sheet.fieldnames[period_position+1:]
        
        self.start_date = pandas.Period(self.periods[0], freq=self.frequency)
        self.end_date = pandas.Period(self.periods[-1], freq=self.frequency)
        
    @property
    def url(self):
        return "http://www.bis.org/statistics/full_bis_lbs_diss_csv.zip"

    def __next__(self):
        row = next(self.sheet) 
        series = self.build_series(row)
        if series is None:
            raise StopIteration()
        return(series)
    
    def build_series(self, row):
        """Build one serie
        
        Return instance of :class:`dict`
        """
        if row.get('Measure'):
            series = {}
            values = [row[period] for period in self.periods]
            dimensions = {}
            
            for d in self.dimension_keys:
                dim_short_id = row[d].split(":")[0]
                dim_long_id = row[d].split(":")[1]
                dimensions[d] = self.dimension_list.update_entry(d, dim_short_id, dim_long_id)
                
            # concatene la valeur de toutes les dimensions
            series_name = "-".join([row[d] for d in self.dimension_keys])
            
            series_key = row['Time Period']

            release_dates = [self.release_date for v in values]
            
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
            series['frequency'] = self.frequency
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
