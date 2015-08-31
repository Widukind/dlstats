# -*- coding: utf-8 -*-

from dlstats.fetchers._skeleton import Skeleton, Category, Series, Dataset, Provider, CodeDict
from dlstats.fetchers.make_elastic_index import ElasticIndex
import urllib
import xlrd
import csv
import codecs
from datetime import datetime
import pandas
import pprint
from collections import OrderedDict
from re import match
from time import sleep

class IMF(Skeleton):
    def __init__(self):
        super().__init__(provider_name='IMF') 
        self.provider_name = 'IMF'
        self.provider = Provider(name=self.provider_name,website='http://www.imf.org/')
        
    def upsert_dataset(self, datasetCode):
        if datasetCode=='WEO':
            weo_urls = [
                'http://www.imf.org/external/pubs/ft/weo/2006/02/data/WEOSep2006all.xls',
                'http://www.imf.org/external/pubs/ft/weo/2007/01/data/WEOApr2007all.xls',
#                'http://www.imf.org/external/pubs/ft/weo/2007/02/weodata/WEOOct2007all.xls',
#                'http://www.imf.org/external/pubs/ft/weo/2008/01/weodata/WEOApr2008all.xls',
#                'http://www.imf.org/external/pubs/ft/weo/2008/02/weodata/WEOOct2008all.xls',
#                'http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009all.xls',
#                'http://www.imf.org/external/pubs/ft/weo/2009/02/weodata/WEOOct2009all.xls',
#                'http://www.imf.org/external/pubs/ft/weo/2010/01/weodata/WEOApr2010all.xls',
#                'http://www.imf.org/external/pubs/ft/weo/2010/02/weodata/WEOOct2010all.xls',
#                'http://www.imf.org/external/pubs/ft/weo/2011/01/weodata/WEOApr2011all.xls',
#                'http://www.imf.org/external/pubs/ft/weo/2011/02/weodata/WEOSep2011all.xls',
#                'http://www.imf.org/external/pubs/ft/weo/2012/01/weodata/WEOApr2012all.xls',
#                'http://www.imf.org/external/pubs/ft/weo/2012/02/weodata/WEOOct2012all.xls',
#                'http://www.imf.org/external/pubs/ft/weo/2013/01/weodata/WEOApr2013all.xls',
#                'http://www.imf.org/external/pubs/ft/weo/2013/02/weodata/WEOOct2013all.xls',
#                'http://www.imf.org/external/pubs/ft/weo/2014/01/weodata/WEOApr2014all.xls',
#                'http://www.imf.org/external/pubs/ft/weo/2014/02/weodata/WEOOct2014all.xls',
#                'http://www.imf.org/external/pubs/ft/weo/2015/01/weodata/WEOApr2015all.xls'
            ]
            for u in weo_urls:
                self.upsert_weo_issue(u,datasetCode)
            es = ElasticIndex()
            es.make_index(self.provider_name,datasetCode)
        else:
            raise Exception("This dataset is unknown" + dataCode)
        
    def upsert_weo_issue(self,url,dataset_code):
        dataset = Dataset(self.provider_name,dataset_code)
        weo_data = WeoData(dataset,url)
        dataset.name = 'World Economic Outlook'
        dataset.doc_href = 'http://www.imf.org/external/ns/cs.aspx?id=28'
        dataset.last_update = weo_data.release_date
        dataset.attribute_list.update(CodeDict({'flags': {'e': 'Estimated'}}))
        dataset.series.data_iterator = weo_data
        dataset.update_database()

    def upsert_categories(self):
        document = Category(provider = self.provider_name, 
                            name = 'WEO' , 
                            categoryCode ='WEO')
        return document.update_database()
        
class WeoData():
    def __init__(self,dataset,url):
        self.provider_name = dataset.provider_name
        self.dataset_code = dataset.dataset_code
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        datafile = urllib.request.urlopen(url).read().decode('latin-1').splitlines()
        self.sheet = csv.DictReader(datafile, delimiter='\t')
        self.years = self.sheet.fieldnames[9:-1]
        print(self.years)
        self.start_date = pandas.Period(self.years[0],freq='annual')
        self.end_date = pandas.Period(self.years[-1],freq='annual')
        self.release_date = datetime.strptime(match(".*WEO(\w{7})",url).groups()[0], "%b%Y")

    def __next__(self):
        row = next(self.sheet)
        series = self.build_series(row)
        if series is None:
            raise StopIteration()            
        return(series)
        
    def build_series(self,row):
        if row['Country']:               
            series = {}
            values = []
            dimensions = {}
            for year in self.years:
                values.append(row[year])
            dimensions['Country'] = self.dimension_list.update_entry('Country', row['ISO'], row['Country'])
            dimensions['WEO Country Code'] = self.dimension_list.update_entry('WEO Country Code', row['WEO Country Code'], row['WEO Country Code'])
            dimensions['Subject'] = self.dimension_list.update_entry('Subject', row['WEO Subject Code'], row['Subject Descriptor'])
            dimensions['Units'] = self.dimension_list.update_entry('Units', '', row['Units'])
            dimensions['Scale'] = self.dimension_list.update_entry('Scale', row['Scale'], row['Scale'])
            series_name = row['Subject Descriptor']+'.'+row['Country']+'.'+row['Units']
            series_key = row['WEO Subject Code']+'.'+row['ISO']+'.'+dimensions['Units']
            release_dates = [ self.release_date for v in values]
            series['provider'] = self.provider_name
            series['datasetCode'] = self.dataset_code
            series['name'] = series_name
            series['key'] = series_key
            series['values'] = values
            series['attributes'] = {}
            if row['Estimates Start After']:
                estimation_start = int(row['Estimates Start After']);
                series['attributes'] = {'flag': [ '' if int(y) < estimation_start else 'e' for y in self.years]}
            series['dimensions'] = dimensions
            series['releaseDates'] = release_dates
            series['startDate'] = self.start_date.ordinal
            series['endDate'] = self.end_date.ordinal
            series['frequency'] = 'A'
            if row['Subject Notes']:
                series['notes'] = row['Subject Notes']
            if row['Country/Series-specific Notes']:
                row['Country/Series-specific Notes'] += '\n' + row['Country/Series-specific Notes']
            return(series)
        else:
            return None
        
if __name__ == "__main__":
    import IMF
    w = IMF.IMF()
    w.provider.update_database()
    w.upsert_categories()
    w.upsert_dataset('WEO') 


              
