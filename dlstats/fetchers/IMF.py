# -*- coding: utf-8 -*-

from dlstats.fetchers._skeleton import Skeleton, Category, Series, BulkSeries, Dataset, Provider, DlstatsCollection
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
        self.provider = Provider(name=self.provider_name,website='http://http://www.imf.org/')
        
    def upsert_dataset(self, datasetCode):
        if datasetCode=='WEO':
            weo_urls = [
                'http://localhost:8800/imf/WEOSep2006all.xls',
                #                'http://localhost:8800/imf/WEOApr2007all.xls'
                #            'http://www.imf.org/external/pubs/ft/weo/2006/02/data/WEOSep2006all.xls',
                #            'http://www.imf.org/external/pubs/ft/weo/2007/01/data/WEOApr2007all.xls',
                #        'http://www.imf.org/external/pubs/ft/weo/2007/02/weodata/WEOOct2007all.xls',
                #        'http://www.imf.org/external/pubs/ft/weo/2008/01/weodata/WEOApr2008all.xls',
                #        'http://www.imf.org/external/pubs/ft/weo/2008/02/weodata/WEOOct2008all.xls',
                #        'http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009all.xls',
                #        'http://www.imf.org/external/pubs/ft/weo/2009/02/weodata/WEOOct2009all.xls',
                #        'http://www.imf.org/external/pubs/ft/weo/2010/01/weodata/WEOApr2010all.xls',
                #        'http://www.imf.org/external/pubs/ft/weo/2010/02/weodata/WEOOct2010all.xls',
                #        'http://www.imf.org/external/pubs/ft/weo/2011/01/weodata/WEOApr2011all.xls',
                #        'http://www.imf.org/external/pubs/ft/weo/2011/02/weodata/WEOSep2011all.xls',
                #        'http://www.imf.org/external/pubs/ft/weo/2012/01/weodata/WEOApr2012all.xls',
                #        'http://www.imf.org/external/pubs/ft/weo/2012/02/weodata/WEOOct2012all.xls',
                #        'http://www.imf.org/external/pubs/ft/weo/2013/01/weodata/WEOApr2013all.xls',
                #        'http://www.imf.org/external/pubs/ft/weo/2013/02/weodata/WEOOct2013all.xls',
                #        'http://www.imf.org/external/pubs/ft/weo/2014/01/weodata/WEOApr2014all.xls', 
                #        'http://www.imf.org/external/pubs/ft/weo/2014/02/weodata/WEOOct2014all.xls',
                #        'http://www.imf.org/external/pubs/ft/weo/2015/01/weodata/WEOApr2015all.xls'
            ]
            for u in weo_urls:
                self.upsert_weo_issue(u)
            es = ElasticIndex()
            es.make_index('IMF','WEO')
        else:
            raise Exception("The name of dataset was not entered!")
        
    def upsert_weo_issue(self,url):
        release_date = datetime.strptime(match(".*WEO(\w{7})",url).groups()[0], "%b%Y")
        dataset = Dataset('IMF','WEO')
        dataset.set_name('World Economic Outlook')
        dataset.set_doc_href('http://www.imf.org/')
        dataset.set_last_update(release_date)
        dataset.set_attribute_list({'OBS_VALUE': [('e', 'Estimates Start After')]})
        series = Series(dataset)
        datafile = urllib.request.urlopen(url).read().decode('latin-1').splitlines()
        sheet = csv.DictReader(datafile, delimiter='\t')
        series.set_data_iterator(sheet)
        series.process_series()
        dataset.update_database()
    
    def upsert_categories(self):
        document = Category(provider = self.provider_name, 
                            name = 'WEO' , 
                            categoryCode ='WEO')
        return document.update_database()
        
class Series(Series):
    def initialize_series(self):
        self.years = self.data_iterator.fieldnames[9:-1]
        print(self.years)
        self.period_index = pandas.period_range(self.years[0], self.years[-1] , freq = 'annual')   

    def handle_one_series(self):
        series = {}
        values = []
        row = next(self.data_iterator)
        if row['Country']:               
            self.attributes = {}
            dimensions = {}
            for year in self.years:
                values.append(row[year])
            dimensions['Country'] = self.dimension_dict.update_entry('Country', row['ISO'], row['Country'])
            dimensions['WEO Country Code'] = self.dimension_dict.update_entry('WEO Country Code', row['WEO Country Code'], row['WEO Country Code'])
            dimensions['Subject'] = self.dimension_dict.update_entry('Subject', row['WEO Subject Code'], row['Subject Descriptor'])
            dimensions['Units'] = self.dimension_dict.update_entry('Units', '', row['Units'])
            dimensions['Scale'] = self.dimension_dict.update_entry('Scale', row['Scale'], row['Scale'])
            series_name = row['Subject Descriptor']+'.'+row['Country']+'.'+row['Units']
            series_key = row['WEO Subject Code']+'.'+row['ISO']+'.'+dimensions['Units']
            release_dates = [ self.last_update for v in values]
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
            series['period_index'] = self.period_index
            series['revisions'] = []
            series['frequency'] = 'A'
            return(series)
        else:
            raise StopIteration()
        
if __name__ == "__main__":
    import IMF
    w = IMF.IMF()
    w.provider.update_database()
    w.upsert_categories()
    w.upsert_dataset('WEO') 


              
