# -*- coding: utf-8 -*-

from dlstats.fetchers._skeleton import Skeleton, Category, Series, BulkSeries, Dataset, Provider
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
                'http://localhost:8800/imf/WEOApr2007all.xls'
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
                # needed to be able to search Elasticsearch just after indexing
                sleep(5)
        else:
            raise Exception("The name of dataset was not entered!")
        
    def upsert_weo_issue(self,url):
        response = urllib.request.urlopen(url)
        sheet = csv.DictReader(codecs.iterdecode(response, 'latin-1'), delimiter='\t')
        releaseDate = datetime.strptime(match(".*WEO(\w{7})",url).groups()[0], "%b%Y")
        
        [effective_dimension_list, dataset_data] = self.update_series('WEO', sheet, releaseDate)
        dataset_data.update_database()
        dataset_data.update_es_database(effective_dimension_list)
    
    def upsert_categories(self):
        document = Category(provider = self.provider_name, 
                            name = 'WEO' , 
                            categoryCode ='WEO')
        return document.update_database()
        

    def update_series(self,datasetCode,sheet,releaseDate):
        if datasetCode=='WEO':
            years = sheet.fieldnames[9:-1]
            print(years)
            period_index = pandas.period_range(years[0], years[-1] , freq = 'annual')   
            series = BulkSeries(datasetCode)
            dimensionList = series.EffectiveDimensionList({})
            # get exisisting effective dimensions in Elasticsearch datasets index
            dimensionList.load_previous_effective_dimension_dict(self.provider_name,datasetCode)
            attributeList = {'OBS_VALUE': [('e', 'Estimates Start After')]} 
            for row in sheet:
                value = []
                if row['Country']:               
                    attributes = {}
                    dimensions = {}
                    for year in years:
                        value.append(row[year])
                    dimensions['Country'] = dimensionList.update_entry('Country', row['ISO'], row['Country'])
                    dimensions['WEO Country Code'] = dimensionList.update_entry('WEO Country Code', row['WEO Country Code'], row['WEO Country Code'])
                    dimensions['Subject'] = dimensionList.update_entry('Subject', row['WEO Subject Code'], row['Subject Descriptor'])
                    dimensions['Units'] = dimensionList.update_entry('Units', '', row['Units'])
                    dimensions['Scale'] = dimensionList.update_entry('Scale', row['Scale'], row['Scale'])
                    series_name = row['Subject Descriptor']+'.'+row['Country']+'.'+row['Units']
                    series_key = row['WEO Subject Code']+'.'+row['ISO']+'.'+dimensions['Units']
                    if row['Estimates Start After']:
                        estimation_start = int(row['Estimates Start After']);
                        attributes = {'flag': [ '' if int(y) < estimation_start else 'e' for y in years]}
                    release_dates = [ releaseDate for v in value]
                    series.append(Series(provider=self.provider_name,
                                            key= series_key,
                                            name=series_name,
                                            datasetCode= 'WEO',
                                            period_index= period_index,
                                            values=value,
                                            releaseDates= release_dates,
                                            frequency='A',
                                            attributes = attributes,
                                            dimensions=dimensions))
            attributeList = {'VALUE_STATUS': [('e', 'Estimated')]}
            dataset = Dataset(provider = self.provider_name, 
                               name = 'World Economic Outlook' ,
                               datasetCode = 'WEO', lastUpdate = releaseDate,
                               dimensionList = dimensionList.get(), docHref = "http://http://www.imf.org/",
                               attributeList = attributeList) 
            series.bulk_update_database()
            codeDict = {d1: {d2[0]: d2[1] for d2 in dimensionList.get()[d1]} for d1 in dimensionList.get() }
            return(series.bulk_update_elastic(codeDict,codeDict),dataset)
        else:
            raise Exception("The name of dataset was not entered!")

if __name__ == "__main__":
    import IMF
    w = IMF.IMF()
    w.provider.update_database()
    w.upsert_categories()
    w.upsert_dataset('WEO') 


              
