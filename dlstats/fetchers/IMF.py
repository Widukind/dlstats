# -*- coding: utf-8 -*-
"""
Created on Fri Feb 20 10:25:29 2015

@author: salimeh/ CEPREMAP
"""
from dlstats.fetchers._skeleton import Skeleton, Category, Series, Dataset, Provider
import urllib
import xlrd
import csv
import codecs
import datetime
import pandas

class IMF(Skeleton):
    def __init__(self):
        super().__init__() 
        self.response= urllib.request.urlopen('http://www.imf.org/external/pubs/ft/weo/2014/01/weodata/WEOApr2014all.xls')
        self.readers = csv.DictReader(codecs.iterdecode(self.response, 'latin-1'), delimiter='\t')
        self.files_ = {'WEOApr2014all':self.readers}
        self.provider = Provider(name='IMF',website='http://http://www.imf.org/')
        self.releaseDates_ = self.response.getheaders()[3][1] 
        self.releaseDates = [datetime.datetime.strptime(self.releaseDates_[5:], "%d %b %Y %H:%M:%S GMT")]
        
    def update_selected_database(self, datasetCode):
        if datasetCode=='WEO':
            reader = self.files_['WEOApr2014all']
        else:
            raise Exception("The name of dataset was not entered!")
        countries_list = []
        ISO_list = []
        Subject_Notes_list = []
        Units_list = []
        Scale_list = []
        WEO_Country_Code_list = []
        Country_Series_specific_Notes_list = []        
        for count, row in enumerate(reader):
            # last 2 rows are blank/metadata
            # so get out when we hit a blank row
            if row['Country']:
                #countrys[row['ISO']] = row['Country']
                if row['Country'] not in countries_list: countries_list.append(row['Country'])
                if row['WEO Country Code'] not in WEO_Country_Code_list: WEO_Country_Code_list.append(row['WEO Country Code'])
                if row['ISO'] not in ISO_list: ISO_list.append(row['ISO']) 
                if row['Subject Notes'] not in Subject_Notes_list: Subject_Notes_list.append(row['Subject Notes'])
                if row['Units'] not in Units_list: Units_list.append(row['Units'])
                if row['Scale'] not in Scale_list: Scale_list.append(row['Scale'])
                if row['Country/Series-specific Notes'] not in Country_Series_specific_Notes_list: Country_Series_specific_Notes_list.append(row['Country/Series-specific Notes'])
                

                    
        dimensionList=[{'name':'WEO Country Code', 'values': WEO_Country_Code_list},
                       {'name':'ISO', 'values': ISO_list},
                       {'name':'country', 'values': countries_list},
                       {'name':'Subject Notes', 'values': Subject_Notes_list},
                       {'name':'Units', 'values': Units_list},
                       {'name':'Scale', 'values': Scale_list},
                       {'name':'Country/Series-specific Notes', 'values': Country_Series_specific_Notes_list}]
                       
        for count, row in enumerate(reader):
            if row['Country']:               
                name = row['Subject Descriptor']
                #key = 'WEO_'+row['WEO Subject Code']
                
                document = Dataset(provider = 'IMF', 
                           name = name ,
                           datasetCode = datasetCode, lastUpdate = self.releaseDates,
                           dimensionList = dimensionList )
                document.update_database()    
    def upsert_categories(self):
        document = Category(provider = 'IMF', 
                            name = 'WEO' , 
                            categoryCode ='WEO')
        return document.update_database()
    def update_a_series(self,datasetCode):
        value = []
        if datasetCode=='WEO':
            reader = self.files_['WEOApr2014all']
        else:
            raise Exception("The name of dataset was not entered!") 
            
        years = reader.fieldnames[9:-1]      
        period_index = pandas.period_range(years[0], years[-1] , freq = 'annual')
        
                    
        for count, row in enumerate(reader):
            if row['Country']:               
                name = row['Subject Descriptor']
                key = 'WEO_'+row['WEO Subject Code'] 
                for year in years:
                    value.append(row[year])
                
                
                dimensions=[{'name':'WEO Country Code', 'values': row['WEO Country Code']},
                           {'name':'ISO', 'values': row['ISO']},
                           {'name':'country', 'values': row['Country']},
                           {'name':'Subject Notes', 'values': row['Subject Notes']},
                           {'name':'Units', 'values': row['Units']},
                           {'name':'Scale', 'values': row['Scale']},
                           {'name':'Country/Series-specific Notes', 'values': row['Country/Series-specific Notes']}]
         
            document = Series(provider = 'WorldBank', 
                              name = name , key = key,
                              datasetCode = 'WEO', values = value,
                              period_index = period_index
                              , releaseDates = self.releaseDates,
                              dimensions =  dimensions)
            document.update_database(key=key)     
            



              