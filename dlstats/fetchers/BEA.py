# -*- coding: utf-8 -*-
"""
Created on Thu Sep 10 11:35:26 2015

@author: salimeh
"""

from ._commons import Fetcher, Category, Series, Dataset, Provider, CodeDict, ElasticIndex
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

class BEA(Fetcher):
    def __init__(self):
        super().__init__(provider_name='BEA') 
        self.provider_name = 'BEA'
        self.provider = Provider(name=self.provider_name,website='www.bea.gov/')
        self.sheet = 0
        self.url = 0
    def upsert_nipa(self):     
        urls = ['http://www.bea.gov//national/nipaweb/GetCSV.asp?GetWhat=SS_Data/Section1All_xls.xls&Section=2']
                #'http://www.bea.gov//national/nipaweb/GetCSV.asp?GetWhat=SS_Data/Section2All_xls.xls&Section=3',
                #'http://www.bea.gov//national/nipaweb/GetCSV.asp?GetWhat=SS_Data/Section3All_xls.xls&Section=4',
                #'http://www.bea.gov//national/nipaweb/GetCSV.asp?GetWhat=SS_Data/Section4All_xls.xls&Section=5',
                #'http://www.bea.gov//national/nipaweb/GetCSV.asp?GetWhat=SS_Data/Section5All_xls.xls&Section=6',
                #'http://www.bea.gov//national/nipaweb/GetCSV.asp?GetWhat=SS_Data/Section6All_xls.xls&Section=7',
                #'http://www.bea.gov//national/nipaweb/GetCSV.asp?GetWhat=SS_Data/Section7All_xls.xls&Section=8']
                
        for self.url in urls:
            response= urllib.request.urlopen(self.url)
            reader = xlrd.open_workbook(file_contents = response.read())
            for sheet_name in reader.sheet_names(): 
                sheet = reader.sheet_by_name(sheet_name)
                if  sheet_name != 'Contents':
                    datasetCode = sheet_name
                    self.upsert_dataset(datasetCode, sheet)
                    
                        
    def upsert_dataset(self, datasetCode, sheet):    
        
        dataset = Dataset(self.provider_name,datasetCode)
        bea_data = BeaData(dataset,self.url, self.sheet)
        dataset.name = datasetCode
        dataset.doc_href = 'http://www.bea.gov/newsreleases/national/gdp/gdpnewsrelease.htm'
        #dataset.last_update = (datetime.strptime(sheet.col(0)[4].value[15:].strip(), "%B %d, %Y"))
        dataset.series.data_iterator = BeaData
        dataset.update_database()
        
    def upsert_categories(self):
        document = Category(provider = self.provider_name, 
                            name = 'BEA' , 
                            categoryCode ='BEA')
        return document.update_database() 
                
class BeaData():
    def __init__(self,dataset,url, sheet):
        self.provider_name = dataset.provider_name
        self.dataset_code = dataset.dataset_code
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        self.response= urllib.request.urlopen(url)
        self.reader = xlrd.open_workbook(file_contents = self.response.read()) 
        self.start_date = 1
        self.end_date =  1
        self.lastUpdate = 0
        self.dimensions = {}
    def __next__(self):
        for sheet_name in self.reader.sheet_names():  
            #sheet = self.reader.sheet_by_name(sheet_name)
            line_ = []
            concept = []
            concept_code = []
            year_row = []
            
            
            if  sheet_name != 'Contents':
                if 'Ann' in sheet_name:
                    frequency = 'annual'
                else :
                    frequency = 'quarterly' 
                
                data_vertic = [[sheet.cell_value(r,c) for r in range(8,sheet.nrows)] for c in range(sheet.ncols)] 
                for i in range(len(data_vertic[1])):
                    self.dimensions['concept'] = self.dimension_list.update_entry('concept',data_vertic[2][i],data_vertic[1][i])
                    self.dimensions['line'] = self.dimension_list.update_entry('line','', data_vertic[0][i])

                year = [[sheet.cell_value(7,c).value for c in range(3,sheet.ncols) ]]               
                start_period = year[0][0]
                end_period = year[0][-1]
                if frequency == 'annual':    
                    self.start_date = pandas.Period(str(int(start_period)),freq='A').ordinal
                    self.end_date = pandas.Period(str(int(end_period)),freq='A').ordinal
                elif frequency == 'quarterly':    
                    self.start_date = pandas.Period(start_period,freq='Q').ordinal
                    self.end_date = pandas.Period(end_period,freq='Q').ordinal                 
                self.lastUpdate = (datetime.strptime(self.sheet.col(0)[4].value[15:].strip(), "%B %d, %Y"))           
                
                for g in range(8 ,len(self.sheet.col(0))): 
                    if self.sheet.col(1)[g].value :
                        series = self.build_series(self.sheet,g,frequency)                                                 
                        if series is None:
                            raise StopIteration()            
                                           
    def build_series(self,sheet,g,frequency):  
        series = {}
           
        series_name = self.sheet.col(1)[g].value + frequency 
        series_key = 'BEA.' + self.sheet.col(1)[g].value + '; ' + self.sheet.col(2)[g].value
        series_value = [] 
        for r in range(3, len(self.sheet.row(g))):
            series_value.append(self.sheet.row(g)[r].value)        
        series['values'] = series_value                
        series['provider'] = self.provider_name       
        series['datasetCode'] = self.dataset_code
        series['name'] = series_name
        series['key'] = series_key
        series['startDate'] = self.start_date
        series['endDate'] = self.end_date  
        series['releaseDates'] = self.lastUpdate
        series['dimensions'] = self.dimensions
        series['frequency'] = frequency
        pprint.pprint(series)
        return(series)
     
            
if __name__ == "__main__":
    import BEA
    w = BEA.BEA()
    w.provider.update_database()
    w.upsert_categories()
    w.upsert_nipa()
