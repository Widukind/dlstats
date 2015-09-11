# -*- coding: utf-8 -*-
"""
Created on Thu Sep 10 11:35:26 2015

@author: salimeh
"""

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

class BEA(Skeleton):
    def __init__(self):
        super().__init__(provider_name='BEA') 
        self.provider_name = 'BEA'
        self.provider = Provider(name=self.provider_name,website='www.bea.gov/')
        
    def upsert_dataset(self, datasetCode):
        if datasetCode=='BEA':
            url = 'http://www.bea.gov//national/nipaweb/GetCSV.asp?GetWhat=SS_Data/Section1All_xls.xls&Section=2'
            
            self.upsert_bea_issue(url,datasetCode)
            es = ElasticIndex()                                 
            es.make_index(self.provider_name,datasetCode)      
        else:
            raise Exception("This dataset is unknown" + dataCode)
        
    def upsert_bea_issue(self,url,dataset_code):
        dataset = Dataset(self.provider_name,dataset_code)
        bea_data = BeaData(dataset,url)
        dataset.name = 'National Economic Accounts'
        dataset.doc_href = 'http://www.bea.gov/newsreleases/national/gdp/gdpnewsrelease.htm'
        dataset.series.data_iterator = bea_data
        dataset.update_database()

    def upsert_categories(self):
        document = Category(provider = self.provider_name, 
                            name = 'BEA' , 
                            categoryCode ='BEA')
        return document.update_database() 
                
class BeaData():
    def __init__(self,dataset,url):
        self.provider_name = dataset.provider_name
        self.dataset_code = dataset.dataset_code
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        self.response= urllib.request.urlopen(url)
        self.reader = xlrd.open_workbook(file_contents = self.response.read()) 

    def __iter__(self):
        return self

    def __next__(self):
        return(self.build_series())
        
    def build_series(self,row):
        for sheet_name in self.reader.sheet_names():  
            sheet = self.reader.sheet_by_name(sheet_name)
            line_ = []
            concept = []
            concept_code = []
            year_row = []
            dimensions = {}
            series = {}
            dataset_code = dataset.dataset_code
            provider_name = dataset.provider_name
            if  sheet_name != 'Contents':
                if 'Ann' in sheet_name:
                    frequency = 'annual'
                else :
                    frequency = 'quarterly' 
                line_draft = sheet.col(0) 
                for count_ in range(len(line_draft)):
                    if type(line_draft[count_].value) is float : line_.append(line_draft[count_].value)
                # rows in the table
                for count_i in range(8 ,len(sheet.col(0))): 
                    if sheet.col(1)[count_i].value :
                        concept.append(sheet.col(1)[count_i].value.replace(' ', ''))  
                    if sheet.col(2)[count_i].value  :
                        concept_code.append(sheet.col(2)[count_i].value.replace(' ', ''))                        
                dimensions['concept'] = dimension_list.update_entry('concept',concept_code, concept)        
                dimensions['line'] = dimension_list.update_entry('line','', line_)        
                for count in range(len(sheet.row(7))):
                    if isinstance(sheet.row(7)[count].value, float):
                        year_row.append(int(sheet.row(7)[count].value))                
                start_period = year_row[0]
                end_period = year_row[-1]
                if frequency == 'annual':    
                    start_date = pandas.Period(str(int(start_period)),freq='A').ordinal
                    end_date = pandas.Period(str(int(end_period)),freq='A').ordinal
                elif frequency == 'quarterly':    
                    start_date = pandas.Period(start_period,freq='Q').ordinal
                    end_date = pandas.Period(end_period,freq='Q').ordinal
                    
                lastUpdate = (datetime.datetime.strptime(sheet.col(0)[4].value[15:].strip(), "%B %d, %Y"))  
                
                for g in range(8 ,len(sheet.col(0))): 
                    if sheet.col(1)[g].value :
                        series_name = sheet.col(1)[g].value + frequency 
                        series_key = 'BEA.' + sheet.col(1)[g].value + '; ' + sheet.col(2)[g].value
                        series_value = [] 
                        for r in range(3, len(sheet.row(g))):
                            series_value.append(sheet.row(g)[r].value)        
                series['values'] = series_value                
                series['provider'] = provider_name        
                series['datasetCode'] = dataset_code
                series['name'] = series_name
                series['key'] = series_key
                series['startDate'] = start_date
                series['endDate'] = end_date  
                series['releaseDates'] = lastUpdate
                series['dimensions'] = dimensions
                series['frequency'] = frequency
            return(series)
        else:
            return None        
            
if __name__ == "__main__":
    import BEA
    w = BEA.BEA()
    w.provider.update_database()
    w.upsert_categories()
    w.upsert_dataset('BEA') 