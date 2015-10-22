# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 10:59:20 2015

@author: salimeh
"""


from dlstats.fetchers._commons import Fetcher, Category, Series, Dataset, Provider, CodeDict, ElasticIndex
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
import requests
from lxml import etree



class Esri(Fetcher):
    def __init__(self):
        super().__init__()         
        self.provider_name = 'esri'
        self.provider = Provider(name=self.provider_name,website='http://www.esri.cao.go.jp/index-e.html')
        self.url_amount = []
        url_list_amount = ['mg1442','mk1442','jg1442','jk1442','mfy1442','mcy1442','jfy1442','jcy1442']
        for index in url_list_amount:
            self.url_amount.append('http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2014/qe144_2/__icsFiles/afieldfile/2015/03/04/gaku-'+index+'.csv')
        #read the url links for deflator parts
        self.url_deflator = []
        url_list_deflator = ['def-qg1442','def-qk1442','rdef-qg1442','rdef-qk1442']
        for index in url_list_deflator :
            self.url_deflator.append('http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2014/qe144_2/__icsFiles/afieldfile/2015/03/04/'+index+'.csv')
        self.url_all = url_amount + url_deflator 
        self.datasetCode_list = ['Nominal Gross Domestic Product (original series)',
                'Nominal Gross Domestic Product (seasonally adjusted series)',
                'Real Gross Domestic Product (original series)',
                'Real Gross Domestic Product (seasonally adjusted series)',
                'Annual Nominal GDP (fiscal year)',
                'Annual Nominal GDP (calendar year)' ,
                'Annual Real GDP (fiscal year)',
                'Annual Real GDP (calendar year)',
                'Deflators (quarter:original series)',
                'Deflators (quarter:seasonally adjusted series)' ,
                'Deflators (fiscal year)',
                'Deflators (calendar year)']
    def upsert_categories(self):
        document = Category(provider = self.provider_name, 
                            name = 'esri', 
                            categoryCode ='esri',
                            children = [None])
        return document.update_database()
        
    def esri_issue(self):
        for self.url in self.url_all :
            datasetCode = self.datasetCode_list[self.url_all.index(self.url)]
            self.upsert_dataset(datasetCode)

    def upsert_dataset(self, datasetCode):
        self.upsert_sna(self.url,datasetCode)                  
        es = ElasticIndex()
        es.make_index(self.provider_name,datasetCode)

    def upsert_sna(self, url, dataset_code):
        dataset = Dataset(self.provider_name,dataset_code)
        sna_data = EsriData(dataset,url)
        dataset.name = dataset_code
        dataset.doc_href = 'http://www.esri.cao.go.jp/index-e.html'
        dataset.last_update = sna_data.releaseDate
        dataset.series.data_iterator = sna_data
        dataset.update_database()

        
class EsriData():
    def __init__(self,dataset,url):
        self.provider_name = dataset.provider_name
        self.dataset_code = dataset.dataset_code
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        self.panda_csv = pandas.read_csv(url)
        response = urllib.request.urlopen(url)
        releaseDate = response.info()['Last-Modified'] 
        self.releaseDate = datetime.strptime(releaseDate, 
                                                      "%a, %d %b %Y %H:%M:%S GMT")                                                  
 
        if self.panda_csv.icol(0)[6] == '4' :
            self.frequency = 'A'
            ind = -1 
        else :
            self.frequency = 'Q'
            ind = -4
        end_date = self.panda_csv.icol(0)[len(self.panda_csv.icol(0))+ind][:4]
        start_date = self.panda_csv.icol(0)[6][:4]
        self.end_date = pandas.Period(end_date,freq = self.frequency).ordinal    
        self.start_date = pandas.Period(start_date,freq = self.frequency).ordinal
        self.column_range = iter(range(len(self.panda_csv.irow(5))))
       #generating name of the series             
        columns =self.panda_csv.columns
        for column_ind in range(columns.size):
            if str(self.panda_csv.icol(column_ind)[5]) != "nan":
                self.panda_csv.icol(column_ind)[3] = str(self.panda_csv.icol(column_ind)[4])+'_'+str(self.panda_csv.icol(column_ind)[5])
            else:    
                self.panda_csv.icol(column_ind)[3] = str(self.panda_csv.icol(column_ind)[4])
            if str(self.panda_csv.icol(column_ind)[4]) == "nan" :
                if (str(self.panda_csv.icol(column_ind)[5]) != "nan") and (str(self.panda_csv.icol(column_ind-1)[4])) != "nan":         
                    self.panda_csv.icol(column_ind)[3] = str(self.panda_csv.icol(column_ind-1)[4])+'_'+str(self.panda_csv.icol(column_ind)[5])
                else:
                    if str(self.panda_csv.icol(column_ind-1)[4]) == "nan":
                        self.panda_csv.icol(column_ind)[3] = str(self.panda_csv.icol(column_ind-2)[4])+'_'+str(self.panda_csv.icol(column_ind)[5])            
      
        
        
    def __next__(self):
        column = self.panda_csv.icol(next(self.column_range))
        if column is None:
            raise StopIteration()
        series = self.build_series(column)
        if series is None:
            raise StopIteration()            
        return(series) 
           
                                           
    def build_series(self,column):
        
        dimensions = {}
        series = {}
        series_value = [] 
        
        series_name = str(column[3]) + self.frequency 
        series_key = 'esri.' + str(column[3]) + '; ' + self.frequency
        dimensions['concept'] = self.dimension_list.update_entry('concept','',str(column[3]))  
        
        for r in range(6, len(column)):
            series_value.append(str(column[r]))    
        release_dates = [self.releaseDate for v in series_value] 
        series['values'] = series_value                
        series['provider'] = self.provider_name       
        series['datasetCode'] = self.dataset_code
        series['name'] = series_name
        series['key'] = series_key
        series['startDate'] = self.start_date
        series['endDate'] = self.end_date  
        series['releaseDates'] = release_dates
        series['dimensions'] = dimensions
        series['frequency'] = self.frequency
        series['attributes'] = {}
        return(series)

if __name__ == "__main__":
    e = Esri()
    e.provider.update_database()
    e.esri_issue()
    e.upsert_categories()
    
