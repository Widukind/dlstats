# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 17:04:10 2015
It is the first draft of fetching the National account data from U.S.
Bureau of Economic Analysis (BEA). 
Work In Progress

"""

from dlstats.fetchers._skeleton import Skeleton, Category, Series, Dataset, Provider
import urllib
import xlrd
import codecs
import datetime
import pandas
import pprint


class BEA(Skeleton):
    def __init__(self):
        super().__init__(provider_name='BEA') 
        self.urls = ['http://www.bea.gov//national/nipaweb/GetCSV.asp?GetWhat=SS_Data/Section1All_xls.xls&Section=2']
        self.readers = []
        self.releaseDates = []
        self.files_ = {}
        for self.url in self.urls:
            self.response= urllib.request.urlopen(self.url)
            self.readers.append(xlrd.open_workbook(file_contents = self.response.read()))
        
        self.provider = Provider(name='BEA',website='http://www.bea.gov/')

    def upsert_dataset(self, datasetCode):
        if datasetCode=='BEA':                                          
            for sheet_name in self.readers[0].sheet_names():  
                sheet = self.readers[0].sheet_by_name(sheet_name)
                line_ = []
                concept = []
                year_row = []
                 
                if  sheet_name != 'Contents':
                    if 'Ann' in sheet_name:
                        frequency = 'annual'
                    else :
                        frequency = 'quarterly' 
                    line_draft = sheet.col(0) 
                    # lines in tables
                    for count_ in range(len(line_draft)):
                        if type(line_draft[count_].value) is float : line_.append(line_draft[count_].value)
                    
                    # rows in the table
                    for count_i in range(8 ,len(sheet.col(0))): 
                        if sheet.col(1)[count_i].value :
                            concept.append(sheet.col(1)[count_i].value)  
                    dimensionList = {'line' : line_ , 'concept' : concept }        
                            
                    for count in range(len(self.readers[0].sheet_by_name(sheet_name).row(7))):
                        if isinstance(self.readers[0].sheet_by_name(sheet_name).row(7)[count].value, float):
                            year_row.append(int(self.readers[0].sheet_by_name(sheet_name).row(7)[count].value))
                        else:
                            year_row.append(self.readers[0].sheet_by_name(sheet_name).row(7)[count].value)
                    period_index = pandas.period_range(year_row[3], year_row[-1] , freq = frequency)
                    lastUpdate = (datetime.datetime.strptime(sheet.col(0)[4].value[15:].strip(), "%B %d, %Y"))
                    
                    document = Dataset(provider = 'BEA', 
                               name = sheet.col(0)[0].value ,
                               datasetCode = 'BEA.' + sheet.col(0)[0].value  , lastUpdate = lastUpdate,
                               dimensionList = dimensionList, docHref = "http://www.bea.gov/") 
                    effective_dimension_list = self.update_series('BEA', dimensionList)    
                    document.update_database()
                    document.update_es_database(effective_dimension_list)                                 
        else:
            raise Exception("The name of dataset was not entered!")
            

    #def upsert_categories(self):
    def update_series(self,datasetCode,dimensionList):
        if datasetCode=='BEA':
            documents = BulkSeries(datasetCode,{})                                          
            for sheet_name in self.readers[0].sheet_names():  
                sheet = self.readers[0].sheet_by_name(sheet_name)
                line_ = []
                concept = []
                year_row = []
                dimensions = {}
                 
                if  sheet_name != 'Contents':
                    if 'Ann' in sheet_name:
                        frequency = 'annual'
                    else :
                        frequency = 'quarterly' 
                    line_draft = sheet.col(0) 
                    # lines in tables
                    for count_ in range(len(line_draft)):
                        if type(line_draft[count_].value) is float : line_.append(line_draft[count_].value)
                    
                    # rows in the table
                    for count_i in range(8 ,len(sheet.col(0))): 
                        if sheet.col(1)[count_i].value :
                            concept.append(sheet.col(1)[count_i].value)  
                    dimensionList = {'line' : line_ , 'concept' : concept }        
                            
                    for count in range(len(self.readers[0].sheet_by_name(sheet_name).row(7))):
                        if isinstance(self.readers[0].sheet_by_name(sheet_name).row(7)[count].value, float):
                            year_row.append(int(self.readers[0].sheet_by_name(sheet_name).row(7)[count].value))
                        else:
                            year_row.append(self.readers[0].sheet_by_name(sheet_name).row(7)[count].value)
                    period_index = pandas.period_range(year_row[3], year_row[-1] , freq = frequency)
                    lastUpdate = (datetime.datetime.strptime(sheet.col(0)[4].value[15:].strip(), "%B %d, %Y"))    
                    for g in range(8 ,len(sheet.col(0))): 
                        if sheet.col(1)[g].value :
                            dimensions['concept'] = sheet.col(1)[g].value
                            dimensions['line'] = sheet.col(0)[g].value
                            series_name = sheet.col(1)[g].value + frequency 
                            series_key = 'BEA.' + sheet.col(1)[g].value + '; ' + sheet.col(2)[g].value
                            series_value = [] 
                            for r in range(3, len(sheet.row(g))):
                                series_value.append(sheet.row(g)[r].value)
                                                
                    documents.append(Series(provider='BEA',
                                            key = series_key,
                                            name = sheet.col(0)[0].value,
                                            datasetCode = 'BEA',
                                            period_index = period_index,
                                            values = series_value,
                                            releaseDates = [lastUpdate],
                                            frequency=frequency,
                                            dimensions=dimensions))                                                
            return(documents.bulk_update_database())                    
                    

if __name__ == "__main__":
    import BEA
    w = BEA.BEA()
    w.upsert_dataset('BEA')    
