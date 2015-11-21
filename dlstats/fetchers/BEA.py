# -*- coding: utf-8 -*-
"""
Created on Thu Sep 10 11:35:26 2015

@author: salimeh
"""

from dlstats.fetchers._commons import Fetcher, Categories, Series, Datasets, Providers, CodeDict, ElasticIndex
from dlstats import constants
import urllib
import xlrd
from datetime import datetime
import pandas
from collections import OrderedDict
import zipfile
import io

class BEA(Fetcher):
    def __init__(self, db=None, es_client=None):
        super().__init__(provider_name='BEA',  db=db, es_client=es_client) 
        self.list_sheet = []
        self.provider_name = 'BEA'
        self.provider = Providers(name = self.provider_name ,
                                  long_name = 'Bureau of Economic Analysis',
                                  region = 'USA',
                                  website='www.bea.gov/',
                                  fetcher=self)
       
        #self.urls= {'National Data_GDP & Personal Income' :'http://www.bea.gov//national/nipaweb/GetCSV.asp?GetWhat=SS_Data/SectionAll_xls.zip&Section=11',
        #            'National Data_Fixed Assets': 'http://www.bea.gov//national/FA2004/GetCSV.asp?GetWhat=SS_Data/SectionAll_xls.zip&Section=11', 
         #           'Industry data_GDP by industry_Q': 'http://www.bea.gov//industry/iTables%20Static%20Files/AllTablesQTR.zip',
          #          'Industry data_GDP by industry_A': 'http://www.bea.gov//industry/iTables%20Static%20Files/AllTables.zip',
           #         'International transactions(ITA)': 'http://www.bea.gov/international/bp_web/startDownload.cfm?dlSelect=tables/XLSNEW/ITA-XLS.zip',
            #        'International services': 'http://www.bea.gov/international/bp_web/startDownload.cfm?dlSelect=tables/XLSNEW/IntlServ-XLS.zip',
             #       'International investment position(IIP)': 'http://www.bea.gov/international/bp_web/startDownload.cfm?dlSelect=tables/XLSNEW/IIP-XLS.zip'}
         
        self.urls= ['http://www.bea.gov//national/nipaweb/GetCSV.asp?GetWhat=SS_Data/SectionAll_xls.zip&Section=11',
                     'http://www.bea.gov//national/FA2004/GetCSV.asp?GetWhat=SS_Data/SectionAll_xls.zip&Section=11'] 
                    #'http://www.bea.gov//industry/iTables%20Static%20Files/AllTablesQTR.zip',
                     #'http://www.bea.gov//industry/iTables%20Static%20Files/AllTables.zip',
                    # 'http://www.bea.gov/international/bp_web/startDownload.cfm?dlSelect=tables/XLSNEW/ITA-XLS.zip',
                     #'http://www.bea.gov/international/bp_web/startDownload.cfm?dlSelect=tables/XLSNEW/IntlServ-XLS.zip',
                     #'http://www.bea.gov/international/bp_web/startDownload.cfm?dlSelect=tables/XLSNEW/IIP-XLS.zip']
                    
    def upsert_nipa(self):  
        for self.url in self.urls:
            #self.url = self.urls[url_key]
            #print(self.url)
            response = urllib.request.urlopen(self.url)
            zipfile_ = zipfile.ZipFile(io.BytesIO(response.read()))
            #excel_filenames = iter(zipfile_.namelist())
            #fname = next(excel_filenames)
    
            #if fname is None:
            #    raise StopIteration()
            for section in zipfile_.namelist():
                #print(section)
                if section !='Iip_PrevT3a.xls' and section !='Iip_PrevT3b.xls' and section !='Iip_PrevT3c.xls' :
                    excel_book = xlrd.open_workbook(file_contents = zipfile_.read(section)) 
                    for sheet_name in excel_book.sheet_names(): 
                        sheet = excel_book.sheet_by_name(sheet_name)
                        
                        if sheet_name != 'Contents':
                            self.list_sheet.append(sheet)
                            datasetCode = sheet_name
                            self.upsert_dataset(datasetCode) 
                            
                # else :
                #ToDO: lip_PrevT3a, lip_PrevT3b, lip_PrevT3c          
                    
                        
    def upsert_dataset(self, datasetCode):    
        
        dataset = Datasets(self.provider_name,datasetCode,
                           fetcher=self)
        sheet = self.list_sheet[-1]                   
        bea_data = BeaData(dataset,self.url, sheet)
        dataset.name = datasetCode
        dataset.doc_href = 'http://www.bea.gov/newsreleases/national/gdp/gdpnewsrelease.htm'
        dataset.last_update = bea_data.release_date
        dataset.series.data_iterator = bea_data
        dataset.update_database()
        self.update_metas(datasetCode)

        
    def upsert_categories(self):
        document = Categories(provider = self.provider_name, 
                            name = 'BEA' , 
                            categoryCode ='BEA',
                            children = None,
                            fetcher=self )
        return document.update_database() 
        
    def upsert_all_datasets(self):
        for dataset_code in self.list_sheet :
            self.upsert_dataset(dataset_code.name)     
        
        
class BeaData():
    def __init__(self,dataset,url, sheet):
        self.sheet = sheet
        self.provider_name = dataset.provider_name
        self.dataset_code = dataset.dataset_code
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        str = sheet.cell_value(2,0) #released Date
        info = []
        #retrieve frequency from url        
        if 'AllTablesQTR' in url :
            self.frequency = 'Q'
        if  'AllTables.' in url : 
            self.frequency = 'A'
        #retrieve frequency from sheet name  
        if 'Qtr' in self.sheet.name :
            self.frequency = 'Q' 
        if 'Ann'  in self.sheet.name or 'Annual' in self.sheet.name:
            self.frequency = 'A'
        if 'Month'  in self.sheet.name :
            self.frequency = 'M'            
            
        #print(url)
        #print(self.sheet.name)    
        #print( self.frequency)
        if 'Section' in  url :
            release_datesheet = sheet.cell_value(4,0)[15:] 
        else :
            release_datesheet = sheet.cell_value(3,0)[14:] 
        if 'ITA-XLS' in url or 'IIP-XLS' in url :
            release_datesheet = sheet.cell_value(3,0)[14:].split('-')[0]
            
        years = [int(s) for s in str.split() if s.isdigit()] 
        #To DO: start years and end_dates
        self.start_date = pandas.Period(years[0],freq = self.frequency).ordinal
        self.end_date = pandas.Period(years[1],freq = self.frequency).ordinal
        self.release_date = datetime.strptime(release_datesheet.strip(), "%B %d, %Y") 
        self.dimensions = {} 
        
        if 'Section' in  url :
            row_start = sheet.col_values(0).index(1)
        else:     
            col_values_ = [cell.strip(' ') for cell in sheet.col_values(0)]
            if 'A1' in col_values_:
                row_start = col_values_.index('A1')
            else :    
                row_start = col_values_.index('1')         
        self.row_range = iter(range(row_start, sheet.nrows))
        if '' in sheet.col_values(1)[row_start:] :
            row_info = sheet.col_values(1).index('',row_start,sheet.nrows)+1
            if sheet.col_values(0)[row_info]:
                for row_no in range(row_info, sheet.nrows) : 
                    info.append(sheet.cell_value(row_no,0))

   
    def __next__(self):
        row = self.sheet.row(next(self.row_range))
        if row is None:
            raise StopIteration()
        series = self.build_series(row)
        if series is None:
            raise StopIteration()            
        return(series) 
                                       
                                           
    def build_series(self,row):  
        dimensions = {}
        series = {}
        series_value = [] 
        #TO DO: Syncronize for all series
        series_name = row[1].value + self.frequency 
        series_key = 'BEA.' + self.sheet.col(0)[0].value + '; ' + row[1].value
        #print(row[2].value)
        #print(row[1].value)
        dimensions['concept'] = self.dimension_list.update_entry('concept',row[2].value,row[1].value)  
        dimensions['line'] = self.dimension_list.update_entry('line',str(row[0].value),str(row[0].value))
        for r in range(3, len(row)):
            series_value.append(str(row[r].value))  
        #release_dates = [self.release_date for v in series_value] 
        series['values'] = series_value                
        series['provider'] = self.provider_name       
        series['datasetCode'] = self.dataset_code
        series['name'] = series_name
        series['key'] = series_key
        series['startDate'] = self.start_date
        series['endDate'] = self.end_date  
        series['lastUpdate'] = self.release_date
        series['dimensions'] = dimensions
        series['frequency'] = self.frequency
        series['attributes'] = {}
        return(series)

      

if __name__ == "__main__":
    w = BEA()
    w.provider.update_database()
    w.upsert_categories()
    w.upsert_nipa()
    

