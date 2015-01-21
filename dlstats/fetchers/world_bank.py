from dlstats.fetchers._skeleton import Skeleton, Category, Series, Dataset
import io
import zipfile
import urllib.request
import xlrd
import os
import time
import pandas
import logging
from datetime import datetime
import pprint

class WorldBank(Skeleton):
    def __init__(self):
        super().__init__() 
        self.lgr = logging.getLogger('WorldBank')
        self.lgr.setLevel(logging.DEBUG)
        self.fh = logging.FileHandler('WorldBank.log')
        self.fh.setLevel(logging.DEBUG)
        self.frmt = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.fh.setFormatter(self.frmt)
        self.lgr.addHandler(self.fh)
        #self.lgr.info('Retrieving %s', self.configuration['Fetchers']['Eurostat']['url_table_of_contents'])
        self.lgr.debug('debug message')
        self.lgr.info('info message')
        self.lgr.warn('warn message')
        self.lgr.error('error message')
        self.lgr.critical('critical message')
    def update_selected_database(self):
        response = urllib.request.urlopen(
                   'http://siteresources.worldbank.org/INTPROSPECTS/Resources/' +\
                   'GemDataEXTR.zip')
        zipfile_ = zipfile.ZipFile(io.BytesIO(response.read()))
        excelfile = {name : zipfile_.read(name) for name in zipfile_.namelist()}
        series_ = []
        dataset_ = []
        categories_ = []
        for name_series in excelfile.keys():
            excel_file = xlrd.open_workbook(file_contents = excelfile[name_series])
            for sheet_name in excel_file.sheet_names():
                if sheet_name not in ['Sheet1','Sheet2','Sheet3']: 
                    label_row_list = excel_file.sheet_by_name(sheet_name).col(0)
                    label_column_list = excel_file.sheet_by_name(sheet_name).row(0)
                    for column_index in range (
                        excel_file.sheet_by_name(sheet_name).ncols):
                        series = {}
                        dataset = {}
                        categories = {}
                        value = []
                        value_dataset_ = []
                        column = excel_file.sheet_by_name(sheet_name).col(column_index)
                        for info in zipfile_.infolist():
                            if info.filename == name_series:
                                released_dates = datetime(*info.date_time[0:6])
                        if column[0].value not in ('obs') :
                            if name_series[:-5] in ['Commodity Prices']:
                                dimensions_int = {'name':'Commodity Prices',
                                'value':column[0].value} 
                            if name_series[:-5] not in ['Commodity Prices']:    
                                dimensions_int = {'name':'country',
                                'value':column[0].value} 
                            column_value = column[1:-1]
                            for cell_value in column_value :
                                value.append(cell_value.value)

                            if sheet_name in ('annual') :
                                start_date_b = str(
                                    label_row_list[3].value)[:-2]
                                end_date_b = str(
                                    label_row_list[-1].value)[:-2] 
                                    
                            if sheet_name not in ('annual') :       
                                start_date = str(label_row_list[3].value)
                                start_date_b = start_date.replace('M','-') 
                                end_date = str(label_row_list[-1].value)
                                end_date_b = end_date.replace('M','-') 
                                        
                            if sheet_name == 'annual':    
                                frequency = 'year'
                            if sheet_name == 'quarterly':    
                                frequency = 'quarter'
                            if sheet_name == 'monthly':    
                                frequency = 'month'
                            if sheet_name == 'daily':    
                                frequency = 'day'       
                           # series['provider'] = 'WorldBank' 
                            Key = name_series[:-5].replace(' ',
                                                '_').replace(',', '')+'.'+column[0].value
                            document = Dataset(provider = 'WorldBank', name = name_series[:-5] , datasetCode = Key, attributeList = '', dimensionList = '', docHerf = '',lastUpdate = released_dates)
                            id = document.update_database()
       
    def categories_db(self):
        response = urllib.request.urlopen(
                   'http://siteresources.worldbank.org/INTPROSPECTS/Resources/' +\
                   'GemDataEXTR.zip')
        zipfile_ = zipfile.ZipFile(io.BytesIO(response.read()))
        excelfile = {name : zipfile_.read(name) for name in zipfile_.namelist()}
        series_ = []
        dataset_ = []
        categories_ = []
        for name_series in excelfile.keys():
            excel_file = xlrd.open_workbook(file_contents = excelfile[name_series])
            for sheet_name in excel_file.sheet_names():
                if sheet_name not in ['Sheet1','Sheet2','Sheet3']: 
                    label_row_list = excel_file.sheet_by_name(sheet_name).col(0)
                    label_column_list = excel_file.sheet_by_name(sheet_name).row(0)
                    for column_index in range (
                        excel_file.sheet_by_name(sheet_name).ncols):
                        series = {}
                        dataset = {}
                        categories = {}
                        value = []
                        value_dataset_ = []
                        column = excel_file.sheet_by_name(sheet_name).col(column_index)
                        for info in zipfile_.infolist():
                            if info.filename == name_series:
                                released_dates = datetime(*info.date_time[0:6])
                        if column[0].value not in ('obs') :
                            if name_series[:-5] in ['Commodity Prices']:
                                dimensions_int = {'name':'Commodity Prices',
                                'value':column[0].value} 
                            if name_series[:-5] not in ['Commodity Prices']:    
                                dimensions_int = {'name':'country',
                                'value':column[0].value} 
                            column_value = column[1:-1]
                            for cell_value in column_value :
                                value.append(cell_value.value)
                            if sheet_name in ('annual') :
                                start_date_b = str(
                                    label_row_list[3].value)[:-2]
                                end_date_b = str(
                                    label_row_list[-1].value)[:-2] 
                                    
                            if sheet_name not in ('annual') :       
                                start_date = str(label_row_list[3].value)
                                start_date_b = start_date.replace('M','-') 
                                end_date = str(label_row_list[-1].value)
                                end_date_b = end_date.replace('M','-') 
                                        
                            if sheet_name == 'annual':    
                                frequency = 'year'
                            if sheet_name == 'quarterly':    
                                frequency = 'quarter'
                            if sheet_name == 'monthly':    
                                frequency = 'month'
                            if sheet_name == 'daily':    
                                frequency = 'day'         
                            Key = name_series[:-5].replace(' ',
                                                '_').replace(',', '')+'.'+column[0].value
     
                            document = Category(provider = 'WorldBank', name = name_series[:-5] , datasetCode = Key,lastUpdate = released_dates , docHref ='')
                            _id = document.update_database()
                                  
    def update_a_series(self):
        response = urllib.request.urlopen(
                   'http://siteresources.worldbank.org/INTPROSPECTS/Resources/' +\
                   'GemDataEXTR.zip')
        zipfile_ = zipfile.ZipFile(io.BytesIO(response.read()))
        excelfile = {name : zipfile_.read(name) for name in zipfile_.namelist()}
        series_ = []
        dataset_ = []
        categories_ = []
        start_date_b_test= []
        end_date_b_test= []
        fre_fre_fre = []
        test = []
        test1 = []
        for name_series in excelfile.keys():
            excel_file = xlrd.open_workbook(file_contents = excelfile[name_series])
            for sheet_name in excel_file.sheet_names():
                
                if sheet_name not in ['Sheet1','Sheet2','Sheet3','Sheet4']: 
                    label_row_list = excel_file.sheet_by_name(sheet_name).col(0)
                    label_column_list = excel_file.sheet_by_name(sheet_name).row(0)
                    for column_index in range (
                        excel_file.sheet_by_name(sheet_name).ncols):
                        series = {}
                        dataset = {}
                        categories = {}
                        value = []
                        value_dataset_ = []
                        released_dates= []
                        
                        column = excel_file.sheet_by_name(sheet_name).col(column_index)
                        for info in zipfile_.infolist():
                            if info.filename == name_series:
                                released_dates.append(datetime(*info.date_time[0:6]))
                        if column[0].value not in ('obs') :
                            if name_series[:-5] in ['Commodity Prices']:
                                dimensions_int = {'name':'Commodity Prices',
                                'value':column[0].value} 
                            if name_series[:-5] not in ['Commodity Prices']:    
                                dimensions_int = {'name':'country',
                                'value':column[0].value} 
                            column_value = column[1:-1]
                            for cell_value in column_value :
                                value.append(str(cell_value.value))
                            if sheet_name in ('annual') :
                                start_date_b = str(
                                    label_row_list[3].value)[:-2]

                                end_date_b = str(
                                    label_row_list[-1].value)[:-2] 
 
                                    
                            if sheet_name not in ('annual') :       
                                start_date = str(label_row_list[3].value)
                                start_date_b = start_date.replace('M','-') 
                                end_date = str(label_row_list[-1].value)
                                end_date_b = end_date.replace('M','-') 
                                        
                            if sheet_name == 'annual':    
                                frequency = 'year'
                            if sheet_name == 'quarterly':    
                                frequency = 'quarter'
                            if sheet_name == 'monthly':    
                                frequency = 'month'
                            if sheet_name == 'daily':    
                                frequency = 'day'                                             
                            Key = name_series[:-5].replace(' ',
                                                '_').replace(',', '')+'.'+column[0].value
                        
                            #period_index = pandas.period_range(start_date_b, end_date_b ,freq = frequency)                                                 
                            document = Series(provider = 'WorldBank', name = name_series[:-5] , key = Key, datasetCode = Key, values = value, period_index = pandas.period_range(start_date_b, end_date_b , freq = frequency), releaseDates = released_dates ,frequency=frequency , dimensions =  dimensions_int)
                            _id = document.update_database(key = Key)     
