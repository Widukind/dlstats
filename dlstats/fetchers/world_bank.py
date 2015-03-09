from dlstats.fetchers._skeleton import Skeleton, Category, Series, BulkSeries, Dataset, Provider
import io
import zipfile
import urllib.request
import xlrd
import datetime
import pandas
import random
import pprint

class WorldBank(Skeleton):
    def __init__(self):
        super().__init__()         
        self.response = urllib.request.urlopen(
                   'http://siteresources.worldbank.org/INTPROSPECTS/Resources/' +\
#            'http://localhost:8800/worldbank/'+
            'GemDataEXTR.zip')
       
        #Getting released date from headers of the Zipfile
        self.releaseDates_ = self.response.getheaders()[1][1] 
        self.releaseDates = datetime.datetime.strptime(self.releaseDates_[5:], 
        "%d %b %Y %H:%M:%S GMT")
       
        self.zipfile_ = zipfile.ZipFile(io.BytesIO(self.response.read()))  
        self.excelfile_ = {'GemDataEXTR':{name : self.zipfile_.read(name) for name in
                           self.zipfile_.namelist()}}
        self.provider = Provider(name='World Bank',website='http://www.worldbank.org/')
                           
    def upsert_dataset(self, datasetCode):
        
        if datasetCode=='GEM':
            excelfile = self.excelfile_['GemDataEXTR']
        else:
            raise Exception("The name of dataset was not entered!")
            
        def dictionary_union(*dictionaries):
            keys = [list(dictionary.keys()) for dictionary in dictionaries]
            keys = [item for items in keys for item in items]
            merged_dictionary = {}
            for key in keys:
                values=[]
                for dictionary in dictionaries:
                    if key in dictionary.keys():
                        values.extend(dictionary[key])
                merged_dictionary[key] = list(set(values))
            return merged_dictionary                              
        #List of the name of the excel files
        concept_list=[]
        [concept_list.append(key[:-5]) for key in excelfile.keys()]
        print(concept_list)
        dimensionList_ = []       
        for name_series in excelfile.keys():
            #Saving the Last modified date of each excel file
            index_name_series = list(excelfile.keys()).index(name_series)
            last_Update = datetime.datetime(*self.zipfile_.infolist()[index_name_series].date_time[0:6])
            excel_file = xlrd.open_workbook(file_contents = excelfile[name_series])
            for sheet_name in excel_file.sheet_names():
                if sheet_name not in ['Sheet1','Sheet2','Sheet3','Sheet4',
                'Feuille1','Feuille2','Feuille3','Feuille4']: 
                    label_column_list = excel_file.sheet_by_name(sheet_name).col(0)[2:]                    
                    #List of countries or comodities
                    dimensionList=[]
                    countries_list=[]
                    commodity_prices_list = []
                    if name_series[:-5] not in ['Commodity Prices']:
                        [countries_list.append((excel_file.
                        sheet_by_name(sheet_name).col(column_index))[0].value) for
                        column_index in range (1, excel_file.sheet_by_name
                        (sheet_name).ncols)]
                    if name_series[:-5] in ['Commodity Prices']: 
                        [commodity_prices_list.append((excel_file.
                        sheet_by_name(sheet_name).col(column_index))[0].value) for
                        column_index in range (1, excel_file.sheet_by_name
                        (sheet_name).ncols)]
                    dimensionList_interm=[{'concept': concept_list},
                                   {'country': countries_list}
                                   ,{'Commodity Prices': commodity_prices_list}]
                                   
                    dimensionList_.extend(dimensionList_interm)             
        dimensionList = dictionary_union(*dimensionList_)  
        print(dimensionList) 
        pprint.pprint(dimensionList)         
        document = Dataset(provider = 'WorldBank', 
                           name = 'GEM' ,
                           datasetCode = 'GEM', lastUpdate = self.releaseDates,
                           dimensionList = dimensionList )
        id = document.update_database()
        return self.update_series('GEM', dimensionList)  
       
    def upsert_categories(self):
        document = Category(provider = 'WorldBank', 
                            name = 'GEM' , 
                            categoryCode ='GEM')
        return document.update_database()

    def update_series(self,datasetCode,dimensionList):
        if datasetCode == 'GEM':
            excelfile = self.excelfile_['GemDataEXTR']
        else:
            raise Exception("The name of dataset was not entered!")            
        for name_series in excelfile.keys():
            index_name_series = list(excelfile.keys()).index(name_series)
            S = list(self.zipfile_.infolist()[index_name_series].date_time[0:3])
            last_Update = [datetime.datetime(S[0],S[1],S[2])]
            excel_file = xlrd.open_workbook(file_contents = excelfile[name_series])
            for sheet_name in excel_file.sheet_names():
                if sheet_name not in ['Sheet1','Sheet2','Sheet3','Sheet4','Feuille1',
                                      'Feuille2','Feuille3','Feuille4']: 
                    label_column_list = excel_file.sheet_by_name(sheet_name).col(0)[2:]
                    for column_index in range (1,
                        excel_file.sheet_by_name(sheet_name).ncols):
                        value = []
                        column = excel_file.sheet_by_name(sheet_name).col(column_index)
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
                            start_date_b = str(int(label_column_list[0].value))
                            end_date_b = str(int(label_column_list[-1].value))
                        if sheet_name not in ('annual') :       
                            start_date = str(label_column_list[0].value)
                            start_date_b = start_date.replace('M','-') 
                            end_date = str(label_column_list[-1].value)
                            end_date_b = end_date.replace('M','-')       
                        if sheet_name == 'annual':    
                            frequency = 'A'
                        if sheet_name == 'quarterly':    
                            frequency = 'q'
                        if sheet_name == 'monthly':    
                            frequency = 'm'
                        if sheet_name == 'daily':    
                            frequency = 'd' 'day'                                             
                        series_key = name_series[:-5].replace(' ',
                                            '_').replace(',', '')+'.'+\
                                            column[0].value+ str(random.random())                         
                        documents = BulkSeries(datasetCode,dimensionList)
                        documents.append(Series(provider='WorldBank',
                                            key= series_key,
                                            name=name_series[:-5],
                                            datasetCode= 'GEM',
                                            period_index=pandas.period_range
                                          (start_date_b, end_date_b , freq = frequency),
                                            values=value,
                                            releaseDates= [self.releaseDates],
                                            frequency=frequency,
                                            dimensions=dimensions_int))
        return(documents.bulk_update_database())
        
    def upsert_a_series(self,datasetCode):                              
        if datasetCode == 'GEM':
            excelfile = self.excelfile_['GemDataEXTR']
        else:
            raise Exception("The name of dataset was not entered!")
        for name_series in excelfile.keys():
            index_name_series = list(excelfile.keys()).index(name_series)
            S = list(self.zipfile_.infolist()[index_name_series].date_time[0:3])
            last_Update = [datetime.datetime(S[0],S[1],S[2])]
            excel_file = xlrd.open_workbook(file_contents = excelfile[name_series])
            for sheet_name in excel_file.sheet_names():
                if sheet_name not in ['Sheet1','Sheet2','Sheet3','Sheet4','Feuille1',
                                      'Feuille2','Feuille3','Feuille4']: 
                    label_column_list = excel_file.sheet_by_name(sheet_name).col(0)[2:]
                    for column_index in range (1,
                        excel_file.sheet_by_name(sheet_name).ncols):
                        value = []
                        column = excel_file.sheet_by_name(sheet_name).col(column_index)
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
                            start_date_b = str(int(label_column_list[0].value))
                            end_date_b = str(int(label_column_list[-1].value))
                        if sheet_name not in ('annual') :       
                            start_date = str(label_column_list[0].value)
                            start_date_b = start_date.replace('M','-') 
                            end_date = str(label_column_list[-1].value)
                            end_date_b = end_date.replace('M','-')       
                        if sheet_name == 'annual':    
                            frequency = 'A'
                        if sheet_name == 'quarterly':    
                            frequency = 'q'
                        if sheet_name == 'monthly':    
                            frequency = 'm'
                        if sheet_name == 'daily':    
                            frequency = 'd' 'day'                                             
                        series_key = name_series[:-5].replace(' ',
                                            '_').replace(',', '')+'.'+\
                                            column[0].value+ str(random.random())
                        document = Series(provider = 'WorldBank', 
                                          name = name_series[:-5] , key = series_key,
                                          datasetCode = 'GEM', values = value,
                                          period_index = pandas.period_range
                                          (start_date_b, end_date_b , freq = frequency)
                                          , releaseDates = self.releaseDates ,
                                          frequency=frequency , 
                                          dimensions =  dimensions_int)
                        _id = document.update_database(key=series_key)     

if __name__ == "__main__":
    import world_bank
    w = world_bank.WorldBank()
    w.upsert_dataset('GEM')
