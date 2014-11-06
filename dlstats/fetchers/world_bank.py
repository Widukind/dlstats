from dlstats.fetchers._skeleton import Skeleton
import io
import zipfile
import urllib.request
import xlrd

class WorldBank(Skeleton):
    def __init__(self):
        super().__init__()
    def upsert_a_series(self):
        response = urllib.request.urlopen(
                   'http://siteresources.worldbank.org/INTPROSPECTS/Resources/' +\
                    'GemDataEXTR.zip')
        zipfile_ = zipfile.ZipFile(io.BytesIO(response.read()))
        excelfile = {name : zipfile_.read(name) for name in zipfile_.namelist()}
        excel_files_list = []
        series_ = []
        dataset_ = []
        for name_series in excelfile.keys():
            excel_file =xlrd.open_workbook(file_contents = excelfile[name_series])
            for sheet_name in excel_file.sheet_names():
                if sheet_name not in ['Sheet1','Sheet2','Sheet3']: 
                    label_row_list = excel_file.sheet_by_name(sheet_name).col(0)
                    label_column_list = excel_file.sheet_by_name(sheet_name).row(0)
                    for column_index in range (
                        excel_file.sheet_by_name(sheet_name).ncols):
                        series = {}
                        dataset = {}
                        value = []
                        value_dataset_ = []
                        column = excel_file.sheet_by_name(sheet_name).col(column_index)
                        if column[0].value not in ('obs') :
                            if name_series[:-5] in ['Commodity Prices']:
                                series['dimensions'] = {'name':'Commodity Prices',
                                'value':column[0].value} 
                            if name_series[:-5] not in ['Commodity Prices']:    
                                series['dimensions'] = {'name':'country',
                                'value':column[0].value} 
                            column_value = column[1:-1]
                            for cell_value in column_value :
                                value.append(cell_value.value)
                            series['value'] = value
                            series['name'] = name_series[:-5] 
                            if sheet_name in ('annual') :
                                series['start_date'] = str(
                                    label_row_list[3].value)[:-2]
                                series['end_date'] = str(
                                    label_row_list[-1].value)[:-2] 
                            if sheet_name not in ('annual') :       
                                series['start_date'] = str(label_row_list[3].value) 
                                series['end_date'] = str(label_row_list[-1].value)  
                                        
                            if sheet_name == 'annual':    
                                frequency = 'a'
                            if sheet_name == 'monthly':    
                                frequency = 'm'
                            if sheet_name == 'daily':    
                                frequency = 'd'              
                            series['frequency'] = frequency       
                            series['provider'] = 'WorldBank' 
                            series['key'] = name_series[:-5].replace(' ',
                                                '_').replace(',', '')+'.'+column[0].value
                            series_.append(series)
                            dataset['provider'] = 'WorldBank'
                            dataset['name'] = name_series[:-5]
                            value_dataset_ = []
                            for value_dataset in label_column_list :
                                value_dataset_.append(value_dataset.value) 
                            if name_series[:-5] in ['Commodity Prices']:
                                dataset['dimensions'] = {'name':'Commodity Prices'
                                                         ,'value': value_dataset_[1:-1]} 
                            if name_series[:-5] not in ['Commodity Prices']:        
                                dataset['dimension_list'] = {'name': 'country' , 
                                                             'value': value_dataset_[1:-1]}
                            dataset['dataset_code'] = 'worldBank_'+name_series[:-5]
                            dataset_.append(dataset)
				               #bulk = self.db.series.initialize_unordered_bulk_op()
        for series in series_:
#            print(series)
            self.db.series.insert(series)
