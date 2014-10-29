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
            'http://siteresources.worldbank.org/INTPROSPECTS/Resources/'+\
            'GemDataEXTR.zip')
        zipfile_ = zipfile.ZipFile(io.BytesIO(response.read()))

        excelfile = {name : zipfile_.read(name) for name in zipfile_.namelist()}

        excel_files_list = []
        series_ = []
        series_sheet_name = {}
        for name_series in excelfile.keys():
        #slicing the string .xl
            excel_file =xlrd.open_workbook(
                file_contents = excelfile[name_series])
            for sheet_name in excel_file.sheet_names():
                if sheet_name not in ['Sheet1','Sheet2','Sheet3']: 
                    label_row_list = excel_file.sheet_by_name(
                        sheet_name).col(0)
                    label_column_list = excel_file.sheet_by_name(
                        sheet_name).row(0)
                    for column_index in range (excel_file.sheet_by_name(
                        sheet_name).ncols):
                        series = {}
                        value = []
                        column = excel_file.sheet_by_name(sheet_name).col(
                            column_index)
                        series['dimensions'] = {'name':'country',
                                                'value':column[0].value} 
                        column_value = column[1:-1]
                        for cell_value in column_value :
                            value.append(cell_value.value)
                        series['value'] = value
                        series['name'] = name_series[:-5] 
                        series['start_date'] = label_row_list[3].value 
                        series['end_date'] = label_row_list[-1].value
                        if sheet_name == 'annual':
                                 frequency = 'a'
                        if sheet_name == 'monthly':
                                 frequency = 'm'
                        if sheet_name == 'daily':
                                 frequency = 'd'
                        series['frequency'] = frequency
                        series['provider'] = 'World Bank'
                        series_.append(series)

            #bulk = self.db.series.initialize_unordered_bulk_op()
            for series in series_:
                self.db.series.insert(series)



                        
                    
                   
                    #series['dimensions'] = {'name': 'country' , 'value': label_column_list[1:len(label_column_list)]}


            
