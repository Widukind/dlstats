# -*- coding: utf-8 -*-
"""
Created on Thu Sep 10 11:35:26 2015

@author: salimeh
"""

from datetime import datetime
import zipfile
import logging
from pprint import pprint

import xlrd
import pandas

from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats.utils import Downloader, get_ordinal_from_period
from dlstats import constants

VERSION = 1

logger = logging.getLogger(__name__)

"""
Chaque Section est une categories contenant des datasets !!!
Chaque sheet dans un excel contient 1 dataset !
"""
CATEGORIES = {
    "nipa": {
        "name": "NIPA",
        #"url": "http://www.bea.gov/national/nipaweb/GetCSV.asp?GetWhat=SS_Data/SectionAll_xls.zip&Section=11",
        "doc_href": None
    },
    "nipa-section1": {
        "name": "NIPA - Section 1 - Domestic Product and Income",
        "parent": "nipa",
        "url": "http://www.bea.gov/national/nipaweb/GetCSV.asp?GetWhat=SS_Data/Section1All_xls.zip&Section=2",
        "filename": "nipa-section1.xls.zip",
        "doc_href": None
    },
    "nipa-section2": {
        "name": "NIPA - Section 2 - Personal Income and Outlays",
        "parent": "nipa",
        "url": "http://www.bea.gov/national/nipaweb/GetCSV.asp?GetWhat=SS_Data/Section2All_xls.zip&Section=3",
        "filename": "nipa-section2.xls.zip",
        "doc_href": None
    },
    "nipa-section3": {
        "name": "NIPA - Section 3 - Government Current Receipts and Expenditures",
        "parent": "nipa",
        "url": "http://www.bea.gov/national/nipaweb/GetCSV.asp?GetWhat=SS_Data/Section3All_xls.zip&Section=4",
        "filename": "nipa-section3.xls.zip",
        "doc_href": None
    },
    "nipa-section4": {
        "name": "NIPA - Section 4 - Foreign Transactions",
        "parent": "nipa",
        "url": "http://www.bea.gov/national/nipaweb/GetCSV.asp?GetWhat=SS_Data/Section4All_xls.zip&Section=5",
        "filename": "nipa-section4.xls.zip",
        "doc_href": None
    },
    "nipa-section5": {
        "name": "NIPA - Section 5 - Saving and Investment",
        "parent": "nipa",
        "url": "http://www.bea.gov/national/nipaweb/GetCSV.asp?GetWhat=SS_Data/Section5All_xls.zip&Section=6",
        "filename": "nipa-section5.xls.zip",
        "doc_href": None
    },
    "nipa-section6": {
        "name": "NIPA - Section 6 - Income and Employment by Industry",
        "parent": "nipa",
        "url": "http://www.bea.gov/national/nipaweb/GetCSV.asp?GetWhat=SS_Data/Section6All_xls.zip&Section=7",
        "filename": "nipa-section6.xls.zip",
        "doc_href": None
    },
    "nipa-section7": {
        "name": "NIPA - Section 7 - Supplemental Tables",
        "parent": "nipa",
        "url": "http://www.bea.gov/national/nipaweb/GetCSV.asp?GetWhat=SS_Data/Section7All_xls.zip&Section=8",
        "filename": "nipa-section7.xls.zip",
        "doc_href": None
    },
}

class BEA(Fetcher):
    
    def __init__(self, **kwargs):
        super().__init__(provider_name='BEA', version=VERSION, **kwargs)
         
        self.provider = Providers(name=self.provider_name ,
                                  long_name='Bureau of Economic Analysis',
                                  region='USA',
                                  version=VERSION,
                                  website='www.bea.gov/',
                                  fetcher=self)
        #self.urls= {'National Data_GDP & Personal Income' :'http://www.bea.gov//national/nipaweb/GetCSV.asp?GetWhat=SS_Data/SectionAll_xls.zip&Section=11',
        #            'National Data_Fixed Assets': 'http://www.bea.gov//national/FA2004/GetCSV.asp?GetWhat=SS_Data/SectionAll_xls.zip&Section=11', 
        #            'Industry data_GDP by industry_Q': 'http://www.bea.gov//industry/iTables%20Static%20Files/AllTablesQTR.zip',
        #            'Industry data_GDP by industry_A': 'http://www.bea.gov//industry/iTables%20Static%20Files/AllTables.zip',
        #            'International transactions(ITA)': 'http://www.bea.gov/international/bp_web/startDownload.cfm?dlSelect=tables/XLSNEW/ITA-XLS.zip',
        #            'International services': 'http://www.bea.gov/international/bp_web/startDownload.cfm?dlSelect=tables/XLSNEW/IntlServ-XLS.zip',
        #            'International investment position(IIP)': 'http://www.bea.gov/international/bp_web/startDownload.cfm?dlSelect=tables/XLSNEW/IIP-XLS.zip'}
        
        self.urls = ['http://www.bea.gov/national/nipaweb/GetCSV.asp?GetWhat=SS_Data/SectionAll_xls.zip&Section=11',
                    #'http://www.bea.gov/national/FA2004/GetCSV.asp?GetWhat=SS_Data/SectionAll_xls.zip&Section=11'
                    ]
        #                    'http://www.bea.gov//industry/iTables%20Static%20Files/AllTablesQTR.zip',
        #                    'http://www.bea.gov//industry/iTables%20Static%20Files/AllTables.zip',
        #                    'http://www.bea.gov/international/bp_web/startDownload.cfm?dlSelect=tables/XLSNEW/ITA-XLS.zip',
        #                    'http://www.bea.gov/international/bp_web/startDownload.cfm?dlSelect=tables/XLSNEW/IntlServ-XLS.zip',
        #                    'http://www.bea.gov/international/bp_web/startDownload.cfm?dlSelect=tables/XLSNEW/IIP-XLS.zip']
        
        self._spreadsheets = None
        
        self._datasets_settings = None
        self._current_urls = {}
    
    @property
    def spreadsheets(self):
        
        if self._spreadsheets:
            return self._spreadsheets
        
        self._spreadsheets = {}
        
        for url in self.urls:
            
            #response = urllib.request.urlopen(self.url)
            
            download = Downloader(url=url,
                                  filename="SectionAll_xls.zip",
                                  store_filepath=self.store_path)
            filepath = download.get_filepath()        

            zipfile_ = zipfile.ZipFile(filepath)
            #zipfile_ = zipfile.ZipFile(io.BytesIO(response.read()))
            
            for section in zipfile_.namelist():
                
                if not section in ['Iip_PrevT3a.xls', 'Iip_PrevT3b.xls', 'Iip_PrevT3c.xls']:
                    
                    file_contents = zipfile_.read(section)
                    
                    excel_book = xlrd.open_workbook(file_contents=file_contents)
                    
                    for sheet_name in excel_book.sheet_names():
                        
                        sheet = excel_book.sheet_by_name(sheet_name)
                        
                        if  sheet_name != 'Contents':
                            dataset_code = sheet_name.replace(' ','_')
                            self._spreadsheets[dataset_code] = sheet
                                
        return self._spreadsheets

    def _get_sheet(self, url, filename, sheet_name):
        
        if url in self._current_urls:
            filepath = self._current_urls[url]
        else:
            download = Downloader(url=url,
                                  filename=filename,
                                  store_filepath=self.store_path,
                                  use_existing_file=self.use_existing_file)
            
            filepath = download.get_filepath()
            self._current_urls[url] = filepath        

        zipfile_ = zipfile.ZipFile(filepath)
        section = zipfile_.namelist()[0]
                
        file_contents = zipfile_.read(section)
                    
        excel_book = xlrd.open_workbook(file_contents=file_contents)
                    
        return excel_book.sheet_by_name(sheet_name)
        
    def upsert_dataset(self, dataset_code):
        
        settings = self._get_datasets_settings()[dataset_code]
        
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code, 
                           name=settings["name"], 
                           doc_href='http://www.bea.gov', 
                           fetcher=self)
        
        url = settings["metadata"]["url"]
        filename = settings["metadata"]["filename"]
        sheet_name = settings["metadata"]["sheet_name"]

        sheet = self._get_sheet(url, filename, sheet_name)
        bea_data = BeaData(dataset, url=url, sheet=sheet)
        
        dataset.last_update = bea_data.release_date
        dataset.series.data_iterator = bea_data
        
        return dataset.update_database()

    def _get_datasets_settings(self):
        if not self._datasets_settings:
            self._datasets_settings = dict([(d["dataset_code"], d) for d in self.datasets_list()])
        return self._datasets_settings
    
    def load_datasets_first(self):
        self._get_datasets_settings()
        return super().load_datasets_first()

    def _get_frequency(self, sheet_name):
        if 'Qtr' in sheet_name :
            return "Quarterly", "Q" 
        elif 'Ann'  in sheet_name or 'Annual' in sheet_name:
            return "Annually", "A"
        elif 'Month'in sheet_name:
            return "Monthly", "M"
        
        return None, None

    def build_data_tree(self):
        
        categories = []
        
        for category_code, values in CATEGORIES.items():
            cat = {
                "category_code": category_code,
                "name": values["name"],
                "doc_href": values["doc_href"],
                "datasets": []
            }
            categories.append(cat)

        for category_code, category in CATEGORIES.items():
            
            if not "url" in category:
                continue
            
            url = category["url"]
            filename = category["filename"]
            
            download = Downloader(url=url,
                                  filename=filename,
                                  store_filepath=self.store_path,
                                  use_existing_file=self.use_existing_file)
            filepath = download.get_filepath()        

            self._current_urls[url] = filepath
            
            zipfile_ = zipfile.ZipFile(filepath)
            
            for section in zipfile_.namelist():
                
                if section in ['Iip_PrevT3a.xls', 'Iip_PrevT3b.xls', 'Iip_PrevT3c.xls']:
                    continue

                file_contents = zipfile_.read(section)
                excel_book = xlrd.open_workbook(file_contents=file_contents)
    
                try:                    
                    sheet = excel_book.sheet_by_name('Contents')
                    
                    cat = {
                        "category_code": category_code,
                        "name": category["name"],
                        "parent": category.get("parent"),
                        "all_parents": [category.get("parent")],
                        "doc_href": None,
                        "datasets": []
                    }

                    dataset_base_names = {}

                    for i, row in enumerate(sheet.col(1)):
                        if i < 10:
                            continue
                        cell_row = sheet.row(i)
                        if cell_row[1].value != '':
                            dataset_code = cell_row[1].value
                            dataset_name = cell_row[2].value
                            
                            dataset_base_names[dataset_code] = dataset_name
                            
                    for sheet_name in excel_book.sheet_names():
                        _dataset_code = sheet_name.split()[0]
                        
                        if not _dataset_code in dataset_base_names:
                            continue
                        
                        _dataset_name = dataset_base_names[_dataset_code]
                        
                        frequency_name, frequency_code = self._get_frequency(sheet_name)
                        
                        if not frequency_name:
                            msg = "not frequency name for sheet[%s] - url[%s] - filename[%s]" % (sheet_name, url, filename) 
                            logger.critical(msg)
                            raise Exception(msg)
                        
                        dataset_code = "%s-%s" % (_dataset_code, frequency_code.lower()) 
                        dataset_name = "%s - %s" % (_dataset_name, frequency_name)

                        cat["datasets"].append({
                            "name": dataset_name, 
                            "dataset_code": dataset_code,
                            "last_update": None, 
                            "metadata": {
                                "url": url, 
                                "filename": filename,
                                "sheet_name": sheet_name
                            }
                        })

                    categories.append(cat)
                    
                except Exception as err:
                    logger.error(str(err))
                
        return categories
        
class BeaData(SeriesIterator):
    
    def __init__(self, dataset, url=None, sheet=None):
        super().__init__(dataset)
        
        self.url = url
        self.sheet = sheet
        
        self.dataset.dimension_keys = ['concept']
        
        if not "concept" in self.dataset.concepts:
            self.dataset.concepts["concept"] = "Concept"

        if not "concept" in self.dataset.codelists:
            self.dataset.codelists["concept"] = {}
        
        cell_value = self.sheet.cell_value(2,0) #Annual data from 1969 To 2015
        self.frequency = None

        #retrieve frequency from url        
        #if 'AllTablesQTR' in url :
        #    self.frequency = 'Q'
        #elif 'AllTables.' in url : 
        #    self.frequency = 'A'
        
        if 'Qtr' in self.sheet.name :
            self.frequency = 'Q' 
        elif 'Ann'  in self.sheet.name or 'Annual' in self.sheet.name:
            self.frequency = 'A'
        elif 'Month'in self.sheet.name:
            self.frequency = 'M'
        
        if self.frequency is None:
            raise Exception(dataset.name + " " + self.sheet.name + " (" + url + "): frequency can't be found")  
        
        if 'Section' in  url :
            release_datesheet = sheet.cell_value(4,0)[15:] #April 28, 2016 
        else :
            release_datesheet = sheet.cell_value(3,0)[14:] 
        if 'ITA-XLS' in url or 'IIP-XLS' in url :
            release_datesheet = sheet.cell_value(3,0)[14:].split('-')[0]
            
        self.years = [int(s) for s in cell_value.split() if s.isdigit()] #[1969, 2015]

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
        
        self.row_ranges = list(iter(range(row_start, sheet.nrows)))
        
        row_notes = self.sheet.row_values(1)
        if row_notes and len(row_notes[0].strip()) > 0:
            self.dataset.notes = row_notes[0].strip()

        self.keys = []
        self.rows = self._get_datas()
        
    def _get_datas(self):
        try:
            for row_num in self.row_ranges:

                row = self.sheet.row_values(row_num)
                
                key = row[2]

                # skip lines without key or with ZZZZZZx key
                if len(key.replace(' ','')) == 0 or key[0:6] == 'ZZZZZZ':
                    continue
                elif key in self.keys:
                    continue
                else:
                    self.keys.append(key)
                
                yield row, None
        finally:
            try:
                self.sheet.book.release_resources()
            except Exception as err:
                logger.error(str(err))
            
    def build_series(self, row):
        dimensions = {}
        series = {}
        series_values = [] 

        series_name = "%s - %s" % (row[1].strip(), constants.FREQUENCIES_DICT[self.frequency]) 
        series_key = row[2]

        dimensions['concept'] = self.dimension_list.update_entry('concept', row[2], row[1].strip())  
        #dimensions['line'] = self.dimension_list.update_entry('line', str(row[0]), str(row[0]))
        if not dimensions["concept"] in self.dataset.codelists["concept"]:
            self.dataset.codelists["concept"][dimensions["concept"]] = dimensions["concept"]

        start_date = pandas.Period(self.years[0], freq=self.frequency)
        end_date = pandas.Period(self.years[1], freq=self.frequency)

        series['provider_name'] = self.provider_name       
        series['dataset_code'] = self.dataset_code
        series['name'] = series_name
        series['key'] = series_key
        series['start_date'] = start_date.ordinal
        series['end_date'] = end_date.ordinal
        series['last_update'] = self.release_date
        series['dimensions'] = dimensions
        series['frequency'] = self.frequency
        series['attributes'] = {}
        
        for v in row[3:]:
            value = {
                'attributes': None,
                'release_date': self.release_date,
                'ordinal': start_date.ordinal,
                'period': str(start_date),
                'value': str(v)
            }
            series_values.append(value)
            start_date += 1

        series['values'] = series_values
        
        return series

