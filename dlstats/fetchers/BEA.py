# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 17:04:10 2015
It is the first draft of fetching the National account data from U.S.
Bureau of Economic Analysis (BEA). 
Work In Progress

"""

from dlstats.fetchers._skeleton import Skeleton, Category, Series, BulkSeries, Dataset, Provider
import urllib
import xlrd
import codecs
import datetime
import pandas
import pprint


class BEA(Skeleton):
    def __init__(self):
        super().__init__() 
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
            if 'Contents' in self.readers[0].sheet_names():
                for column_index in range (1,
                        self.readers[0].sheet_by_name('Contents').ncols) :
                for count in self.readers[0].sheet_by_name('Contents').ncols
                Table Of Contents
                label_column_list = self.readers[0].sheet_by_name('Contents').col(0)[2:]
                print(label_column_list)
            if '10105 Ann' in self.readers[0].sheet_names():
                print('true')

if __name__ == "__main__":
    import BEA
    w = BEA.BEA()
    w.upsert_dataset('BEA')    