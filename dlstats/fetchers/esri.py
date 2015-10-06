# -*- coding: utf-8 -*-
"""
Created on Fri Apr 24 09:53:55 2015


"""
from ._commons import Fetcher, Category, Series, BulkSeries, Dataset, Provider
import urllib
import codecs
import datetime
import pandas
import csv

import pprint


class esri(Fetcher):
    def __init__(self):
        super().__init__(provider_name='esri') 

#Amount
#http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2014/qe144_2/__icsFiles/afieldfile/2015/03/04/gaku-mk1442.csv
#http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2014/qe144_2/__icsFiles/afieldfile/2015/03/04/gaku-jg1442.csv
#http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2014/qe144_2/__icsFiles/afieldfile/2015/03/04/gaku-jk1442.csv
#http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2014/qe144_2/__icsFiles/afieldfile/2015/03/04/gaku-mfy1442.csv
#http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2014/qe144_2/__icsFiles/afieldfile/2015/03/04/gaku-mcy1442.csv
#http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2014/qe144_2/__icsFiles/afieldfile/2015/03/04/gaku-jfy1442.csv
#http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2014/qe144_2/__icsFiles/afieldfile/2015/03/04/gaku-jcy1442.csv
#Deflator
#http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2014/qe144_2/__icsFiles/afieldfile/2015/03/04/def-qg1442.csv
#http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2014/qe144_2/__icsFiles/afieldfile/2015/03/04/def-qk1442.csv
#http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2014/qe144_2/__icsFiles/afieldfile/2015/03/04/rdef-qg1442.csv
#http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2014/qe144_2/__icsFiles/afieldfile/2015/03/04/rdef-qk1442.csv

#read the url links for Amount parts
        self.url_amount = []
        url_list_amount = ['mg1442','mk1442','jg1442','jk1442','mfy1442','mcy1442','jfy1442','jcy1442']
        for index in url_list_amount:
            self.url_amount.append('http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2014/qe144_2/__icsFiles/afieldfile/2015/03/04/gaku-'+index+'.csv')
        #read the url links for deflator parts
        self.url_deflator = []
        url_list_deflator = ['def-qg1442','def-qk1442','rdef-qg1442','rdef-qk1442']
        for index in url_list_deflator :
            self.url_deflator.append('http://www.esri.cao.go.jp/jp/sna/data/data_list/sokuhou/files/2014/qe144_2/__icsFiles/afieldfile/2015/03/04/'+index+'.csv')
#            self.readers = []
#            self.releaseDates = []
#            self.files_ = {}
#            for self.url in self.urls:
#                self.response= urllib.request.urlopen(self.url)
#                self.readers.append(xlrd.open_workbook(file_contents = self.response.read()))
        self.provider = Provider(name='esri', website='http://www.cao.go.jp/index-e.html/')


    def upsert_dataset(self, datasetCode):
        if datasetCode=='esri':
            url = self.url_amount[0]
            response = urllib.request.urlopen(url)
            draft = csv.reader(codecs.iterdecode(response, 'latin-1'), delimiter=',')
            included_cols = [0]
            list_csv = []
            year_draft = []
            for rrow in draft:
                list_csv.append(rrow)
                year_draft.append(list(rrow[i] for i in included_cols))

            #Generating the year regarding to the standard format
            #year = year_draft[7:]
            #year_last = year[-4][0][:4]+'q4' 
            #period_index = pandas.period_range(year[0][0][:4], year_last , freq = 'quarterly')
            year = year_draft[7:]
            year_last = year[-4][0][:4]+'q4' 
            period_index = pandas.period_range(year[0][0][:4], year_last , freq = 'quarterly')
            list_csv[5][0] = 'year'

            # flatens the tree structure
            for i, j in enumerate(list_csv[6]):
                if j != '':
                    if list_csv[5][i] != '':
                        keep = list_csv[5][i]
                        list_csv[5][i] = list_csv[5][i] + '_' + list_csv[6][i]          
                    else :
                        list_csv[5][i] = keep + '_' + list_csv[6][i]
            
            dimensionList_content = []  
            response = urllib.request.urlopen(url)
            reader = csv.DictReader(codecs.iterdecode(response, 'latin-1'), fieldnames=list_csv[5] ,delimiter=',')
            for i in range(len(reader.fieldnames)):
                if reader.fieldnames[i]!='' and reader.fieldnames[i] != 'year'  :
                    dimensionList_content.append(reader.fieldnames[i])
            
            dimensionList = {'content':dimensionList_content}
                    
            datasetCode = 'esri'
            releaseDates =response.getheaders()[0][1] 
            lastUpdate = datetime.datetime.strptime(releaseDates[5:], "%d %b %Y %H:%M:%S GMT")

            document = Dataset(provider = 'esri', 
                       name = year_draft[1][0] ,
                       datasetCode = 'esri', lastUpdate = lastUpdate,
                       dimensionList = dimensionList, 
                       docHref = "http://www.cao.go.jp/index-e.html") 
            #print(document)           
            effective_dimension_list = self.update_series('esri', dimensionList)
            document.update_database()
            #print(effective_dimension_list)
            effective_dimension_list = self.update_series('esri', dimensionList)
            document.update_database()
            document.update_es_database(effective_dimension_list)
        else:
            raise Exception("The name of dataset was not entered!")        
    def update_series(self,datasetCode,dimensionList):
    
        if datasetCode=='esri':
            url = self.url_amount[0]
            response = urllib.request.urlopen(url)
            draft = csv.reader(codecs.iterdecode(response, 'latin-1'), delimiter=',')
            included_cols = [0]
            list_csv = []
            year_draft = []
            for rrow in draft:
                list_csv.append(rrow)
                year_draft.append(list(rrow[i] for i in included_cols))
                
            #Generating the year regarding to the standard format
            year = year_draft[7:]
            year_last = year[-4][0][:4]+'q4' 
            period_index = pandas.period_range(year[0][0][:4], year_last , freq = 'q')

            period_index = pandas.period_range(year[0][0][:4], year_last , freq = 'quarterly')
            list_csv[5][0] = 'year'
            
            # flatening the tree structure
            for i, j in enumerate(list_csv[6]):
                if j != '':
                    if list_csv[5][i] != '':
                        keep = list_csv[5][i]
                        list_csv[5][i] = list_csv[5][i] + '_' + list_csv[6][i]          
                    else :
                        list_csv[5][i] = keep + '_' + list_csv[6][i]
            

            #dimensionList_content = []  
            #response = urllib.request.urlopen(url)
            reader = csv.DictReader(codecs.iterdecode(response, 'latin-1'), fieldnames=list_csv[5] ,delimiter=',')
            #for i in range(len(reader.fieldnames)):
            #    if reader.fieldnames[i]!='' and reader.fieldnames[i] != 'year'  :
            #        dimensionList_content.append(reader.fieldnames[i])
            
            #dimensionList = {'content':dimensionList_content}

            dimensionList_content = []
            response = urllib.request.urlopen(url)
            reader = csv.DictReader(codecs.iterdecode(response, 'latin-1'), fieldnames=list_csv[5] ,delimiter=',')
            for i in range(len(reader.fieldnames)):
                if reader.fieldnames[i]!='' and reader.fieldnames[i] != 'year'  :
                    dimensionList_content.append(reader.fieldnames[i])

            dimensionList = {'content':dimensionList_content}
            values_columns = {}
            for f in reader.fieldnames:
                if f :
                    value_column=[]
                    response = urllib.request.urlopen(url)
                    reader = csv.DictReader(codecs.iterdecode(response, 'latin-1'), fieldnames=list_csv[5] ,delimiter=',')
                    for rowdict in reader:
                        value_column.append(rowdict[f])
                        values_columns[f] = value_column[7:-1]                    
            datasetCode = 'esri'
            releaseDates =response.getheaders()[0][1] 
            lastUpdate = datetime.datetime.strptime(releaseDates[5:], "%d %b %Y %H:%M:%S GMT") 
            documents = BulkSeries(datasetCode,{})

            for count_key in dimensionList:
                for count in dimensionList[count_key]:
        
                    dimensions = {}
                    series_name = count + '; ' + 'Japan'
                    series_key = 'esri.' + 'NationalAccount' + ';' + 'Japan' 
                    value = values_columns[f]               
                    dimensions['content'] = count
                    #print('dooool')
                    documents.append(Series(provider='esri',
                                            key= series_key,
                                            name=series_name,
                                            datasetCode= 'esri',
                                            period_index= period_index,
                                            values=value,
                                            releaseDates= [lastUpdate],
                                            frequency='q',
                                            dimensions=dimensions,
                                            ))
                    pprint.pprint('provider=esri')
                    print(series_key)
                    print(series_name)
                    print( 'esri')
                    print( period_index)
                    print(value)
                    print([lastUpdate])
                    print('q')
                    print(dimensions)
            documents.bulk_update_database()  
            ff =    documents.bulk_update_elastic()  
            print(ff)                        
            return(documents.bulk_update_elastic())

            for count in dimensionList:
                dimensions = {}
                series_name = count + '; ' + 'Japan'
                series_key = 'esri.' + 'NationalAccount' + ';' + 'Japan' 
                value = values_columns[f]
                dimensions['content'] = count
                documents.append(Series(provider='esri',
                                        key= series_key,
                                        name=series_name,
                                        datasetCode= 'esri',
                                        period_index= period_index,
                                        values=value,
                                        releaseDates= [lastUpdate],
                                        frequency='q',
                                        dimensions=dimensions))                                                
            return(documents.bulk_update_database())
        else:
            raise Exception("The name of dataset was not entered!")
        

if __name__ == "__main__":
    import esri
    w = esri.esri()
    w.upsert_dataset('esri') 
