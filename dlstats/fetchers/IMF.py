# -*- coding: utf-8 -*-

from dlstats.fetchers._skeleton import Skeleton, Category, Series, BulkSeries, Dataset, Provider
import urllib
import xlrd
import csv
import codecs
import datetime
import pandas
import pprint
from collections import OrderedDict

class IMF(Skeleton):
    def __init__(self):
        super().__init__(provider_name='IMF') 
        self.urls = [
            'http://localhost:8800/imf//WEOSep2006all.xls',
            'http://localhost:8800/imf/WEOApr2007all.xls'
#            'http://www.imf.org/external/pubs/ft/weo/2006/02/data/WEOSep2006all.xls',
#            'http://www.imf.org/external/pubs/ft/weo/2007/01/data/WEOApr2007all.xls',
#        'http://www.imf.org/external/pubs/ft/weo/2007/02/weodata/WEOOct2007all.xls',
#        'http://www.imf.org/external/pubs/ft/weo/2008/01/weodata/WEOApr2008all.xls',
#        'http://www.imf.org/external/pubs/ft/weo/2008/02/weodata/WEOOct2008all.xls',
#        'http://www.imf.org/external/pubs/ft/weo/2009/01/weodata/WEOApr2009all.xls',
#        'http://www.imf.org/external/pubs/ft/weo/2009/02/weodata/WEOOct2009all.xls',
#        'http://www.imf.org/external/pubs/ft/weo/2010/01/weodata/WEOApr2010all.xls',
#        'http://www.imf.org/external/pubs/ft/weo/2010/02/weodata/WEOOct2010all.xls',
#        'http://www.imf.org/external/pubs/ft/weo/2011/01/weodata/WEOApr2011all.xls',
#        'http://www.imf.org/external/pubs/ft/weo/2011/02/weodata/WEOSep2011all.xls',
#        'http://www.imf.org/external/pubs/ft/weo/2012/01/weodata/WEOApr2012all.xls',
#        'http://www.imf.org/external/pubs/ft/weo/2012/02/weodata/WEOOct2012all.xls',
#        'http://www.imf.org/external/pubs/ft/weo/2013/01/weodata/WEOApr2013all.xls',
#        'http://www.imf.org/external/pubs/ft/weo/2013/02/weodata/WEOOct2013all.xls',
#        'http://www.imf.org/external/pubs/ft/weo/2014/01/weodata/WEOApr2014all.xls', 
#        'http://www.imf.org/external/pubs/ft/weo/2014/02/weodata/WEOOct2014all.xls',
#        'http://www.imf.org/external/pubs/ft/weo/2015/01/weodata/WEOApr2015all.xls'
        ]
        self.readers = []
        self.releaseDates = []
        self.files_ = {}
        for url in self.urls:
            self.response= urllib.request.urlopen(url)
            self.readers.append(csv.DictReader(codecs.iterdecode(self.response, 'latin-1'), delimiter='\t'))
        self.date = [
            'September 2006', 'April 2007'
#            'April 2007', 'October 2007', 'April 2008', 'October 2008', 'April 2009','October 2009',
#            'April 2010', 'October 2010', 'April 2011', 'October 2011', 'April 2012','October 2012',
#            'April 2013', 'October 2013', 'April 2014', 'October 2014', 'April 2015'
            ]
        for self.count, self.value_date in enumerate(self.date):
            #print(self.count)
            self.files_[self.value_date] = self.readers[self.count]
            self.releaseDates.append(datetime.datetime.strptime(self.value_date, "%B %Y"))
        self.provider_name = 'IMF'
        self.provider = Provider(name=self.provider_name,website='http://http://www.imf.org/')
        
    def upsert_dataset(self, datasetCode):
        if datasetCode=='WEO':
            WEO_Country_Code_list = OrderedDict()
            country_list =  OrderedDict()
            subject_list =  OrderedDict()
            Units_list =  OrderedDict()
            Scale_list =  OrderedDict()
            for co, v_date in enumerate(self.date):
                #print(v_date)
                print(self.files_.keys)
                reader = self.files_[v_date]
                for count, row in enumerate(reader):
                    # last 2 rows are blank/metadata
                    # so get out when we hit a blank row
                    if row['Country']:
                        if row['WEO Country Code'] not in WEO_Country_Code_list: WEO_Country_Code_list[row['WEO Country Code']] = row['WEO Country Code']
                        if row['ISO'] not in country_list.keys(): country_list[row['ISO']] = row['Country'] 
                        if row['Units'] not in Units_list.values(): Units_list[str(int(next(reversed(Units_list),0))+1)] = row['Units']
                        if row['Scale'] not in Scale_list: Scale_list[row['Scale']] = row['Scale']
                        if row['WEO Subject Code'] not in subject_list: subject_list[row['WEO Subject Code']] = row['Subject Descriptor']
                            
            dimensionList = { 'Country': [[k,country_list[k]] for k in country_list],
                              'WEO Country Code': [[k,WEO_Country_Code_list[k]] for k in WEO_Country_Code_list],
                              'Subject': [[k,subject_list[k]] for k in subject_list],
                              'Units': [[k,Units_list[k]] for k in Units_list],
                              'Scale': [[k,Scale_list[k]] for k in Scale_list] }
            attributeList = {'OBS_VALUE': [('e', 'Estimates Start After')]}
            document = Dataset(provider = self.provider_name, 
                               name = 'World Economic Outlook' ,
                               datasetCode = 'WEO', lastUpdate = self.releaseDates[co],
                               dimensionList = dimensionList, docHref = "http://http://www.imf.org/",
                               attributeList = attributeList) 
            effective_dimension_list = self.update_series('WEO', dimensionList)
            document.update_database()
            document.update_es_database(effective_dimension_list)
        else:
            raise Exception("The name of dataset was not entered!")
    def upsert_categories(self):
        document = Category(provider = self.provider_name, 
                            name = 'WEO' , 
                            categoryCode ='WEO')
        return document.update_database()
        

    def update_series(self,datasetCode,dimensionList):
        files_in = {} 
        readers2 =[]
        dimension_reverse_index_units = {c: i for i,c in dimensionList['Units']}
        if datasetCode=='WEO':
            for url in self.urls:
                response= urllib.request.urlopen(url)
                readers2.append(csv.DictReader(codecs.iterdecode(response, 'latin-1'), delimiter='\t'))                   
            for count, value_date in enumerate(self.date):
                reader2 = readers2[count]                         
                years = reader2.fieldnames[9:-1] 
                period_index = pandas.period_range(years[0], years[-1] , freq = 'annual')   
                attributeList = {'OBS_VALUE': [('e', 'Estimates Start After')]} 
                documents = BulkSeries(datasetCode,dimensionList,attributeList)
                for row in reader2:
                    dimensions = {}
                    value = []
                    if row['Country']:               
                        for year in years:
                            value.append(row[year])
                        dimensions['Country'] = row['ISO']
                        dimensions['WEO Country Code'] = row['WEO Country Code']
                        dimensions['Subject'] = row['WEO Subject Code']
                        dimensions['Units'] = dimension_reverse_index_units[row['Units']]
                        dimensions['Scale'] = row['Scale']
                        attributes = {}
                        series_name = row['Subject Descriptor']+'.'+row['Country']+'.'+row['Units']
                        series_key = row['WEO Subject Code']+'.'+row['ISO']+'.'+dimensions['Units']
                        if row['Estimates Start After']:
                            estimation_start = int(row['Estimates Start After']);
                            attributes = {'flag': [ '' if int(y) < estimation_start else 'e' for y in years]}
                        release_dates = [self.releaseDates[count] for v in value]
                        documents.append(Series(provider=self.provider_name,
                                                key= series_key,
                                                name=series_name,
                                                datasetCode= 'WEO',
                                                period_index= period_index,
                                                values=value,
                                                releaseDates= release_dates,
                                                frequency='A',
                                                attributes = attributes,
                                                dimensions=dimensions))
                documents.bulk_update_database()
                return(documents.bulk_update_elastic())
        else:
            raise Exception("The name of dataset was not entered!")
if __name__ == "__main__":
    import IMF
    w = IMF.IMF()
    w.provider.update_database()
    w.upsert_categories()
    w.upsert_dataset('WEO') 


              
