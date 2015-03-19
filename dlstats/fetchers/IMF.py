# -*- coding: utf-8 -*-

from dlstats.fetchers._skeleton import Skeleton, Category, Series, BulkSeries, Dataset, Provider
import urllib
import xlrd
import csv
import codecs
import datetime
import pandas
import pprint

class IMF(Skeleton):
    def __init__(self):
        super().__init__() 
        self.response= urllib.request.urlopen('http://www.imf.org/external/pubs/ft/weo/2014/01/weodata/WEOApr2014all.xls')
        self.readers = csv.DictReader(codecs.iterdecode(self.response, 'latin-1'), delimiter='\t')
        self.files_ = {'WEOApr2014all':self.readers}
        self.provider = Provider(name='IMF',website='http://http://www.imf.org/')
        self.releaseDates_ = self.response.getheaders()[3][1] 
        self.releaseDates = datetime.datetime.strptime(self.releaseDates_[5:], "%d %b %Y %H:%M:%S GMT")
        
    def upsert_dataset(self, datasetCode):
        if datasetCode=='WEO':
            reader = self.files_['WEOApr2014all']
        else:
            raise Exception("The name of dataset was not entered!")
        countries_list = []
        ISO_list = []
        Subject_Notes_list = []
        Units_list = []
        Scale_list = []
        WEO_Country_Code_list = []
        Country_Series_specific_Notes_list = []
        WEO_Subject_Code_list = [] 
        CountryCode_ltuple = []
        Subject_ltuple = []        
        for count, row in enumerate(reader):
            # last 2 rows are blank/metadata
            # so get out when we hit a blank row
            if row['Country']:
                if row['Country'] not in countries_list: countries_list.append(row['Country'])
                if row['WEO Country Code'] not in WEO_Country_Code_list: WEO_Country_Code_list.append(row['WEO Country Code'])
                if row['ISO'] not in ISO_list: ISO_list.append(row['ISO']) 
                if row['Subject Notes'] not in Subject_Notes_list: Subject_Notes_list.append(row['Subject Notes'])
                if row['Units'] not in Units_list: Units_list.append(row['Units'])
                if row['Scale'] not in Scale_list: Scale_list.append(row['Scale'])
                if row['Country/Series-specific Notes'] not in Country_Series_specific_Notes_list: Country_Series_specific_Notes_list.append(row['Country/Series-specific Notes'])
                if row['WEO Subject Code'] not in WEO_Subject_Code_list: WEO_Subject_Code_list.append(row['WEO Subject Code'])
                if [row['ISO'] , row['Country']] not in CountryCode_ltuple:  CountryCode_ltuple.append([row['ISO'] , row['Country']])
                if [row['WEO Subject Code'] , row['Subject Descriptor']] not in Subject_ltuple:  Subject_ltuple.append([row['WEO Subject Code'] , row['Subject Descriptor']])
                    
        dimensionList = {'Country Code': WEO_Country_Code_list,
                       'ISO': CountryCode_ltuple,
                       'Subject Code': Subject_ltuple,
                       'Units': Units_list,
                       'Scale': Scale_list}
        attributeList = {'OBS_VALUE': [('e', 'Estimates Start After')]}
        document = Dataset(provider = 'IMF', 
                   name = 'World Economic Outlook' ,
                   datasetCode = 'WEO', lastUpdate = self.releaseDates,
                   dimensionList = dimensionList, docHref = "http://http://www.imf.org/",
                   attributeList = attributeList) 
        effective_dimension_list = self.update_series('WEO', dimensionList)    
        document.update_database()
        document.update_es_database(effective_dimension_list)               
                
    def upsert_categories(self):
        document = Category(provider = 'IMF', 
                            name = 'WEO' , 
                            categoryCode ='WEO')
        return document.update_database()
        

    def update_series(self,datasetCode,dimensionList):
        if datasetCode=='WEO':
            reader = self.files_['WEOApr2014all']
        else:
            raise Exception("The name of dataset was not entered!")     
        years = reader.fieldnames[9:-1] 
        period_index = pandas.period_range(years[0], years[-1] , freq = 'annual')   
        attributeList = {'OBS_VALUE': [('e', 'Estimates Start After')]} 
        response= urllib.request.urlopen('http://www.imf.org/external/pubs/ft/weo/2014/01/weodata/WEOApr2014all.xls')
        reader = csv.DictReader(codecs.iterdecode(response, 'latin-1'), delimiter='\t')
        documents = BulkSeries(datasetCode,{})
        for row in reader:
            dimensions = {}
            value = []
            if row['Country']:               
                series_name = row['Subject Descriptor'] + '; ' + row['Country']
                series_key = 'WEO.' + row['WEO Subject Code'] + '; ' + row['ISO'] 
                for year in years:
                    value.append(row[year])               
                dimensions['Country Code'] = row['WEO Country Code']
                dimensions['ISO'] = row['ISO']
                dimensions['Country'] = row['Country']
                dimensions['Units'] = row['Units']
                dimensions['Scale'] = row['Scale']
                dimensions['Subject Code'] = row['WEO Subject Code']
                attributes = {}
                if row['Estimates Start After']:
                    estimation_start = int(row['Estimates Start After']);
                    attributes = {'flag': [ '' if int(y) < estimation_start else 'e' for y in years]}
                documents.append(Series(provider='IMF',
                                        key= series_key,
                                        name=series_name,
                                        datasetCode= 'WEO',
                                        period_index= period_index,
                                        values=value,
                                        releaseDates= [self.releaseDates],
                                        frequency='A',
                                        attributes = attributes,
                                        dimensions=dimensions))
                                    
            
        return(documents.bulk_update_database())

if __name__ == "__main__":
    import IMF
    w = IMF.IMF()
    w.upsert_dataset('WEO') 


              
