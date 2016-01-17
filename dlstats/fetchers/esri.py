# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 10:59:20 2015

@author: salimeh
"""

from dlstats.fetchers._commons import Fetcher, Series, Datasets, Providers, CodeDict
import urllib
import xlrd
import csv
import codecs
from datetime import datetime, date
import pandas
import pprint
from collections import OrderedDict
from re import match
import time
import requests
import os
from urllib.parse import urljoin
import tempfile
from lxml import etree
import logging
import re
import json

VERSION = 1

logger = logging.getLogger(__name__)

REGEX_ANNUAL = re.compile('(\d\d\d\d)/((4-3)|(1-4)|(1-12))')
REGEX_QUARTER = re.compile('(?:(\d\d\d\d)/ )?((\d\d-\d\d)|(\d- \d))')

PROVIDER_NAME = 'ESRI'

INDEX_URL = 'http://www.esri.cao.go.jp/index-e.html'

DATABASES = {
    'QGDP' : {
        'name': 'Quarterly Estimates of GDP',
        'url_base': 'http://www.esri.cao.go.jp/en/sna/data/sokuhou/files/',
        'filename': 'toukei_top.html',
        'store_filepath': '/tmp/esri'
    }
}

def parse_quarter(quarter_str):
    if quarter_str == '1- 3':
        quarter = 1
    elif quarter_str == '4- 6':
        quarter = 2
    elif quarter_str == '7- 9':
        quarter = 3
    elif quarter_str == '10-12':
        quarter = 4
    else:
        raise Exception('quarter not recognized')
    return quarter

def parse_dates(column):
    if re.match(REGEX_ANNUAL,column[0]):
        freq = 'A'
        start_year = int(re.match(REGEX_ANNUAL,column[0]).group(1))
        end_year = start_year
        for c in column[1:]:
            matches = re.match(REGEX_ANNUAL,c)
            if not matches:
                break
            else:
                next_year = int(matches.group(1))
            if next_year != end_year + 1:
                raise Exception('error in year sequence')
            end_year = next_year
    else:
        freq = 'Q'
        matches = re.match(REGEX_QUARTER,column[0])
        if not matches:
            raise Exception('start_date not recognized')
        start_year = int(matches.group(1))
        start_quarter = parse_quarter(matches.group(2))
        # checking next year beginning
        matches = re.match(REGEX_QUARTER,column[5-start_quarter])
        if (not matches) or int(matches.group(1)) != start_year + 1:
            raise Exception('start_date not recognized')

        end_year = start_year
        end_quarter = start_quarter
        for c in column[1:]:
            if type(c) is not str:
                break
            matches = re.match(REGEX_QUARTER,c)
            if not matches:
                break
            elif matches.group(1):
                next_year = int(matches.group(1))
                if next_year != end_year + 1:
                    raise Exception('error in year sequence')
                next_quarter = parse_quarter(matches.group(2))
                if next_quarter != 1:
                    raise Exception('first quarter of the year is not 1')
                end_year = next_year
            else:
                next_quarter = parse_quarter(matches.group(2))
                if next_quarter != end_quarter + 1:
                    raise Exception('error in quarter sequence')
            end_quarter = next_quarter

    if freq == 'A':
        start_date = pandas.Period(start_year,freq='A').ordinal
        end_date = pandas.Period(end_year,freq='A').ordinal
    elif freq == 'Q':
        start_date = pandas.Period('%sQ%s' % (start_year,start_quarter),freq='Q').ordinal
        end_date = pandas.Period('%sQ%s' % (end_year,end_quarter),freq='Q').ordinal

    return(freq,start_date,end_date)

def download_page(url):
        try:
            response = requests.get(url)

            if not response.ok:
                msg = "download url[%s] - status_code[%s] - reason[%s]" % (url, 
                                                                           response.status_code, 
                                                                           response.reason)
                logger.error(msg)
                raise Exception(msg)
            
            return response.content
                
            #TODO: response.close() ?
            
        except requests.exceptions.ConnectionError as err:
            raise Exception("Connection Error")
        except requests.exceptions.ConnectTimeout as err:
            raise Exception("Connect Timeout")
        except requests.exceptions.ReadTimeout as err:
            raise Exception("Read Timeout")
        except Exception as err:
            raise Exception("Not captured exception : %s" % str(err))            

def make_dataset(anchor, url):
    url = urljoin(url,anchor.get('href'))
    dirs = url.split('/')
    filename = dirs[-1]
    release_date = date(int(dirs[-4]),int(dirs[-3]),int(dirs[-2]))
    code = re.match('(.*)\d\d\d\d.csv',filename).group(1)
    name = re.match('(.*)[ ]*\([^(]*$',anchor.text).group(1)
    return {'name': name,
            'dataset_code': code,
            'filename': filename,
            'url': url,
            'release_date': release_date}
    
def parse_esri_site():
    url = INDEX_URL
    # general index
    page = download_page(url)
    html = etree.HTML(page)
    uls = html.findall('.//ul[@class="bulletList"]')
    sna = parse_sna(uls[0],url)
#    bs = parse_business_statistics(uls[1])
    site_tree = {'name': 'root',
                 'doc_href': None,
                 'children': [sna]}
#                 'children': [sna, bs]}
    return site_tree

def parse_sna(ul,url):
    anchors = ul.findall('.//li/a')
    qgdp = parse_qgdp(urljoin(url,anchors[0].get('href')))
#    parse_capital_stock(anchors[2].get('href'))
    branch = {'name': 'National accounts of Japan',
              'doc_href': '',
              'children': [qgdp]}
    return branch

def parse_business_statistics(ul):
    anchors = ul.findall('.//a')
    bc = parse_business_conditions(anchors[0].get('href'),anchors[0].text)
    mo = parse_machinery_orders(anchors[1].get('href'),anchors[1].text)
    cc = parse_consumer_confidence(anchors[2].get('href'),anchors[2].text)
    bo = parse_business_outlook(anchors[3].get('href'),anchors[3].text)
    cb = parse_corporate_behavior(anchors[4].get('href'),anchors[4].text)
    branch = {'name': 'Business statistics',
              'doc_href': '',
              'children': [bc, mo, cc, bo, cb]}
    return branch

def parse_qgdp(url):
    # quarterly estimate of GDP
    page = download_page(url)
    # find latest data
    html = etree.HTML(page)
    # release archive
    anchor = html.find('.//h2[2]/a')
    url = urljoin(url,anchor.get('href'))
    # Release archive (toukei_top.html)
    page = download_page(url)
    html = etree.HTML(page)
    anchor = html.find('.//ul[@class="bulletList ml20"]/li/a')
    # Go to release archive
    url = urljoin(url, anchor.get('href'))
    page = download_page(url)
    # find latest data
    html = etree.HTML(page)
    anchor = html.find('.//table[@class="tableBase"]/tbody/tr/td[2]/a')
    # Go to latest release
    url = urljoin(url, anchor.get('href'))
    page = download_page(url)
    # find urls
    html = etree.HTML(page)
    titles = html.findall('.//h3')
    tbodies = html.findall('.//table[@class="tableBase"]/tbody')
    branch = {}
    branch['name'] = titles[0].text
    branch['children'] = []
    branch['doc_href'] = ''
    subbranch = {}
    subbranch['name'] = titles[1].text
    subbranch['doc_href'] = ''
    amounts = parse_amounts(tbodies[0],url)
    deflators = parse_deflators(tbodies[1],url)
    subbranch['children'] = amounts + deflators
    branch['children'].append(subbranch)
    subbranch = {}
    subbranch['name'] = titles[2].text
    subbranch['doc_href'] = ''
    table = html.find('.//table[@class="tableBase"][2]')
    amounts = parse_amounts(tbodies[2],url)
    deflators = parse_deflators(tbodies[3],url)
    compensation = parse_compensation(tbodies[4],url)
    subbranch['children'] = amounts + deflators + compensation
    branch['children'].append(subbranch)

    return branch

def parse_amounts(tbody,url):
    rows = tbody.findall('.//tr')
    G = []
    branch = {}
    for r in rows:
        header = r.find('.//th')
        if header is not None:
            if branch:
                G.append(branch)
                branch = {}
            branch['datasets'] = []
            branch['name'] = header.text
            branch['doc_href'] = ''
        anchors = r.findall('.//a')
        for a in anchors:
            dataset = make_dataset(a,url)
            branch['datasets'].append(dataset)
    G.append(branch)
    return G

def parse_deflators(tbody,url):
    rows = tbody.findall('.//tr')
    branch = {}
    branch['name'] = 'Deflators'
    branch['doc_href'] = ''
    children = [{},{}]
    children[0]['name'] = 'Amount'
    children[0]['doc_href'] = ''
    children[0]['datasets'] = []
    children[1]['name'] = 'Change from the previous term'
    children[1]['doc_href'] = ''
    children[1]['datasets'] = []
    for r in rows:
        anchors = r.findall('.//a')
        for index,a in enumerate(anchors):
            dataset = make_dataset(a,url)
            children[index]['datasets'].append(dataset)
    branch['children'] = children
    return [branch]

def parse_compensation(tbody,url):
    a = tbody.find('.//a')
    url = urljoin(url,a.get('href'))
    filename = url.split('/')[-1]
    branch = {}
    branch['name'] = 'Compensation of Employees'
    branch['doc_href'] = ''
    branch['datasets'] = [{'name': 'Compensation of Employees',
                          'dataset_code': 'kshotoku',
                          'filename': filename,
                          'url': url}]
    return [branch]

def parse_business_conditions(url,name):
    page = download_page(url)
    html = etree.HTML(page)
    tbody = html.find('.//table/tbody')
    trs = tbody.findall('.//tr')
    children = []
    for tr in trs:
        branch = {'name': tr.find('.//th').text,
                  'doc_href': '',
                  'children': []}
        anchors = tr.findall('.//td/a')
        for a in anchors:
            name = re.match('(.*)\(.*\)',a.text).group(1)
            url_ = urljoin(url,a.get('href'))
            filename = url_.split('/')[-1]
            code = re.match('(.*)\d\d\d\d.csv',filename).group(1)
            dataset = {'name': name,
                       'doc_href': '',
                       'dataset_code': code,
                       'filename': filename}
            branch['children'].append(dataset)
        children.append(branch)
    return {'name': name,
            'doc_href': '',
            'children': children}

def parse_machinery_orders(url,name): 
    page = download_page(url)
    html = etree.HTML(page)
    children = []
    ul = html.find('.//ul[@class="bulletList"]')
    anchors = tr.findall('.//td/a')
    for a in anchors:
        name = re.match('(.*)\(.*\)',a.text).group(1)
        url_ = urljoin(url,a.get('href'))
        filename = url_.split('/')[-1]
        code = re.match('(.*)\d\d\d\d.csv',filename).group(1)
        dataset = {'name': name,
                   'doc_href': '',
                   'dataset_code': code,
                   'filename': filename}
        children.append(dataset)
    return {'name': name,
            'doc_href': '',
            'children': children}

def parse_consumer_confidence(url,name):
    page = download_page(url)
    html = etree.HTML(page)
    children = []
    ul = html.find('.//ul[@class="bulletList"]')
    anchors = tr.findall('.//td/a')
    for a in anchors:
        name = re.match('(.*)\(.*\)',a.text).group(1)
        url_ = urljoin(url,a.get('href'))
        filename = url_.split('/')[-1]
        code = re.match('(.*)\d\d\d\d.csv',filename).group(1)
        dataset = {'name': name,
                   'doc_href': '',
                   'dataset_code': code,
                   'filename': filename}
        children.append(dataset)
    return {'name': name,
            'doc_href': '',
            'children': children}

def parse_business_outlook(url,name):
    pass

def parse_corporate_behavior(url,name):
    page = download_page(url)
    html = etree.HTML(page)
    children = []
    ul = html.find('.//ul[@class="bulletList"]')
    anchors = tr.findall('.//td/a')
    for a in anchors:
        name = re.match('(.*)\(.*\)',a.text).group(1)
        url_ = urljoin(url,a.get('href'))
        filename = url_.split('/')[-1]
        code = re.match('(.*)\d\d\d\d.csv',filename).group(1)
        dataset = {'name': name,
                   'doc_href': '',
                   'dataset_code': code,
                   'filename': filename}
        children.append(dataset)
    return {'name': name,
            'doc_href': '',
            'children': children}


class Downloader():
    
    headers = {
        'user-agent': 'dlstats - https://github.com/Widukind/dlstats'
    }
    
    def __init__(self, url=None, filename=None, store_filepath=None, 
                 timeout=None, max_retries=0, replace=True):
        self.url = url
        self.filename = filename
        self.store_filepath = store_filepath
        self.timeout = timeout
        self.max_retries = max_retries
        
        if not self.store_filepath:
            self.store_filepath = tempfile.mkdtemp()
        else:
            if not os.path.exists(self.store_filepath):
                os.makedirs(self.store_filepath, exist_ok=True)
        self.filepath = os.path.abspath(os.path.join(self.store_filepath, self.filename))
        
        #TODO: force_replace ?
        
        if os.path.exists(self.filepath) and not replace:
            raise Exception("filepath is already exist : %s" % self.filepath)
        
    def _download(self):
        
        #TODO: timeout
        #TODO: max_retries (self.max_retries)
        #TODO: analyse rate limit dans headers
        
        start = time.time()
        try:
            #TODO: Session ?
            response = requests.get(self.url, 
                                    timeout=self.timeout, 
                                    stream=True, 
                                    allow_redirects=True,
                                    verify=False, #ssl
                                    headers=self.headers)

            if not response.ok:
                msg = "download url[%s] - status_code[%s] - reason[%s]" % (self.url, 
                                                                           response.status_code, 
                                                                           response.reason)
                logger.error(msg)
                raise Exception(msg)
            
            with open(self.filepath,'wb') as f:
                for chunk in response.iter_content():
                    f.write(chunk)
                    #TODO: flush ?            
                
            #TODO: response.close() ?
            
        except requests.exceptions.ConnectionError as err:
            raise Exception("Connection Error")
        except requests.exceptions.ConnectTimeout as err:
            raise Exception("Connect Timeout")
        except requests.exceptions.ReadTimeout as err:
            raise Exception("Read Timeout")
        except Exception as err:
            raise Exception("Not captured exception : %s" % str(err))            

        end = time.time() - start
        logger.info("download file[%s] - END - time[%.3f seconds]" % (self.url, end))
    
    def get_filepath(self, force_replace=False):
        
        if os.path.exists(self.filepath) and force_replace:
            os.remove(self.filepath)
        
        if not os.path.exists(self.filepath):
            logger.info("not found file[%s] - download dataset url[%s]" % (self.filepath, self.url))
            self._download()
        else:
            logger.info("use local dataset file [%s]" % self.filepath)
        
        return self.filepath

class Esri(Fetcher):
    def __init__(self, db=None):
        super().__init__(provider_name='esri', db=db)         
        self.provider_name = 'esri'
        self.provider = Providers(name=self.provider_name,
                                  long_name='Economic and Social Research Institute, Cabinet Office',
                                  version=VERSION,
                                  region='Japan',
                                  website='http://www.esri.cao.go.jp/index-e.html',
                                  fetcher=self)
        self.datasets_dict = {}

    def make_datasets_dict(self):
        datas = parse_esri_site()
        def make_node(data):
            if 'children' in data:
                for c in data['children']:
                    make_node(c)
            elif 'datasets' in data:
                for d in data['datasets']:
                    self.datasets_dict.update({d['dataset_code']:d})

        make_node(datas)    
#        self.provider.add_data_tree(data_tree)
        
    def upsert_categories(self):
        data_tree = []
        datas = parse_esri_site()
        def make_node(data,parent):
            node = {}
            node['name'] = data['name']
            node['doc_href'] = data['doc_href']
            if parent is 0:
                node['parent'] = None
            else:
                node['parent'] = parent
            code = parent + 1
            node['category_code'] = str(code)
            if 'children' in data:
                node['datasets'] = []
                for c in data['children']:
                    code = make_node(c,code)
            elif 'datasets' in data:
                node['datasets'] = [d['dataset_code'] for d in data['datasets']]
            data_tree.append(node)
            return code

        make_node(datas, 0)    
#        self.provider.add_data_tree(data_tree)
        
    def esri_issue(self):
        for self.url in self.url_all :
            dataset_code = self.dataset_code_list[self.url_all.index(self.url)]
            self.upsert_dataset(dataset_code)

    def upsert_dataset(self, dataset_code):
        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        if not self.datasets_dict:
            self.make_datasets_dict()
        cat = self.datasets_dict[datasets_dict]
        dataset = Datasets(self.provider_name,dataset_code,
                           fetcher=self)
        data_iterator = EsriData(dataset,cat['url'])
        dataset.name = cat['name']
        dataset.doc_href = cat['doc_href']
        dataset.last_update = cat['release_date']
        dataset.series.data_iterator = data_iterator
        dataset.update_database()
        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))

    def upsert_all_datasets(self):
        self.upsert_categories()
        self.esri_issue()
        
    def upsert_all_datasets(self):
        self.upsert_categories()
        self.esri_issue()

    def upsert_latest_quarterly_estimates_of_gdp(self):
        self.parse_quarterly_esimates_of_gdp_release_archive_page()

class EsriData():
    def __init__(self, dataset,  filename=None, store_filepath=None):
        self.provider_name = dataset.provider_name
        self.dataset_code = dataset.dataset_code
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        self.filename = filename
        self.store_filepath = store_filepath
        self.dataset_url = self.make_url()
        self.panda_csv = self.get_csv_data()
#        self.release_date = self.get_release_date()
        self.release_date = dataset.last_update
        [self.nrow,self.ncol] = self.panda_csv.shape
        self.column_nbr = 0
        (self.frequency,self.start_date,self.end_date) = parse_dates(list(self.panda_csv.iloc[6:,0]))
        self.series_names = self.fix_series_names()
        self.key = 0

    def get_store_path(self):
        return self.store_filepath or os.path.abspath(os.path.join(
            tempfile.gettempdir(), 
            self.provider_name))
    
    def _load_datas(self):
        
        store_filepath = self.get_store_path()
        # TODO: timeout, replace
        download = Downloader(url=self.dataset_url, filename=self.filename, store_filepath=store_filepath)
            
        return(download.get_filepath(force_replace=False))

    def get_csv_data(self):
        return pandas.read_csv(self._load_datas(),encoding='cp932')
    
    def get_release_date(self):
        response = urllib.request.urlopen(self.url)
        releaseDate = response.info()['Last-Modified'] 
        return datetime.strptime(releaseDate,"%a, %d %b %Y %H:%M:%S GMT")                                                  
        
    def fix_series_names(self):
        #generating name of the series             
        columns = self.panda_csv.columns
        series_names = ['nan']*columns.size
        for column_ind in range(1,columns.size):
            if str(self.panda_csv.iloc[5,column_ind]) != "nan":
                series_names[column_ind] = self.edit_seriesname(str(self.panda_csv.iloc[4,column_ind]))+', '+str(self.panda_csv.iloc[5,column_ind])
            else:    
                series_names[column_ind] = self.edit_seriesname(str(self.panda_csv.iloc[4,column_ind]))
            if str(self.panda_csv.iloc[4,column_ind]) == "nan" :
                if (str(self.panda_csv.iloc[5,column_ind]) != "nan") and (str(self.panda_csv.iloc[4,column_ind-1])) != "nan":         
                    series_names[column_ind] = self.edit_seriesname(str(self.panda_csv.iloc[4,column_ind-1]))+', '+str(self.panda_csv.iloc[5,column_ind])
                else:
                    if str(self.panda_csv.iloc[4,column_ind-1]) == "nan":
                        series_names[column_ind] = self.edit_seriesname(str(self.panda_csv.iloc[4,column_ind-2]))+', '+str(self.panda_csv.iloc[5,column_ind])  
            #Take into the account FISIM 
            if str(self.panda_csv.iloc[5,column_ind-1]) == "Excluding FISIM":
                series_names[column_ind] = self.edit_seriesname(str(self.panda_csv.iloc[4,column_ind]))+', '+str(self.panda_csv.iloc[5,column_ind-1])               
            if str(self.panda_csv.iloc[5,column_ind-2]) == "Excluding FISIM":
                series_names[column_ind] = self.edit_seriesname(str(self.panda_csv.iloc[4,column_ind]))+', '+str(self.panda_csv.iloc[5,column_ind-2])
            if str(self.panda_csv.iloc[5,column_ind-3]) == "Excluding FISIM":
                series_names[column_ind] = self.edit_seriesname(str(self.panda_csv.iloc[4,column_ind]))+', '+str(self.panda_csv.iloc[5,column_ind-3])
                
        lent = len(self.panda_csv.iloc[0,:])
        if str(self.panda_csv.iloc[0,:][lent-1]) == "(%)":
            self.currency = str(self.panda_csv.iloc[0,lent-2])
        else:
            self.currency = str(self.panda_csv.iloc[0,lent-1])
        return series_names
        
    def edit_seriesname(self,seriesname):   
         seriesname = seriesname.replace(' ','')  
         seriesname = re.sub(r'([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))', r'\1 ', seriesname)
         seriesname = re.sub(r"((of)|(in) |(from/to))", r" \1 ", seriesname)  
         seriesname = re.sub(r"(&)", r" \1 ", seriesname)
         seriesname = re.sub(r"(\()", r" \1", seriesname) 
         seriesname = seriesname.replace('  ',' ')
         return(seriesname)  
        
    def __next__(self):
        if self.column_nbr == self.ncol:
            raise StopIteration()
        column = self.panda_csv.iloc[:,self.column_nbr]
        if ((self.series_names[self.column_nbr] == "nan, nan")
            or ( self.series_names[self.column_nbr] == "nan" )) :
            self.column_nbr += 1
            if self.column_nbr == self.ncol:
                raise StopIteration()
            column = self.panda_csv.iloc[:,self.column_nbr]
        series = self.build_series(column,str(self.key),self.series_names[self.column_nbr])
        self.key += 1
        self.column_nbr += 1
        return(series) 
                                           
    def build_series(self,column,key,name):
        dimensions = {}
        series = {}
        series_value = []
        dimensions['concept'] = self.dimension_list.update_entry('concept','',name)
        for r in range(6, len(column)):
            if type(column[r]) is str:
                series_value.append(str(column[r]).replace(' ',''))    
        series['values'] = series_value                
        series['provider_name'] = self.provider_name       
        series['dataset_code'] = self.dataset_code
        series['name'] = name
        series['key'] = key
        series['start_date'] = self.start_date
        series['end_date'] = self.end_date  
        series['last_update'] = self.release_date
        series['dimensions'] = dimensions
        series['frequency'] = self.frequency
        series['attributes'] = {}
        return(series)

    def make_url(self):
        # TODO: add url's root
        return self.dataset_code

if __name__ == "__main__":
#    e = Esri()
    parse_quarterly_esimates_of_gdp_release_archive_page()
#    e.provider.update_database()
#    e.upsert_all_datasets()
#    e.upsert_categories()
    
