# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 10:59:20 2015

@author: salimeh
"""

from dlstats import constants
from dlstats.utils import Downloader
from dlstats.fetchers._commons import Fetcher, Datasets, Providers
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

INDEX_URL = 'http://www.esri.cao.go.jp/index-e.html'


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
    for row_nbr,c in enumerate(column):
        if type(c) is not str:
            continue
        matches = re.match(REGEX_ANNUAL,c)
        if matches:
            freq = 'A'
            start_year = int(matches.group(1))
            end_year = start_year
            first_row = row_nbr
            last_row = first_row
            break
        matches = re.match(REGEX_QUARTER,c)
        if matches:
            freq = 'Q'
            start_year = int(matches.group(1))
            start_quarter = parse_quarter(matches.group(2))
            # checking next year beginning
            matches = re.match(REGEX_QUARTER,column[row_nbr + 5 - start_quarter])
            if (not matches) or int(matches.group(1)) != start_year + 1:
                raise Exception('start_date not recognized')
            end_year = start_year
            end_quarter = start_quarter
            first_row = row_nbr
            last_row = first_row
            break
        if (row_nbr + 1) == len(column):
            raise Exception('start_date not recognized')

    if freq == 'A':
        for c in column[first_row+1:]:
            if type(c) is not str:
                break
            matches = re.match(REGEX_ANNUAL,c)
            if not matches:
                break
            else:
                next_year = int(matches.group(1))
            if next_year != end_year + 1:
                raise Exception('error in year sequence')
            end_year = next_year
            last_row = last_row + 1
    else:
        for c in column[first_row+1:]:
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
            last_row = last_row + 1

    if freq == 'A':
        start_date = pandas.Period(start_year,freq='A').ordinal
        end_date = pandas.Period(end_year,freq='A').ordinal
    elif freq == 'Q':
        start_date = pandas.Period('%sQ%s' % (start_year,start_quarter),freq='Q').ordinal
        end_date = pandas.Period('%sQ%s' % (end_year,end_quarter),freq='Q').ordinal

    return(freq,start_date,end_date,first_row,last_row)

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
    release_date = datetime(int(dirs[-4]),int(dirs[-3]),int(dirs[-2]))
    code = re.match('(.*)\d\d\d\d.csv',filename).group(1)
    name = re.match('(.*)\([^(]*$',anchor.text).group(1).rstrip()
    return {'name': name,
            'dataset_code': code,
            'filename': filename,
            'url': url,
            'doc_href': None,
            'release_date': release_date}
    
def parse_esri_site():
    url = INDEX_URL
    # general index
    page = download_page(url)
    html = etree.HTML(page)
    uls = html.findall('.//ul[@class="bulletList"]')
    sna = parse_sna(uls[0],url)
#    bs = parse_business_statistics(uls[1])
    site_tree = [sna]
#   site_tree = [sna, bs]
    return site_tree

def parse_sna(ul,url):
    anchors = ul.findall('.//li/a')
    qgdp = parse_qgdp(urljoin(url,anchors[0].get('href')))
#    parse_capital_stock(anchors[2].get('href'))
    branch = {'name': 'National accounts of Japan',
              'category_code': 'SNA',
              'doc_href': None,
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
              'category_code': 'BusinessStatistics',
              'doc_href': None,
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
    branch['category_code'] = 'QuarterlyGDP'
    branch['children'] = []
    branch['doc_href'] = None
    subbranch = {}
    subbranch['name'] = titles[1].text
    subbranch['category_code'] = 'GDP'
    subbranch['doc_href'] = None
    amounts = parse_amounts(tbodies[0],url)
    deflators = parse_deflators(tbodies[1],url)
    subbranch['children'] = amounts + deflators
    branch['children'].append(subbranch)
    subbranch = {}
    subbranch['name'] = titles[2].text
    subbranch['category_code'] = 'FD'
    subbranch['doc_href'] = None
    table = html.find('.//table[@class="tableBase"][2]')
    amounts = parse_amounts(tbodies[2],url)
    deflators = parse_deflators(tbodies[3],url)
    subbranch['children'] = amounts + deflators
    # TODO
    #    compensation = parse_compensation(tbodies[4],url)
    #    subbranch['children'] = amounts + deflators + compensation
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
            # Use first word as category_code
            branch['category_code'] = header.text.split(' ')[0]
            branch['doc_href'] = None
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
    branch['category_code'] = 'Deflators'
    branch['doc_href'] = None
    children = [{},{}]
    children[0]['name'] = 'Amount'
    children[0]['category_code'] = 'Amount'
    children[0]['doc_href'] = None
    children[0]['datasets'] = []
    children[1]['name'] = 'Change from the previous term'
    children[1]['category_code'] ='Change'
    children[1]['doc_href'] = None
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
    dirs = url.split('/')
    filename = dirs[-1]
    release_date = datetime(int(dirs[-4]),int(dirs[-3]),int(dirs[-2]))
    branch = {}
    branch['name'] = 'Compensation of Employees'
    branch['category_code'] = 'Compensation'
    branch['doc_href'] = None
    branch['datasets'] = [{'name': 'Compensation of Employees',
                           'dataset_code': 'kshotoku',
                           'filename': filename,
                           'release_date': release_date,
                           'doc_href': None,
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
                  'doc_href': None,
                  'children': []}
        anchors = tr.findall('.//td/a')
        for a in anchors:
            name = re.match('(.*)\(.*\)',a.text).group(1)
            url_ = urljoin(url,a.get('href'))
            dirs = url.split('/')
            filename = dirs[-1]
            release_date = datetime(int(dirs[-4]),int(dirs[-3]),int(dirs[-2]))
            code = re.match('(.*)\d\d\d\d.csv',filename).group(1)
            dataset = {'name': name,
                       'doc_href': None,
                       'dataset_code': code,
                       'release_date': release_date,
                       'filename': filename}
            branch['children'].append(dataset)
        children.append(branch)
    return {'name': name,
            'doc_href': None,
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
        dirs = url.split('/')
        filename = dirs[-1]
        release_date = datetime(int(dirs[-4]),int(dirs[-3]),int(dirs[-2]))
        code = re.match('(.*)\d\d\d\d.csv',filename).group(1)
        dataset = {'name': name,
                   'doc_href': None,
                   'dataset_code': code,
                   'filename': filename}
        children.append(dataset)
    return {'name': name,
            'doc_href': None,
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
        dirs = url.split('/')
        filename = dirs[-1]
        release_date = datetime(int(dirs[-4]),int(dirs[-3]),int(dirs[-2]))
        code = re.match('(.*)\d\d\d\d.csv',filename).group(1)
        dataset = {'name': name,
                   'doc_href': None,
                   'dataset_code': code,
                   'release_date': release_date,
                   'filename': filename}
        children.append(dataset)
    return {'name': name,
            'doc_href': None,
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
        dirs = url.split('/')
        filename = dirs[-1]
        release_date = datetime(int(dirs[-4]),int(dirs[-3]),int(dirs[-2]))
        code = re.match('(.*)\d\d\d\d.csv',filename).group(1)
        dataset = {'name': name,
                   'doc_href': None,
                   'dataset_code': code,
                   'release_date': release_date,
                   'filename': filename}
        children.append(dataset)
    return {'name': name,
            'doc_href': None,
            'children': children}

class Esri(Fetcher):
    def __init__(self, db=None):
        super().__init__(provider_name='ESRI', db=db)         
        self.provider = Providers(name=self.provider_name,
                                  long_name='Economic and Social Research Institute, Cabinet Office',
                                  version=VERSION,
                                  region='Japan',
                                  website='http://www.esri.cao.go.jp/index-e.html',
                                  fetcher=self)
        self.datasets_dict = {}
        self.selected_codes = ['SNA']
        
    def build_data_tree(self, force_update=False):
        """Build data_tree from ESRI site parsing
        """
        if self.provider.count_data_tree() > 1 and not force_update:
            return self.provider.data_tree

        def make_node(data,parent_key):
            _category = dict(name=data['name'],
                             category_code=data['category_code'])
            _category_key = self.provider.add_category(_category,
                                                       parent_code=parent_key)
            if 'children' in data:
                for c in data['children']:
                    make_node(c,_category_key)
            if 'datasets' in data:
                for d in data['datasets']:
                    self.provider.add_dataset(dict(dataset_code = d['dataset_code'],
                                                   name = d['name'],
                                                   last_update = d['release_date'],
                                                   metadata={'url': d['url'],
                                                             'doc_href': d['doc_href']}),
                                              _category_key)                        
        try:
            for data in parse_esri_site():
                make_node(data, self.provider_name)
        except Exception as err:
            logger.error(err)   
            raise                             
        return self.provider.data_tree
        
    def get_selected_datasets(self):
        """Collects the dataset codes that are in data_tree
        below the ones indicated in "selected_codes" provided in configuration
        :returns: list of dict of dataset settings"""
        category_filter = [".*%s.*" % d for d in self.selected_codes]
        category_filter = "|".join(category_filter)
        self.selected_datasets = {d['dataset_code']: d for d in self.datasets_list(category_filter=category_filter)}
        return self.selected_datasets

    # necessary for test mock
    def make_url(self):
        return self.dataset_settings['metadata']['url']

    def upsert_dataset(self, dataset_code):
        """Updates data in Database for selected datasets
        :dset: dataset_code
        :returns: None"""
        self.get_selected_datasets()
        
        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))

        self.dataset_settings = self.selected_datasets[dataset_code]
        url = self.make_url()
        dataset = Datasets(self.provider_name,dataset_code,
                           fetcher=self)
        dataset.name = self.dataset_settings['name']
        dataset.doc_href = self.dataset_settings['metadata']['doc_href']
        dataset.last_update = self.dataset_settings['last_update']
        data_iterator = EsriData(dataset,url,filename=dataset_code)
        dataset.series.data_iterator = data_iterator
        dataset.update_database()
        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))

    # TO BE FINISHED    
    def parse_sna_agenda(self):
        #TODO: use Downloader
        download = Downloader(url="http://www.esri.cao.go.jp/en/sna/kouhyou/kouhyou_top.html",
                              filename="agenda_sna.html")
        with open(download.get_filepath(), 'rb') as fp:
            agenda = lxml.html.parse(fp)
        
    # TO BE FINISHED
    def get_calendar(self):
        datasets = [d["dataset_code"] for d in self.datasets_list()]

        for entry in self.parse_agenda():

            if entry['dataflow_key'] in datasets:

                yield {'action': 'update_node',
                       'kwargs': {'provider_name': self.provider_name,
                                  'dataset_code': entry['dataflow_key']},
                       'period_type': 'date',
                       'period_kwargs': {'run_date': datetime.strptime(
                           entry['scheduled_date'], "%d/%m/%Y %H:%M CET"),
                           'timezone': pytz.timezone('Asia/Tokyo')
                       }
                      }

    # TODO: load earlier versions to get revisions
    def load_datasets_first(self):
        start = time.time()        
        logger.info("datasets first load. provider[%s] - START" % (self.provider_name))
        
        
        self.provider.update_database()
        self.build_data_tree()
        self.upsert_data_tree()

        datasets_list = [d for d in self.get_selected_datasets().keys()]
        for dataset_code in datasets_list:
            try:
                self.upsert_dataset(dataset_code)
            except Exception as err:
                logger.fatal("error for dataset[%s]: %s" % (dataset_code, str(err)))

        end = time.time() - start
        logger.info("datasets first load. provider[%s] - END - time[%.3f seconds]" % (self.provider_name, end))

    def load_datasets_update(self):
        start = time.time()        
        logger.info("datasets first load. provider[%s] - START" % (self.provider_name))
        
        self.provider.update_database()
        self.upsert_data_tree()

        datasets_list = [d["dataset_code"] for d in self.datasets_list()]
        for dataset_code in datasets_list:
            try:
                self.upsert_dataset(dataset_code)
            except Exception as err:
                logger.fatal("error for dataset[%s]: %s" % (dataset_code, str(err)))

        end = time.time() - start
        logger.info("datasets first load. provider[%s] - END - time[%.3f seconds]" % (self.provider_name, end))

class EsriData():
    def __init__(self, dataset,  url, filename=None, store_filepath=None):
        self.provider_name = dataset.provider_name
        self.dataset_code = dataset.dataset_code
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        self.filename = filename
        self.store_filepath = store_filepath
        self.dataset_url = url
        self.panda_csv = self.get_csv_data()
#        self.release_date = self.get_release_date()
        self.release_date = dataset.last_update
        [self.nrow,self.ncol] = self.panda_csv.shape
        self.column_nbr = 0

        (self.frequency,
         self.start_date,
         self.end_date,
         self.first_row,
         self.last_row) = parse_dates(list(self.panda_csv.iloc[:,0]))

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
            
        return(download.get_filepath())
    
    def get_csv_data(self):
        return pandas.read_csv(self._load_datas(),header=None,encoding='cp932')
    
    def fix_series_names(self):
        #generating name of the series             
        columns = self.panda_csv.columns
        series_names = ['nan']*columns.size
        for column_ind in range(1,columns.size):
            if str(self.panda_csv.iloc[5,column_ind]) != "nan":
                name_first_part = str(self.panda_csv.iloc[5,column_ind])
            if self.first_row == 8 and str(self.panda_csv.iloc[6,column_ind]) != "nan":
                name_second_part = str(self.panda_csv.iloc[6,column_ind])
            if self.first_row == 8 and str(self.panda_csv.iloc[7,column_ind]) != "nan":
                series_names[column_ind] = (self.edit_seriesname(name_first_part + ', ' +
                                                                 name_second_part) + ', ' +
                                            self.edit_seriesname(str(self.panda_csv.iloc[7,column_ind])))
            elif str(self.panda_csv.iloc[6,column_ind]) != "nan":
                series_names[column_ind] = self.edit_seriesname(name_first_part + ', ' + str(self.panda_csv.iloc[6,column_ind]))
            elif str(self.panda_csv.iloc[5,column_ind]) != "nan":    
                series_names[column_ind] = self.edit_seriesname(name_first_part)
            #Take into the account FISIM 
            if str(self.panda_csv.iloc[6,column_ind-1]) == "Excluding FISIM":
                series_names[column_ind] = self.edit_seriesname(str(self.panda_csv.iloc[5,column_ind])+', '+str(self.panda_csv.iloc[6,column_ind-1]))               
            if str(self.panda_csv.iloc[6,column_ind-2]) == "Excluding FISIM":
                series_names[column_ind] = self.edit_seriesname(str(self.panda_csv.iloc[5,column_ind])+', '+str(self.panda_csv.iloc[6,column_ind-2]))
            if str(self.panda_csv.iloc[6,column_ind-3]) == "Excluding FISIM":
                series_names[column_ind] = self.edit_seriesname(str(self.panda_csv.iloc[5,column_ind])+', '+str(self.panda_csv.iloc[6,column_ind-3]))
            if series_names[column_ind] == 'Of Which Change in Inventories':
                series_names[column_ind] = 'Gross Capital Formation, Change in Inventories'
        lent = len(self.panda_csv.iloc[0,:])
        if str(self.panda_csv.iloc[0,:][columns.size-1]) == "(%)":
            self.currency = str(self.panda_csv.iloc[0,columns.size-2])
        else:
            self.currency = str(self.panda_csv.iloc[0,columns.size-1])
        return series_names

    def edit_seriesname(self,seriesname):
        seriesname = seriesname.replace(' ','')  
        seriesname = re.sub(r'([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))', r'\1 ', seriesname)
        seriesname = re.sub(r"((of)|(in) |(from/to)|(by)|(and) |(etc.))", r" \1 ", seriesname)  
        seriesname = re.sub(r"(&)", r" \1 ", seriesname)
        seriesname = re.sub(r",([A-z])",r", \1",seriesname)
#        seriesname = re.sub(r"(\()", r" \1", seriesname) 
        seriesname = seriesname.replace('Purchasesinthe','Purchases in the')
        seriesname = seriesname.replace('ResidentialInvestment','Residential Investment')        
        seriesname = seriesname.replace('S of tware','Software')        
        seriesname = seriesname.replace('  ',' ')
        m = re.match('(.+)\(.*\)',seriesname)
        if m:
            seriesname = m.group(1)
        m = re.match('\(.*\)(.+)',seriesname)
        if m:
            seriesname = m.group(1)
        seriesname = seriesname.strip()
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
        for r in range(self.first_row,self.last_row+1):
            series_value.append(str(column[r]).strip())    
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

    
