# test limitation on line 98

from ._commons import Fetcher, Category, Series, Dataset, Provider, CodeDict, ElasticIndex
from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
from  urllib.error import URLError
import re
import datetime
import pymongo
import zipfile
from io import BytesIO, StringIO
import requests
import csv
from bson import ObjectId
from json import loads
import collections
from numpy import prod
import sys
import pandas

class Insee(Fetcher):   
    """Class for managing INSEE data in dlstats"""
    def __init__(self):
        super().__init__()
        self.initial_page = "http://www.bdm.insee.fr/bdm2/index?request_locale=en"
        self.test_count = 0
        self.provider_name = 'INSEE'
        self.provider = Provider(name=self.provider_name,website='http://www.insee.fr')
        self.data_url = "http://www.bdm.insee.fr/bdm2/exporterSeries"
        
    def get_categories(self,url):
        """Gets categories for INSEE BDM
        
        Stores a big theme in a category and call parser for 2nd page

        :param url: url of first page of INSEE BDM
        :type url: str
        :return: None"""

        fh = self.open_url_and_check(url)
        soup = BeautifulSoup(fh,"lxml")
#        soup = BeautifulSoup(urllib.request.urlopen("http://localhost:8800/insee/Insee - Databases - BDM - Macro-economic database.html"))
        ul = soup.find(id="contenu").find('ul','liens')
        children = []
        for a in ul.find_all("a"):
            href = 'http://www.bdm.insee.fr'+a['href']
            children += self.get_page_2(href)
        document = Category(provider=self.provider_name,name='root',children=children,categoryCode='0')
        document.update_database()

    def get_page_2(self,url):
        """Parser for  2nd level pages of INSEE BDM
        
        Walks tree of themes and groups; stores them in a category

        :param url: url of first page of INSEE BDM
        :type url: str
        :return: None"""
        def walktree(li):
            name = ''
            children = []
            for c in li.find_all(recursive=False):
                if c.name == 'ul':
                    code = c['id']
                    for l in c.find_all('li',recursive=False):
                        (code1,children1) = walktree(l)
                        children += [children1]
                elif c.name == 'span':
                    name = c.string
                elif c.name == 'a':
                    name = c.string
                    href = c['href']
                    code = href.split('=')[1]
                    document = Category(provider=self.provider_name,name=name,children=children,categoryCode=code,lastUpdate=datetime.datetime.now())
                    _id = document.update_database()
                    return (None,_id)
            document = Category(provider=self.provider_name,name=name,children=children,categoryCode=code,lastUpdate=datetime.datetime.now())
            _id = document.update_database()
            return (code1,_id)
        fh = self.open_url_and_check(url)
        page = BeautifulSoup(fh,"lxml")
#        page = BeautifulSoup(urllib.request.urlopen("http://localhost:8800/insee/1"))
        racine = page.find('ul',id='racine')
        children = []
        for l1 in racine.find_all('li',recursive=False):
            (code, children1) = walktree(l1)
            children += [children1]
        return(children)
                                    
    def update_datasets(self):
        """Updates datasets and series

        Walks the categories tree and update datasets that don't "Stopped series" in their name
        :return: None"""

        def walktree(_id):
            node = self.db.categories.find_one({'_id': _id})
            if len(node['children']) == 0:
                if not re.search(re.compile('Stopped series'),node['name']):
                    # !!!!!!!!!!!!!
                    # limit number of datasets for testing 
                    # !!!!!!!!!!!!!
                    if self.test_count < 20:
                        try:
                            self.get_data(node['categoryCode'])
                        except:
                            print("CodeGroup {} can't be downloaded".format(node['categoryCode']))
                            print(sys.exc_info()[0])
                        else:
                            print(node['categoryCode'])
                        self.test_count += 1
            else:
                for id in node['children']:
                    walktree(id)
        node = self.db.categories.find_one({'provider': self.provider_name, 'name': 'root'},{'_id': True})
        walktree(ObjectId(node['_id']))
    
    def get_data(self,datasetCode):
        """Get dataset for a given code"""

        dataset = Dataset(self.provider_name,datasetCode)
        data = InseeData(dataset,self.data_url)
        dataset.series.data_iterator = data
        dataset.name = data.dp.dataset_name
        dataset.doc_href = "http://www.bdm.insee.fr/bdm2/documentationGroupe?codeGroupe=" + datasetCode
        dataset.last_update = data.dp.lastUpdate
        
        dataset.update_database()
        es = ElasticIndex()
        es.make_index(self.provider_name,datasetCode)

    def parse_agenda(self):
        """Parse agenda of new releases and schedule jobs"""
        
        DATEEXP = re.compile("(January|February|March|April|May|June|July|August|September|October|November|December)[ ]+\d+[ ]*,[ ]+\d+[ ]+\d+:\d+")
        url = 'http://www.insee.fr/en/publics/presse/agenda.asp'
        # url = "http://localhost:8800/insee/agenda.html"
        fh = self.open_url_and_check(url)
        agenda = BeautifulSoup(fh,"lxml")
        ul = agenda.find('div',id='contenu').find('ul','liens')
        for li in ul.find_all('li'):
            href = li.find('a')['href']
            groups = self.parse_theme(href)
            text = li.find('p','info').string
            date = datetime.datetime.strptime(DATEEXP.match(text).group(),'%B %d, %Y %H:%M')
            print(href,groups,text,date)
            
    def parse_theme(self,url):
        """Find updated code groups"""
        
        #url = "http://localhost:8800/insee/industrial_production.html"
        #fh = self.open_url_and_cxheck(url)
        fh = self.open_url_and_check('http://www.insee.fr' + url)
        theme = BeautifulSoup(fh,"lxml")
        p = theme.find('div',id='savoirplus').find('p')
        groups = []
        print(p.find_all('a'))
        for a in p.find_all('a'):
            groups += [a.string[1:]]
        return groups

    def open_url_and_check(self,url,params=None):
        try:
            fh = urllib.request.urlopen(url,params)
        except URLError as e:
            if hasattr(e, 'reason'):
                print("Couldn't complete the request")
                print("Reason: ",e.reason)
            else:
                print("Couldn't reach the server")
                print("Code: ",e.code)
            raise
        else:
            return fh

class InseeData():
    def __init__(self,dataset,data_url):
        self.provider_name = dataset.provider_name
        self.dataset_code = dataset.dataset_code
        self.buffer = iter([])
        self.dp = dataset_page(self.dataset_code)
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        self.data_url = data_url
        
    def __iter__(self):
        return self

    def __next__(self):
        try:
            return(next(self.buffer))
        except StopIteration:
            self.load_series_in_buffer()
            return(next(self.buffer))

    def load_series_in_buffer(self):
        keys = next(self.dp)
        params = urllib.parse.urlencode([('periode', 'toutes'),
                                         ('qualite', 'true'),
                                         ('chrono', 'true'),
                                         ('liste_formats', 'txt'),
                                         ('request_locale','en')]+
                                        [('idbank', k) for k in keys])
        params = params.encode('utf-8')
        fh = Insee.open_url_and_check(self,self.data_url,params)
        #            fh = urllib.request.urlopen("http://localhost:8800/insee/values.zip")
        file_buffer = BytesIO(fh.read())
        file = zipfile.ZipFile(file_buffer)
        (series,s_offset) = self.get_charact_csv(file,self.dataset_code)
        (attributes,values) = self.get_values_csv(file,series,s_offset)
        self.buffer = iter([{'provider': self.provider_name,
                             'datasetCode': self.dataset_code,
                             'name': z[0]['name'],
                             'key': z[0]['key'],
                             'values': z[2],
                             'attributes': {'flags': z[1]},
                             'dimensions': z[0]['dimensions'],
                             'frequency': z[0]['frequency'],
                             'startDate': z[0]['startDate'],
                             'endDate': z[0]['endDate'],
                             'releaseDates': [z[0]['releaseDate'] for j in z[2]]}
                            for z in zip(series,attributes,values)])
                   
    def get_charact_csv(self,file,datasetCode):
        """Parse and store dataset parameters in Charact.csv"""
        
        series = []
        startDate = []
        endDate = []
        buffer = file.read('Charact.csv').decode(encoding='cp1252')
        lines = buffer.split('\r\n')
        m = 0
        for line in lines:
            fields = re.split(r"[;](?![ ])",line)
            if (fields[0] == 'Heading') or (fields[0] == 'Title'):
                for i in range(len(fields)-1):
                    name_parts = fields[i+1].split(' - ')
                    series.append({'name': fields[i+1],'dimensions': {}})
                    for j in range(len(name_parts)):
                        series[i]['dimensions']['code'+str(j+1)] = self.dimension_list.update_entry('code'+str(j+1),'',name_parts[j])
            elif fields[0] == 'IdBank':
                for i in range(len(fields)-1):
                    series[i]['key'] = fields[i+1]
            elif fields[0] == 'Last update':
                for i in range(len(fields)-1):
                    series[i]['releaseDate'] = datetime.datetime.strptime(fields[i+1],'%B %d, %Y')
            elif fields[0] == 'Periodicity': 
                for i in range(len(fields)-1):
                    if fields[i+1] == 'Annual':
                        series[i]['frequency'] = 'A'
                        # format to parse startDate and endDate
                        datefmt = '%Y'
                        # column offset in Values.zip
                        s_offset = 1
                    elif fields[i+1] == 'Quarterly':
                        series[i]['frequency'] = 'Q'
                        # column offset in Values.zip
                        s_offset = 2
                    elif fields[i+1] == 'Monthly':
                        series[i]['frequency'] = 'M'
                        # format to parse startDate and endDate
                        datefmt = '%B %Y'
                        # column offset in Values.zip
                        s_offset = 2
            elif (fields[0] == 'Beginning date') or (fields[0] == 'Start date'):
                for i in range(len(fields)-1):
                    # Quaterly dates are in a special format
                    if series[i]['frequency'] == 'Q':
                        date = fields[i+1].split()
                        series[i]['startDate'] = pandas.Period(date[3]+'Q'+ date[1],freq='quarterly').ordinal
                    else:
                        series[i]['startDate'] = pandas.Period(fields[i+1],freq=series[i]['frequency']).ordinal
            elif (fields[0] == 'Ending date') or (fields[0] == 'End date'):
                for i in range(len(fields)-1):
                    endDate.append(fields[i+1])
                    # Quaterly dates are in a special format
                    if series[i]['frequency'] == 'Q':
                        date = fields[i+1].split()
                        series[i]['endDate'] = pandas.Period(date[3]+'Q'+date[1],freq='quarterly').ordinal
                    else:
                        series[i]['endDate'] = pandas.Period(fields[i+1],freq=series[i]['frequency']).ordinal
            else:
                for i in range(len(fields)-1):
                    series[i]['dimensions'][fields[0]] = self.dimension_list.update_entry(fields[0],'', fields[i+1])

            m += 1
                
        return (series,s_offset)

    def get_values_csv(self,file,series,s_offset):
        """Parse and store values in Values.csv"""

        buffer = file.read('Values.csv').decode(encoding='cp1252')
        lines = buffer.split('\r\n')
        v = []
        f = []
        s = dict()
        m = 0;
        names = []
        for line in lines:
            fields = re.split(r"[;](?![ ])",line)
            if m == 0:
                # names is used to check for Flags
                # we keep the heading to have the same aligning as fields
                for i in range(len(fields)):
                    names.append(fields[i])
            elif m == 1:
                k = 0
                for i in range(s_offset,len(fields)):
                    if names[i] != 'Flags':
                        if fields[i] != series[k]['key']:
                            print('key error in Values.csv',fields[i],series[k]['key'])
                        else:
                            k += 1
            elif (m == 3) :
                for i in range(s_offset,len(fields)):
                    if names[i] == 'Flags':
                        f.append([fields[i]])
                    else:
                        v.append([re.sub(re.compile(','),'',fields[i])])
                        f.append([])
            elif (m > 3) and (len(fields[0]) > 0):
                k = 0;
                for i in range(s_offset,len(fields)):
                    if names[i] == 'Flags':
                        f[k-1].append(fields[i])
                    else:
                        v[k].append(re.sub(re.compile(','),'',fields[i]))
                        k += 1 
            elif (len(fields) > 2) and (len(fields[0]) == 0) and (len(fields[1]) == 0):
                s0 = fields[2].split(' : ')
                s[s0[0]] = s0[1]
                if len(fields) > 3:
                    s0 = fields[3].split(' : ')
                    self.attribute_list.update_entry('flags',s0[0],s0[1])
            m += 1
        return (f,v)

class dataset_page(Insee):
    """Iterator serves variable keys in chunks small enough for Insee web site."""

    def __init__(self,dataset_code):
        """Parse codeGroupe page and stores parameters"""
        
        # Value of codeGroupe for this dataset
        self.dataset_code = dataset_code
        # Iterator counter
        self.iter = 0
        # Time of the variable last updated initialization
        self.lastUpdate = datetime.datetime(1900,1,1)
        
        # Parse parameters
        url = "http://www.bdm.insee.fr/bdm2/choixCriteres?request_locale=en&codeGroupe=" + dataset_code
        fh = Insee.open_url_and_check(self,url)
        page = BeautifulSoup(fh,"lxml")
        #        page = BeautifulSoup(urllib.request.urlopen("http://localhost:8800/insee/rub"))
        h1 = page.find('h1')
        self.dataset_name = h1.string
        f = page.find('form',id='listeSeries')
        codes_nbr = collections.defaultdict(list)
        self.codes_desc = collections.defaultdict(list)
        multiselect = {}
        size = {}
        self.nbrCriterium = 0
        for field in f.find_all('fieldset'):
            self.nbrCriterium += 1
            legend = field.find('legend').string
            legend = re.match(re.compile('(.*) (\(.*\))'),legend).group(1)
            id = field.find('select')
            if id is None:
                for input in field.find_all('input'):
                    code = input['name']
                    size[code] = 1
                    codes_nbr[code].append(input['value'])
                    label = field.find('label')
                    self.codes_desc[legend].append([label.string,label.string])
                    size[code] += 1
            else:
                code = id['name']
                size[code] = 0
                for option in field.find_all('option'):
                    if "selected" in option:
                        codes_nbr[code] = option['value']
                        self.codes_desc[legend] = [option.string,option.string]
                        break
                    else:
                        codes_nbr[code].append(option['value'])
                        self.codes_desc[legend].append([option.string,option.string])
                    size[code] += 1
                multiselect[code] = field.find('input')['name']

        if dataset_code == '158':
            self.params = self.params_158(codes_nbr,multiselect)
            return
        
        # Establish heuristic iteration stategy so as not to request more than 100 variables at a time
        # Implementation is limited to 3 criteria or less
        if self.nbrCriterium > 4:
            raise TooManyCriteriaError("Dataset_code: {} has {} Criteria.".format(self.dataset_code,self.nbrCriterium))
        # Storage for request parameters
        self.params = []
        # Total number of variables    
        total_size = 1
        for s in size.values():
            total_size *= s
        # Different strategies to build requests and construct params
        if total_size > 100:
            sstar = 0
            kstar = []
            for k in size.keys():
                s = total_size/size[k]
                if (s < 100) and (s > sstar):
                    # Search for smallest criterium that let us
                    # get chunks of less than 100 variables by
                    # combining the two other criteria
                    sstar = s
                    kstar = k
            if sstar > 0:
                # Run one iteration around optimal criterium
                for c1 in codes_nbr[kstar]:
                    requests = []
                    requests.append((kstar, c1))
                    if kstar in multiselect:
                        requests.append((multiselect[kstar],''))
                    for k in codes_nbr.keys():
                        if k != kstar:
                            requests += [(k,c2) for c2 in codes_nbr[k]]
                            if k in multiselect:
                                requests.append((multiselect[k],''))
                            self.params.append(self.build_request_params(requests))
            else:
                # Run iterations on two criteria
                # Assumes that no criterium is larger than 100 
                # Criterium keys sorted by size of criterium
                ks = sorted(size,key=lambda k:size[k])
                # Number of times the larges criterium can be
                # read before reaching 100 variables
                n = round(100/size[ks[self.nbrCriterium-1]]-0.5)
                # Iterate over smallest criteria
                if self.nbrCriterium == 3:
                    for c1 in codes_nbr[ks[0]]:
                        requests = []
                        requests.append((ks[0], c1))
                        if ks[0] in multiselect:
                            requests.append((multiselect[ks[0]],''))
                        # Iterates over optimal chunks of intermediary size criterium
                        n1 = 0
                        for c2 in codes_nbr[ks[1]][0:size[ks[1]]:n]:
                            requests1 = requests + [(ks[1],c3) for c3 in codes_nbr[ks[1]][n1:(n1+n)]]
                            if ks[1] in multiselect:
                                requests1.append((multiselect[ks[1]],''))
                            n1 += n
                            # Combine with largest criterium as a whole
                            requests1 += [(ks[2],c3) for c3 in codes_nbr[ks[2]]]
                            if ks[2] in multiselect:
                                requests1.append((multiselect[ks[2]],''))
                            self.params.append(self.build_request_params(requests1))
                elif self.nbrCriterium == 4:
                    for c1 in codes_nbr[ks[0]]:
                        requests = []
                        requests.append((ks[0], c1))
                        if ks[0] in multiselect:
                            requests.append((multiselect[ks[0]],''))
                        for c2 in codes_nbr[ks[1]]:
                            requests1 = requests + [(ks[1], c2)]
                            if ks[1] in multiselect:
                                requests1.append((multiselect[ks[1]],''))
                            # Iterates over optimal chunks of intermediary size criterium
                            n1 = 0
                            for c3 in codes_nbr[ks[2]][0:size[ks[2]]:n]:
                                requests2 = requests1 + [(ks[2],c4) for c4 in codes_nbr[ks[2]][n1:(n1+n)]]
                                if ks[2] in multiselect:
                                    requests2.append((multiselect[ks[2]],''))
                                n1 += n
                                # Combine with largest criterium as a whole
                                requests2 += [(ks[3],c4) for c4 in codes_nbr[ks[3]]]
                                if ks[3] in multiselect:
                                    requests2.append((multiselect[ks[3]],''))
                                self.params.append(self.build_request_params(requests2))
        else:
            # one chunk is enough
            requests = []
            for k in codes_nbr.keys():
                requests += [(k,c) for c in codes_nbr[k]]
                if k in multiselect:
                    requests.append((multiselect[k],''))
            self.params.append(self.build_request_params(requests))
        
    def build_request_params(self,requests):
        """Builds request params to get variables page"""
        
        requests += [('codeGroupe',self.dataset_code)]
        requests += [('nombreCriteres',self.nbrCriterium)]
        requests += [('request_locale','en')]
        params = urllib.parse.urlencode(requests)
        params = params.encode('utf-8')
        return params

    def __iter__(self):
        return self

    def __next__(self):
        if self.iter == len(self.params):
            raise StopIteration
        url = "http://www.bdm.insee.fr/bdm2/listeSeries"
        fh = self.open_url_and_check(url,self.params[self.iter])
        page1 = BeautifulSoup(fh,"lxml")
        #            page1 = BeautifulSoup(urllib.request.urlopen("http://localhost:8800/insee/3"))
        self.iter += 1
        tbody = page1.find("tbody")
        keys = []
        if tbody is None:
            # no series are available for these criteria
            return keys
        lastUpdate = []
        for tr in tbody.find_all("tr"):
            td = tr.find_all('td')
            keys += [td[1].string]
            lastUpdate += [datetime.datetime.strptime(td[5].string,'%Y-%m-%d')]
            self.lastUpdate = max(lastUpdate+[self.lastUpdate])
        return (keys)

    def get_dataset(self):
        """Returns dataset data.
        
        Needs to be called last because lastUpdate is updated through the iterations."""
        
        dataset = {'datasetCode': self.dataset_code,
                   'docHref': "http://www.bdm/insee.fr/bdm2/documentationGroupe/codeGroupe=" + self.dataset_code,
                   'name': self.dataset_name,
                   'lastUpdate': self.lastUpdate,
                   'versionDate': datetime.datetime.now()}
        return(dataset)
    
    def params_158(self,codes_nbr,multiselect):
        params = []
        for c359 in ['10', '11']:
            if c359 == '10':
                codes3435 = ['10']
            else:
                codes3435 = ['10', '11']
            for c3435 in codes3435:
                increment = round(len(codes_nbr['358'])/4)+1
                m1 = 0
                m2 = increment
                for i in range(4):
                    request = [('359', c359),('3435', c3435),(multiselect['3435'],'')]
                    for j in range(m1,m2):
                        request.append((358,codes_nbr['358'][j]))
                    request.append((multiselect['358'],''))
                    params.append(self.build_request_params(request))
                    m1 += increment
                    m2 = min(len(codes_nbr['358']),m2+increment)
        return(params)
                    
            
class InseeError(Exception):
    """Base class for exception in Insee fetcher."""
    pass

class TooManyCriteriaError(InseeError):
    """Exception raised in dataset_page()."""

    def __init__(self,message):
        self.message = message
        print(message)

class CodeGroupError(InseeError):
    """Exception raised when CodeGroup can't be downloaded"""

    def __init__(self,message):
        self.message = message
        print(message)

if __name__ == "__main__":
    insee = Insee()
    #    insee.get_categories(insee.initial_page)
    #    HPCI
    insee.get_data('158')
    #    insee.get_data('1427')
    #insee.get_data('1430')
    #    insee.update_datasets()
    # insee.parse_agenda()             
    #    insee.get_data("http://localhost:8800/insee/A.zip")
    #    insee.get_data("http://localhost:8800/insee/Q.zip")
    #    insee.get_data("http://localhost:8800/insee/M.zip")
