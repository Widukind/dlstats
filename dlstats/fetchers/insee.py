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
#from dlstats.fetchers.skeleton import Skeleton
from dlstats.fetchers.skeleton import Skeleton
from bson import ObjectId
from json import loads
import collections
from numpy import prod
import sys

class Insee(Skeleton):   
    """Class for managing INSEE data in dlstats"""
    def __init__(self):
        super().__init__()
        self.db = self.client.insee
        self.initial_page = "http://www.bdm.insee.fr/bdm2/index?request_locale=en"
        self.test_count = 0

    def get_categories(self,url):
        """Gets categories for INSEE BDM
        
        Stores a big theme in a category and call parser for 2nd page

        :param url: url of first page of INSEE BDM
        :type url: str
        :return: None"""

        fh = self.open_url_and_check(url)
        soup = BeautifulSoup(fh)
#        soup = BeautifulSoup(urllib.request.urlopen("http://localhost:8800/insee/Insee - Databases - BDM - Macro-economic database.html"))
        ul = soup.find(id="contenu").find('ul','liens')
        children = []
        for a in ul.find_all("a"):
            href = 'http://www.bdm.insee.fr'+a['href']
            children += self.get_page_2(href)
        node = {'name': 'insee',
                'code': '0',
                'children': children}
        self.bson_update(self.db.categories,node,'code')

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
                    node = {'name': name,
                            'code': code,
                            'children': None}
                    _id = self.bson_update(self.db.categories,node,'code')
                    return (None,_id)
            node = {'name': name,
                    'code': code,
                    'children': children}
            _id = self.bson_update(self.db.categories,node,'code')
            return (code1,_id)
        fh = self.open_url_and_check(url)
        page = BeautifulSoup(fh)
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
            if node['children'] == None:
                if not re.search(re.compile('Stopped series'),node['name']):
                    # limit number of datasets for testing
                    if self.test_count < 10:
                        try:
                            self.get_data(node['code'])
                        except:
                            print("CodeGroup {} can't be downloaded".format(node['code']))
                            print(sys.exc_info()[0])
                        else:
                            print(node['code'])
                        self.test_count += 1
            else:
                for id in node['children']:
                    walktree(id)
        node = self.db.categories.find_one({'name': 'insee'},{'_id': True})
        walktree(ObjectId(node['_id']))
    
    def get_data(self,code):
        """Get dataset for a given code"""

        dataset = dict()
        dataset['codes'] = set()
        dp = dataset_page(code)
        series = []
        for keys in dp:
            # no series are available in this chunk
            if len(keys) == 0:
                continue
            href = "http://www.bdm.insee.fr/bdm2/exporterSeries"
            params = urllib.parse.urlencode([('periode', 'toutes'),
                                             ('qualite', 'true'),
                                             ('chrono', 'true'),
                                             ('liste_formats', 'txt'),
                                             ('request_locale','en')]+
                                            [('idbank', k) for k in keys])
            params = params.encode('utf-8')
            fh = self.open_url_and_check(href,params)
            #            fh = urllib.request.urlopen("http://localhost:8800/insee/A.zip")
            buffer = BytesIO(fh.read())
            file = zipfile.ZipFile(buffer)
            (codes,series,s_offset) = self.get_charact_csv(file,code)
            (f,s,v) = self.get_values_csv(file,series,s_offset)
            for i in range(len(v)):
                series[i]['datasetCode'] = code
                series[i]['values'] = v[i]
                series[i]['status'] = f[i]
                series[i]['releaseDates'] = [series[i]['releaseDates'] for j in v[i]]
                series[i]['revisions'] = []
            # identical codes['status'] are recorded at each iteration ...
            codes['status'] = s
            dataset['codes'].update(codes)
            for s in series:
                self.series_update(self.db.series,s,'key')
        dataset.update(dp.get_dataset())
        self.bson_update(self.db.datasets,dataset,'datasetCode')

    def get_charact_csv(self,file,datasetCode):
        """Parse and store dataset parameters in Charact.csv"""
        
        series = []
        buffer = file.read('Charact.csv').decode(encoding='UTF-8')
        lines = buffer.split('\r\n')
        codes = collections.defaultdict(set)
        m = 0
        for line in lines:
            fields = re.split(r"[;](?![ ])",line)
            if m == 0:
                for i in range(len(fields)-1):
                    series += [{'name': fields[i+1]}]
            elif m == 1:
                for i in range(len(fields)-1):
                    series[i]['key'] = "{}.{}".format(datasetCode,fields[i+1])
            elif m == 2:
                for i in range(len(fields)-1):
                    series[i]['releaseDates'] = datetime.datetime.strptime(fields[i+1],'%B %d, %Y')
            elif m == 6: 
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
            elif m == 7:
                for i in range(len(fields)-1):
                    # Quaterly dates are in a special format
                    if series[i]['frequency'] == 'Q':
                        date = fields[i+1].split()
                        date[1] = str(3*(int(date[1])-1)+1)
                        series[i]['startDate'] = datetime.datetime.strptime(date[1]+' '+date[3],'%M %Y')
                    else:
                        series[i]['startDate'] = datetime.datetime.strptime(fields[i+1],datefmt)
            elif m == 8:
                for i in range(len(fields)-1):
                    # Quaterly dates are in a special format
                    if series[i]['frequency'] == 'Q':
                        date = fields[i+1].split()
                        date[1] = str(3*(int(date[1])-1)+1)
                        series[i]['endDate'] = datetime.datetime.strptime(date[1]+' '+date[3],'%M %Y')
                    else:
                        series[i]['endDate'] = datetime.datetime.strptime(fields[i+1],datefmt)
            elif m == 3:
                for i in range(len(fields)-1):
                    series[i]['codes'] = {}
                    series[i]['codes'][fields[0]] = fields[i+1]
                    codes[fields[0]].add(fields[i+1])
            else:
                for i in range(len(fields)-1):
                    series[i]['codes'][fields[0]] = fields[i+1]
                    codes[fields[0]].add(fields[i+1])
            m += 1
        # convert code set to list of identical pairs
        # Insee codes don't have short form
        for k in codes.keys():
            codes[k] = [(k1,k1) for k1 in codes[k]]
        return (codes,series,s_offset)

    def get_values_csv(self,file,series,s_offset):
        """Parse and store values in Values.csv"""

        buffer = file.read('Values.csv').decode(encoding='UTF-8')
        lines = buffer.split('\r\n')
        v = []
        f = []
        s = []
        m = 0;
        for line in lines:
            fields = re.split(r"[;](?![ ])",line)
            if m == 1:
                k = s_offset
                for i in range(len(series)):
                    if fields[k] != series[i]['key'].split('.')[1]:
                        print('key error in Values.csv',fields[i+s_offset],series[i]['key'])
                    k += 2
            elif m == 3:
                k = s_offset
                while (k < len(fields)):
                    v += [[re.sub(re.compile(','),'',fields[k])]]
                    f += [[fields[k+1]]]
                    k += 2
            elif (m > 3) and (len(fields[0]) > 0):
                k = s_offset
                i = 0
                while (k < len(fields)):
                    v[i] += [re.sub(re.compile(','),'',fields[k])]
                    f[i] += [fields[k+1]]
                    k += 2
                    i += 1
            elif (len(fields) > 2) and (len(fields[0]) == 0) and (len(fields[1]) == 0):
                s0 = fields[2].split(' : ')
                s.append([s0[0],s0[1]])
                if len(fields) > 3:
                    s0 = fields[3].split(' : ')
                    s.append([s0[0],s0[1]])
            m += 1
        return (f,s,v)

    def parse_agenda(self):
        """Parse agenda of new releases and schedule jobs"""
        
        DATEEXP = re.compile("(January|February|March|April|May|June|July|August|September|October|November|December)[ ]+\d+[ ]*,[ ]+\d+[ ]+\d+:\d+")
        #    url = 'http://www.insee.fr/en/publics/presse/agenda.asp'
        url = "http://localhost:8800/insee/agenda.html"
        fh = self.open_url_and_check(url)
        agenda = BeautifulSoup(fh)
        ul = agenda.find('div',id='contenu').find('ul','liens')
        for li in ul.find_all('li'):
            href = li.find('a')['href']
            groups = parse_theme(href)
            text = li.find('p','info').string
            date = datetime.datetime.strptime(DATEEXP.match(text).group(),'%B %d, %Y %H:%M')
            print(date)

    def parse_theme(self,url):
        """Find updated code groups"""
        
        url = "http://localhost:8800/insee/industrial_production.html"
        fh = self.open_url_and_check(url)
        theme = BeautifulSoup(fh)
        p = theme.find('div',id='savoirplus').find('p')
        groups = []
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
        page = BeautifulSoup(fh)
        #            page = BeautifulSoup(urllib.request.urlopen("http://localhost:8800/insee/rub"))
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
            code = id['name']
            size[code] = 0
            for option in field.find_all('option'):
                codes_nbr[code].append(option['value'])
                self.codes_desc[legend].append([option.string,option.string])
                size[code] += 1
                multiselect[code] = field.find('input')['name']

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
                    requests.append((multiselect[kstar],''))
                    for k in codes_nbr.keys():
                        if k != kstar:
                            requests += [(k,c2) for c2 in codes_nbr[k]]
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
                        requests.append((multiselect[ks[0]],''))
                        # Iterates over optimal chunks of intermediary size criterium
                        n1 = 0
                        for c2 in codes_nbr[ks[1]][0:size[ks[1]]:n]:
                            requests1 = requests + [(ks[1],c3) for c3 in codes_nbr[ks[1]][n1:(n1+n)]]
                            requests1.append((multiselect[ks[1]],''))
                            n1 += n
                            # Combine with largest criterium as a whole
                            requests1 += [(ks[2],c3) for c3 in codes_nbr[ks[2]]]
                            requests1.append((multiselect[ks[2]],''))
                            self.params.append(self.build_request_params(requests1))
                elif self.nbrCriterium == 4:
                    for c1 in codes_nbr[ks[0]]:
                        requests = []
                        requests.append((ks[0], c1))
                        requests.append((multiselect[ks[0]],''))
                        for c2 in codes_nbr[ks[1]]:
                            requests1 = requests + [(ks[1], c2)]
                            requests1.append((multiselect[ks[1]],''))
                            # Iterates over optimal chunks of intermediary size criterium
                            n1 = 0
                            for c3 in codes_nbr[ks[2]][0:size[ks[2]]:n]:
                                requests2 = requests1 + [(ks[2],c4) for c4 in codes_nbr[ks[2]][n1:(n1+n)]]
                                requests2.append((multiselect[ks[2]],''))
                                n1 += n
                                # Combine with largest criterium as a whole
                                requests2 += [(ks[3],c4) for c4 in codes_nbr[ks[3]]]
                                requests2.append((multiselect[ks[3]],''))
                                self.params.append(self.build_request_params(requests2))
        else:
            # one chunk is enough
            requests = []
            for k in codes_nbr.keys():
                requests += [(k,c) for c in codes_nbr[k]]
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
        page1 = BeautifulSoup(fh)
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
            #        href = "http://www.bdm/insee.fr/bdm2/affichageSeries?bouton=Download+file&"+ '&'.join(['idbank='+k for k in keys])
            # change set into dict for codes. INSEE doesn't have short names so key = value
        return (keys)

    def get_dataset(self):
        """Returns dataset data.
        
        Needs to be called last because lastUpdate is updated through the iterations."""
        
        dataset = {'datasetCode': self.dataset_code,
                   'doc_href': "http://www.bdm/insee.fr/bdm2/documentationGroupe/codeGroupe=" + self.dataset_code,
                   'name': self.dataset_name,
                   'codes': self.codes_desc,
                   'lastUpdate': self.lastUpdate,
                   'versionDate': datetime.datetime.now()}
        return(dataset)

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
    insee.update_datasets()
    #    insee.get_categories(self.initial_page)
    #    Insee.parse_agenda()             
    # get_data("http://localhost:8800/insee/A.zip")
    # get_data("http://localhost:8800/insee/Q.zip")
    # get_data("http://localhost:8800/insee/M.zip")
