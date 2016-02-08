# -*- coding: utf-8 -*-

import logging
import datetime
import time
from collections import OrderedDict

import requests

from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats.utils import clean_datetime, get_ordinal_from_period, Downloader

logger = logging.getLogger(__name__)

VERSION = 1

def key_monthly(point):
    "Key function for sorting dates of the format 2008M12"
    string_month = point[0]
    year, month = string_month.split('M')
    return int(year)*100+int(month)

def key_yearly(point):
    "Key function for sorting dates of the format 2008"
    string_year = point[0]
    return int(string_year)

def retry(tries=1, sleep_time=2):
    """Retry calling the decorated function
    :param tries: number of times to try
    :type tries: int
    """
    def try_it(func):
        def f(*args,**kwargs):
            attempts = 0
            while True:
                try:
                    return func(*args,**kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts > tries:
                        raise e
                    time.sleep(sleep_time)
        return f
    return try_it

"""
Incoming level:
    http://data.worldbank.org/developers/api-overview/income-level-queries
    <wb:incomeLevel id="HIC">High income</wb:incomeLevel> 
    Income levels show the income category of a particular country as identified by ..
    
http://api.worldbank.org/countries?incomeLevel=LMC    
"""

class WorldBankAPI(Fetcher):
    
    def __init__(self, **kwargs):
        super().__init__(provider_name='WORLDBANK', version=VERSION, **kwargs)
        
        self.provider = Providers(name=self.provider_name,
                                 long_name='World Bank',
                                 version=VERSION,
                                 region='World',
                                 website='http://www.worldbank.org/',
                                 fetcher=self)

        self.api_url = 'http://api.worldbank.org/v2/'
        
        self.requests_client = requests.Session()
        
        self.blacklist = {'15': ['TOT']}
        #self.whitelist = ['1', '15']
        """
        26 Corporate scorecard # datacatalog id="89"
        36 Statistical Capacity Indicators # datacatalog id="8"
        37 LAC Equity Lab
        31 Country Policy and Institutional Assessment (CPIA)
        45 INDO-DAPOER
        41 Country Partnership Strategy for India
        13 Enterprise Surveys
        29 Global Social Protection
        44 Readiness for Investment in Sustainable Energy (RISE)
        
        economycoverage: WLD, EAP, ECA, LAC, MNA, SAS, SSA, HIC, LMY, IBRD, IDA
        numberofeconomies: 214
        topics: 
        mobileapp: ???
        
        > Les données agrégés par régions sont aussi dans les countries mais avec un id="NA" dans region
        <wb:region id="NA">Aggregates</wb:region>
        """
        
        self._available_countries = None

    @retry(tries=2, sleep_time=2)
    def download_or_raise(self, url, params={}):
        print("URL : ", url)
        #TODO: stream
        response = self.requests_client.get(url, params=params)
        pprint(response.headers)
        #TODO: ?
        response.raise_for_status()
        return response 

    def download_json(self, url, parameters={}):
        #TODO: settings
        per_page = 30000
        payload = {'format': 'json', 'per_page': per_page}
        payload.update(parameters)
        
        request = self.download_or_raise(self.api_url + url, params=payload)
        
        first_page = request.json()
        number_of_pages = int(first_page[0]['pages'])

        for page in range(1, number_of_pages+1):
            if page != 1:
                payload = {'format': 'json', 'per_page': per_page, 'page': page}
                request = self.download_or_raise(self.api_url + url, params=payload)
                #TODO: ?
                request.raise_for_status()
            yield request.json()

    def download_indicator(self, country_code, indicator_code):
        """
        http://api.worldbank.org/v2/countries/FRA/indicators/NY.GDP.PCAP.CD?format=json        
        
        sample values in series:
        [
            {
                "indicator": {
                    "id": "NY.GDP.PCAP.CD",
                    "value": "GDP per capita (current US$)"
                },
                "country": {
                    "id": "FR",
                    "value": "France"
                },
                "countryiso3code": "FRA",
                "date": "2015",
                "value": null,
                "unit": "",
                "obs_status": "",
                "decimal": ​1
            },        
        ]        
        """
        for page in self.download_json('/'.join(['countries',
                                                 country_code,
                                                 'indicators',
                                                 indicator_code])):
            yield page

    """
    def datasets_list(self):
        output = []
        for page in self.download_json('sources'):
            for source in page[1]:
                output.append(source['id'])
        return output
    """
    
    def _download_datasets_list(self):
        """
        TODO: Remplacer id par code et mettre code en meta ?
        
        {
            "id": "15",
            "name": "Global Economic Monitor",
            "code": "GEM",
            "description": "",
            "url": "",
            "dataavailability": "Y",
            "metadataavailability": "Y"
        }
        """
        output = []
        for page in self.download_json('sources'):
            for source in page[1]:
                output.append((source['id'], source['name']))
        return output

    @property
    def available_countries(self):
        if self._available_countries:
            return self._available_countries
        
        self._available_countries = OrderedDict()
        for page in self.download_json('countries'):
            for source in page[1]:
                self._available_countries[source['id']] = source['name']
        
        return self._available_countries

    def series_list(self,dataset_code):
        #TODO: si réutilisable, voir store
        """
        # définition d'un indicator :
        
        http://api.worldbank.org/v2/indicators?format=json
        15608 indicators - 313 pages
        
        > values d'un indicator pour le BR (brésil)
        http://api.worldbank.org/countries/br/indicators/NY.GDP.MKTP.CD?format=json
        
        http://api.worldbank.org/v2/indicators/NY.GDP.MKTP.CD?format=json
        {
            id": "1.0.HCount.1.25usd",
            "name": "Poverty Headcount ($1.25 a day)",
            "unit": "",
            "source": {
                "id": "37",
                "value": "LAC Equity Lab"
            
            },
            "sourceNote": "The poverty headcount index measures the proportion of the population with daily per capita income below the poverty line.",
            "sourceOrganization": "LAC Equity Lab tabulations of SEDLAC (CEDLAS and the World Bank).",
            "topics": [
                {
                    "id": "11",
                    "value": "Poverty "
                }            
            ]        
        
        [
            {
                "id": "CPTOTNSXN",
                "name": "CPI Price, nominal",
                "source": 
                {
                
                    "id": "15",
                    "value": "Global Economic Monitor"
                
                },
                "sourceNote": "The consumer price index reflects the change in prices for the average consumer of a constant basket of consumer goods. Data is not seasonally adjusted.",
                "sourceOrganization": "World Bank staff calculations based on Datastream data.",
                "topics": [ ]
            },
        ]        
        """ 
        output = []
        for page in self.download_json('/'.join(['sources',
                                                 dataset_code,
                                                 'indicators'])): #indicators or indicator ?
            for source in page[1]:
                output.append(source['id'])
        return output

    def build_data_tree(self):
        """
        http://api.worldbank.org/v2/datacatalog?format=xml&per_page=20
        http://api.worldbank.org/v2/datacatalog/3?format=json&per_page=20

        > toujours le catalogue mais en limitant les champs:
        http://api.worldbank.org/v2/datacatalog/metatypes/name;type;acronym?format=json&per_page=200
        http://api.worldbank.org/v2/datacatalog/metatypes/type;url;lastrevisiondate?format=json&per_page=50
        
        datacatalog": [
            {
                id": "3",
                "metatype": [
                    {
                    "id": "name",
                    "value": "Global Economic Monitor"
                    },
                    {
                        "id": "acronym",
                        "value": "GEM"
                    },
                    {
                        "id": "description",
                        "value": "Providing...."
                    },
                    {
                        "id": "url",
                        "value": "http://databank.worldbank.org/data/views/variableselection/selectvariables.aspx?source=global-economic-monitor-(gem)"                        
                    },
                    {
                        "id": "apisourceid",    !!! lien avec id source !
                        "value": "15"
                    }                    
                    
            },         
        ]        
        """

        categories = []
        
        for source in self._download_datasets_list():
            cat = {
                "category_code": source[0],
                "name": source[1],
                #TODO: "doc_href": ?,
                "datasets": [{
                    "name": source[1], 
                    "dataset_code": source[0],
                    "last_update": None, 
                    "metadata": None
                }]
            }
            categories.append(cat)

        return categories

    def upsert_dataset(self, dataset_code):

        self.get_selected_datasets()
        
        dataset_settings = self.selected_datasets[dataset_code]

        dataset = Datasets(provider_name=self.provider_name,
                           dataset_code=dataset_code,
                           name=dataset_settings["name"],
                           last_update=clean_datetime(),
                           fetcher=self)
        
        dataset.series.data_iterator = WorldBankAPIData(dataset)
        return dataset.update_database()

class WorldBankAPIData(SeriesIterator):

    def __init__(self, dataset):
        super().__init__(dataset)
        
        self.available_countries = self.fetcher.available_countries
        
        dimension_list = OrderedDict()
        dimension_list['country'] = self.available_countries
        self.dimension_list.set_dict(dimension_list)
        
        #TODO: simplifier 
        self.blacklisted_indicators = []
        if self.dataset_code in self.fetcher.blacklist:
            self.blacklisted_indicators = self.fetcher.blacklist[self.dataset_code]
        
        self.series_listed = self.fetcher.series_list(self.dataset_code)
        #TODO: simplifier 
        self.series_to_process = list(set(self.series_listed) - set(self.blacklisted_indicators))
        
        self.countries_to_process = []
        self.i=0

    def _process(self):
        for row in self._rows:
            yield row, None

    def __next__(self):
        #TODO: Check for NaNs
        series = {}

        if not self.countries_to_process:
            if not self.series_to_process:
                yield None, None
                #raise StopIteration()
            
            self.countries_to_process = list(self.available_countries.keys())
            self.current_series = self.series_to_process.pop()

        self.current_country = self.countries_to_process.pop()
        logger.debug("Fetching the series {0} for the country {1}"
                     .format(self.current_series, self.current_country))
        # Only retrieve the first page to get more information about the series
        indicator = self.fetcher.download_indicator(self.current_country,
                                                    self.current_series)

        dates_and_values = []
        #dates = []
        has_page = False
        for page in indicator:
            has_page = True
            self.release_date = page[0]['lastupdated']
            for point in page[1]:
                if len(point['date']) == 4:
                    series['frequency'] = 'A'
                if len(point['date']) == 7:
                    series['frequency'] = 'M'
                series['name'] = point['indicator']['value']
                break
            break
        if has_page == False:
            return self.__next__()
        # Then proceed with all the pages
        indicator = self.fetcher.download_indicator(self.current_country,
                                                    self.current_series)
        for page in indicator:
            for point in page[1]:
                dates_and_values.append((point['date'],point['value']))

        series['dates_and_values'] = dates_and_values
        return self.build_series(series)

    def build_series(self, series):
        bson = {}

        dates_and_values = series.pop('dates_and_values')

        if series['frequency'] == 'A':
            key_function = key_yearly
        elif series['frequency'] == 'M':
            key_function = key_monthly
        
        bson["frequency"] = series['frequency']

        dates_and_values = sorted(dates_and_values, key=key_function)

        values = []
        for point in dates_and_values:
            value = {
                'attributes': None,
                'release_date': datetime.datetime.strptime(self.release_date, '%Y-%m-%d'),
                'value': str(point[1]) or 'NaN',
                'ordinal': get_ordinal_from_period(point[0],
                                         freq=series['frequency']),
                'period': point[0],
                'period_o': point[0]
            }
            values.append(value)

        series['provider_name'] = self.provider_name
        series['dataset_code'] = self.dataset_code
        series['key'] = "%s.%s" % (self.current_series, self.current_country)
        series['name'] = series['key']
        series['values'] = values
        series['start_date'] = get_ordinal_from_period(dates_and_values[0][0],
                                            freq=series['frequency'])
        series['end_date'] = get_ordinal_from_period(dates_and_values[-1][0],
                                          freq=series['frequency'])
        series['attributes'] = None
        series['dimensions'] = {'country': self.current_country}

        self.i += 1

        return series


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG,
                        filename="wbapi.log",
                        format='line:%(lineno)d - %(asctime)s %(name)s: [%(levelname)s] - [%(process)d] - [%(module)s] - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    from pprint import pprint
    import sys
    import os
    import tempfile
    print("WARNING : run main for testing only", file=sys.stderr)
    try:
        import requests_cache
        cache_filepath = os.path.abspath(os.path.join(tempfile.gettempdir(), 'wb.cache'))
        requests_cache.install_cache(cache_filepath, backend='sqlite', expire_after=None)#=60 * 60) #1H
        print("requests cache in %s" % cache_filepath)
    except ImportError:
        pass
    
    response = requests.get("http://api.worldbank.org/countries/FRA/indicators/TOTRESV?format=json")

    wb = WorldBankAPI()
    for d in wb.datasets_list():
        print(d["dataset_code"], d["name"])
    #for d in wb.datasets_long_list():
        #print(d)
    """
    1 Doing Business
    11 Africa Development Indicators
    12 Education Statistics
    13 Enterprise Surveys
    14 Gender Statistics
    15 Global Economic Monitor
    16 Health Nutrition and Population Statistics    
    """
    #wb.build_data_tree()
    #wb.build_data_tree()
    #pprint(wb.available_countries)
    pprint(wb.series_list("15"))
    """
    ['CPTOTNSXN',
 'CPTOTNSXNZGY',
 'CPTOTSAXMZGY',
 'CPTOTSAXN',
 'CPTOTSAXNZGY',
    """

"""
http://api.worldbank.org/countries/all/indicators/SP.POP.TOTL?format=json
    "page": ​1,
    "pages": ​278,
    "per_page": "50",
    "total": ​13888

http://api.worldbank.org/countries/all/indicators/SP.POP.TOTL?format=json&page=278
    "page": ​278,
    "pages": ​278,
    "per_page": "50",
    "total": ​13888
    
http://api.worldbank.org/countries/FRA/indicators/TOTRESV?format=json
    "page": ​1,
    "pages": ​1,
    "per_page": "50",
    "total": ​25    
"""

