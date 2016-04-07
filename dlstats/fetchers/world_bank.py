# -*- coding: utf-8 -*-

import logging
from datetime import datetime
import time
from collections import OrderedDict
import os
import json
import hashlib

import requests
from slugify import slugify

from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats.utils import clean_datetime, get_ordinal_from_period
from dlstats import errors

logger = logging.getLogger(__name__)

VERSION = 1

HTTP_ERROR_SERVICE_CURRENTLY_UNAVAILABLE = 503 #Service currently unavailable

HTTP_ERROR_ENDPOINT_NOTFOUND = 400 #Endpoint “XXX” not found

#TODO: use for limit search: http://api.worldbank.org/v2/countries/wld/indicators/CHICKEN
ONLY_WORLD_COUNTRY = [
    "GMC",
]

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
        
        self.blacklist = [
            '13', # Enterprise Surveys
            '26', # Corporate scorecard # datacatalog id="89"    
            '29', # Global Social Protection
            '31', # Country Policy and Institutional Assessment (CPIA)
            '36', # Statistical Capacity Indicators # datacatalog id="8"
            '37', # LAC Equity Lab
            '41', # Country Partnership Strategy for India
            '44', # Readiness for Investment in Sustainable Energy (RISE)
            '45', # INDO-DAPOER
        ]
        
        """
        A Exclure:
        economycoverage: WLD, EAP, ECA, LAC, MNA, SAS, SSA, HIC, LMY, IBRD, IDA
        numberofeconomies: 214
        topics: 
        mobileapp: ???
        > Les données agrégés par régions sont aussi dans les countries mais avec un id="NA" dans region
        <wb:region id="NA">Aggregates</wb:region>
        """
        
        self._available_countries = None

    @retry(tries=5, sleep_time=2)
    def download_or_raise(self, url, params={}):
        
        if not os.path.exists(self.store_path):
            os.makedirs(self.store_path, exist_ok=True)
        
        filename = hashlib.sha224(url.encode("utf-8")).hexdigest()
        filepath = os.path.abspath(os.path.join(self.store_path, filename))
        if os.path.exists(filepath):
            os.remove(filepath)
                
        response = self.requests_client.get(url, params=params, stream=True)
        #response = requests.get(url, params=params)
        
        logger.info("download url[%s] - filepath[%s]" %  (response.url, filepath))

        response.raise_for_status()

        with open(filepath, mode='wb') as f:
            for chunk in response.iter_content():
                f.write(chunk)
                
        with open(filepath, mode='rb') as f:
            return json.load(f)
        
    def download_json(self, url, parameters={}):
        #TODO: settings
        per_page = 1000
        payload = {'format': 'json', 'per_page': per_page}
        payload.update(parameters)
        
        response_json = self.download_or_raise(self.api_url + url, params=payload)
        
        first_page = response_json#.json()
        if isinstance(first_page, list):
            number_of_pages = int(first_page[0]['pages'])
        else:
            number_of_pages = int(first_page['pages'])

        for page in range(1, number_of_pages + 1):
            if page != 1:
                payload = {'format': 'json', 'per_page': per_page, 'page': page}
                response_json = self.download_or_raise(self.api_url + url, params=payload)
            yield response_json#.json()

    @property
    def available_countries(self):
        if self._available_countries:
            return self._available_countries
        
        self._available_countries = OrderedDict()
        
        """
        {
            "id": "ABW",
            "iso2Code": "AW",
            "name": "Aruba",
            "region": 
        {
            "id": "LCN",
            "iso2code": "ZJ",
            "value": "Latin America & Caribbean (all income levels)"
        },
        "adminregion": 
        {
            "id": "",
            "iso2code": "",
            "value": ""
        },
        "incomeLevel": 
        {
            "id": "NOC",
            "iso2code": "XR",
            "value": "High income: nonOECD"
        },
        "lendingType": 
            {
                "id": "LNX",
                "iso2code": "XX",
                "value": "Not classified"
            },
            "capitalCity": "Oranjestad",
            "longitude": "-70.0167",
            "latitude": "12.5167"
        },        
        """
        
        for page in self.download_json('countries'):
            for source in page[1]:
                self._available_countries[source['id']] = source
        
        return self._available_countries


    def build_data_tree(self):
        """
        http://api.worldbank.org/v2/datacatalog?format=xml&per_page=20
        http://api.worldbank.org/v2/datacatalog/3?format=json&per_page=20

        > toujours le catalogue mais en limitant les champs:
        http://api.worldbank.org/v2/datacatalog/metatypes/name;type;acronym?format=json&per_page=200
        http://api.worldbank.org/v2/datacatalog/metatypes/type;url;lastrevisiondate?format=json&per_page=50

        > Voir si numberofeconomies = nombre de series ?
        
        > calendar: updatefrequency, updateschedule
        
        > use: detailpageurl pour doc_href
        
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
        
        position = 0
        
        for page in self.download_json('sources'):
            for source in page[1]:
        
                if source["id"] in self.blacklist:
                    continue
                
                position += 1
                
                cat = {
                    "provider_name": self.provider_name,
                    "category_code": source["code"],
                    "name": source["name"],
                    #TODO: "doc_href": ?,
                    "position": position,
                    "datasets": [{
                        "name": source["name"], 
                        "dataset_code": source["code"],
                        "last_update": None, 
                        "metadata": {"id": source["id"]}
                    }]
                }
                categories.append(cat)

        return categories
        
        """
        http://api.worldbank.org/v2/datacatalog?format=json&per_page=20

        FIXME: Par le catalogue: manque datasets. que:
        ADI                  | Africa Development Indicators                                          | 2013-02-22
        DB                   | Doing Business                                                         | 2015-11-24
        EdStats              | Education Statistics                                                   | 2016-03-04
        GEM                  | Global Economic Monitor                                                | 2016-03-22
        GEP                  | Global Economic Prospects                                              | 2016-01-06
        GFDD                 | Global Financial Development                                           | 2015-09-14
        GPE                  | GPE Results Forms Database                                             | 2013-01-10
        Global Findex        | Global Financial Inclusion (Global Findex) Database                    | 2015-04-15
        IDA                  | IDA Results Measurement System                                         | 2015-12-30
        IDS                  | International Debt Statistics                                          | 2015-12-16
        JOBS                 | Jobs                                                                   | 2015-09-21
        MDGs                 | Millennium Development Goals                                           | 2015-11-16
        QEDS/GDDS            | Quarterly External Debt Statistics GDDS (New)                          | 2016-01-28
        QEDS/SDDS            | Quarterly External Debt Statistics SDDS (New)                          | 2016-01-28
        SE4ALL               | Sustainable Energy for All                                             | 2015-09-09
        WDI                  | World Development Indicators                                           | 2016-02-17
        WGI                  | Worldwide Governance Indicators                                        | 2015-09-25        
        """
        
        for page in self.download_json('datacatalog'):
            
            for source in page["datacatalog"]:
                name = None
                is_time_series = False
                dataset_id = None
                dataset_code = None
                doc_href = None
                last_update = None
                metadata = {}
                for value in source["metatype"]:
                    if value["id"] == "type" and value["value"] == "Time series":
                        is_time_series = True
                    elif value["id"] == "name":
                        name = value["value"]
                    elif value["id"] == "acronym":
                        dataset_code = value["value"]
                    elif value["id"] == "apisourceid":
                        metadata["id"] = value["value"]
                        dataset_id = value["value"]
                    elif value["id"] == "detailpageurl":
                        doc_href = value["value"]
                    elif value["id"] == "lastrevisiondate":
                        print("Date: ", value["value"])
                        if value["value"].lower() == "current":
                            last_update = clean_datetime()
                        else:
                            try:    
                                last_update = clean_datetime(datetime.strptime(value["value"], '%d-%b-%Y')) #17-Feb-2016
                            except: 
                                pass
                    elif value["id"] == "updatefrequency":
                        metadata["updatefrequency"] = value["value"]  
                    elif value["id"] == "updateschedule":
                        metadata["updateschedule"] = value["value"]  
                
                if not dataset_id or is_time_series is False or not dataset_code or dataset_id in self.blacklist:
                    continue
                
                position += 1
                
                cat = {
                    "provider_name": self.provider_name,
                    "category_code": dataset_code,
                    "name": name,
                    "doc_href": doc_href,
                    "position": position,
                    "datasets": [{
                        "dataset_code": dataset_code,
                        "name": name, 
                        "last_update": last_update or clean_datetime(), 
                        "metadata": metadata
                    }]
                }
                categories.append(cat)
        
        return categories

    def upsert_dataset(self, dataset_code):
        
        self.get_selected_datasets()
        
        dataset_settings = self.selected_datasets[dataset_code]

        #http://data.worldbank.org/indicator/AG.AGR.TRAC.NO
        dataset = Datasets(provider_name=self.provider_name,
                           dataset_code=dataset_code,
                           name=dataset_settings["name"],
                           last_update=clean_datetime(),
                           fetcher=self)
        
        dataset.series.data_iterator = WorldBankAPIData(dataset, dataset_settings)
        
        return dataset.update_database()

class WorldBankAPIData(SeriesIterator):

    def __init__(self, dataset, settings):
        super().__init__(dataset)
        
        self.wb_id = settings["metadata"]["id"]
        
        if self.dataset_code in ONLY_WORLD_COUNTRY:
            self.available_countries = {"WLD": self.fetcher.available_countries.get("WLD")}
        else:
            self.available_countries = self.fetcher.available_countries
        
        self.dataset.dimension_keys = ["country"]
        self.dataset.concepts["country"] = "Country"
        self.dataset.codelists["country"] = dict([(k, c["name"]) for k, c in self.available_countries.items()])
        
        """
        Chaque entrée de series_listed:
        {
            "id": "CPTOTNSXN",
            "name": "CPI Price, nominal",
            "unit": "",
            "source": 
            {
                "id": "15",
                "value": "Global Economic Monitor"
            },
            "sourceNote": "The consumer price index reflects the change in prices for the average consumer of a constant basket of consumer goods. Data is not seasonally adjusted.",
            "sourceOrganization": "World Bank staff calculations based on Datastream data.",
            "topics": [ ]
        },        
        """
        self.indicators = self._download_indicators(self.wb_id)
        
        if not self.dataset.metadata:
            self.dataset.metadata = {}
        
        if not "indicators" in self.dataset.metadata:
            self.dataset.metadata["indicators"] = {}

        self.countries_to_process = list(self.available_countries.keys())
        #self.countries_str = ";".join(list(self.available_countries.keys()))
        
        self.blacklist_indicator = [
            "IC.DCP.COST",
            "IC.DCP.PROC",
            "IC.DCP.TIME",
            "IC.EC.COST",
            "IC.EC.PROC",
            "IC.EC.TIME",
            "IC.EXP.COST.EXP",
            "IS.ROD.DNST.K2",
            "IS.ROD.GOOD.MT.K6",
            "IS.ROD.PAVE.ZS",
            "IS.ROD.PSGR.K6",
            "IS.ROD.TOTL.KM",
            "IS.VEH.NVEH.P3",
            "IS.VEH.PCAR.P3",
            "IS.VEH.ROAD.K1",
            "TOT",
            "KSHRIMP_MEX", #GMC: error[Expecting value: line 1 column 1 (char 0)]
            "IC.EXP.COST.IMP",
            "IC.EXP.DOCS.IMP",
            "IC.EXP.TIME.EXP",
            "IC.EXP.TIME.IMP",
            "IC.GE.COST",
            "IC.GE.NUM",
            "IC.GE.TIME",
            "IC.ISV.COST",
            "IC.ISV.RECRT",
            "IC.LIC.NUM",
            "IC.LIC.TIME",
            "IC.PI.DIR",
            "IC.PI.DISCL",
            "IC.PI.INV",
            "IC.PI.SHAR",
            "IC.REG.CAP",
            "IC.REG.COST",
            "IC.RP.COST",
            "IC.RP.PROC",
            "IC.RP.TIME",            
        ]
        
        self.release_date = None
        self.current_indicator = None
        self.current_country = None
        
        self.rows = self._process()

    def _download_indicators(self, dataset_code):
        #TODO: si réutilisable, voir store
        """
        Indicator du dataset 15 ?

        http://api.worldbank.org/v2/indicators/CPTOTNSXN?format=json
        
        http://api.worldbank.org/v2/sources/15/indicators?format=json
        {
            "id": "CPTOTNSXN",
            "name": "CPI Price, nominal",
            "unit": "",
            "source": 
            {
                "id": "15",
                "value": "Global Economic Monitor"
            },
            "sourceNote": "The consumer price index reflects the change in prices for the average consumer of a constant basket of consumer goods. Data is not seasonally adjusted.",
            "sourceOrganization": "World Bank staff calculations based on Datastream data.",
            "topics": [ ]
        },            
                
        Values de l'indicator ci-dessus:
        http://api.worldbank.org/v2/countries/br/indicators/CPTOTNSXN?format=json
                
        """ 
        output = []
        for page in self.fetcher.download_json('/'.join(['sources',
                                                 dataset_code,
                                                 'indicators'])): #indicators or indicator ?
            for source in page[1]:
                output.append(source)
        return output

    def _download_values(self, country_code, indicator_code):
        """

        # définition d'un indicator :
        http://api.worldbank.org/v2/indicators?format=json
        15608 indicators - 313 pages
        
        > all: tous pays
        http://api.worldbank.org/v2/countries/all/indicators/IQ.CPA.ECON.XQ?format=json
        
        > values d'un indicator pour le BR (brésil)
        http://api.worldbank.org/countries/br/indicators/NY.GDP.MKTP.CD?format=json
        
        > Le champs lastupdated est dans les meta seulement en V2
        http://api.worldbank.org/v2/countries/br/indicators/NY.GDP.MKTP.CD?format=json
        
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
        """
        Pas de données: http://api.worldbank.org/v2/countries/all/indicators/DPANUSIFS?format=json&per_page=100
        [
            {
                "page": ​0,
                "pages": ​0,
                "per_page": ​0,
                "lastupdated": null,
                "total": ​0
            },
            null        
        ]        
        """
        
        datas = []
        release_date = None

        try:
            for page in self.fetcher.download_json('/'.join(['countries',
                                                     country_code,
                                                     'indicators',
                                                     indicator_code])):
                
                if not release_date:
                    release_date = page[0]['lastupdated']
                    
                datas.extend(page[1])
        
        except Exception as err:
            logger.critical("dataset[%s] - country[%s] - indicator[%s] - error[%s]" % (self.dataset_code,
                                                                                       country_code,
                                                                                       indicator_code,
                                                                                       str(err)))
        
        return release_date, datas

    def _process(self):
        
        for current_indicator in self.indicators:
            self.current_indicator = current_indicator
            
            #if not self.current_indicator["id"] == "CC.EST":
            #    continue
            
            count = 0
            
            if self.current_indicator["id"] in self.blacklist_indicator:
                continue
            
            is_release_controled = False
            is_rejected = False
            
            slug_indicator = slugify(self.current_indicator["id"], save_order=True)
            
            for current_country in self.countries_to_process:
                self.current_country = current_country
            
                logger.info("Fetching dataset[%s] - indicator[%s] - country[%s]" % (self.dataset_code, 
                                                                                    self.current_indicator["id"], 
                                                                                    self.current_country))
    
                release_date, datas = self._download_values(self.current_country,
                                                            self.current_indicator["id"])
            
                if not datas:
                    continue
                
                self.release_date = clean_datetime(datetime.strptime(release_date, '%Y-%m-%d'))

                if is_release_controled is False:
                    
                    is_release_controled = True
                    
                    if self.dataset.metadata["indicators"].get(slug_indicator):
                        
                        if self.release_date >= self.dataset.metadata["indicators"][slug_indicator]:
                            msg = "Reject series updated for provider[%s] - dataset[%s] - key[%s]"
                            logger.info(msg % (self.provider_name, 
                                               self.dataset_code, 
                                               self.current_indicator["id"]))
                            
                            is_rejected = True
                            break

                    self.dataset.metadata["indicators"][slug_indicator] = self.release_date
                
                count += 1
    
                yield {"datas": datas}, None
            
            if not is_rejected:
                logger.info("TOTAL - dataset[%s] - indicator[%s] - count[%s]" % (self.dataset_code,
                                                                                 self.current_indicator["id"],
                                                                                 count))
                if count == 0:
                    logger.warning("EMPTY dataset[%s] - indicator[%s]"  % (self.dataset_code,
                                                                       self.current_indicator["id"]))

        yield None, None

    def _search_frequency(self, data):
        if "Q" in data['date']:
            return 'Q' #2015Q3    #http://api.worldbank.org/v2/countries/FRA;DE/indicators/DT.AMT.DEAE.CD.IL.03.US?format=json
        elif len(data['date']) == 4:
            return 'A'
        elif len(data['date']) == 7:
            return 'M'
        else:
            return 'D'

    def build_series(self, datas):
        
        datas = datas["datas"]
        
        series = {}
        series['key'] = "%s.%s" % (self.current_indicator["id"], self.current_country)
        series['name'] = "%s - %s" % (self.current_indicator["name"], self.available_countries[self.current_country]["name"])
        series['frequency'] = self._search_frequency(datas[0])

        #if self.current_indicator.get("sourceNote"):
        #    series["notes"] = self.current_indicator.get("sourceNote")
        
        values = []
        value_found = False
        for point in datas:
            
            frequency = self._search_frequency(point)
            if frequency != series['frequency']:
                raise Exception("Diff frequency [%s] != [%s] - series[%s]" % (frequency, series['frequency'], series['key']))
            
            value = {
                'attributes': None,
                'release_date': self.release_date,
                'value': str(point["value"]).replace("None", ""),
                'ordinal': get_ordinal_from_period(point["date"], freq=series['frequency']),
                'period': point["date"],
            }
            if not value_found and value["value"] != "":
                value_found = True
            
            if "obs_status" in point:
                obs_status = point.get("obs_status")
                if obs_status and len(obs_status) > 0:
                    value["attributes"] = {"obs_status": obs_status}
            
            values.append(value)

        if not value_found:
            msg = {"provider_name": self.provider_name, 
                   "dataset_code": self.dataset_code}            
            raise errors.RejectEmptySeries(**msg)                

        keyfunc = lambda x: x["ordinal"]
        series['values'] = sorted(values, key=keyfunc)

        series['provider_name'] = self.provider_name
        series['dataset_code'] = self.dataset_code
                
        series['start_date'] = series['values'][0]["ordinal"]
        series['end_date'] = series['values'][-1]["ordinal"]

        series['dimensions'] = {'country': self.current_country}
        series['attributes'] = None

        return series

