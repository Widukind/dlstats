# -*- coding: utf-8 -*-

import logging
from datetime import datetime
import time
from collections import OrderedDict
import os
import json
import hashlib
import zipfile

import requests
from slugify import slugify

import pandas
import xlrd

from widukind_common import errors

from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats.utils import clean_datetime, get_ordinal_from_period, get_year
from dlstats.utils import Downloader, make_store_path
from dlstats import constants

logger = logging.getLogger(__name__)

VERSION = 2

HTTP_ERROR_SERVICE_CURRENTLY_UNAVAILABLE = 503 #Service currently unavailable

HTTP_ERROR_ENDPOINT_NOTFOUND = 400 #Endpoint “XXX” not found

#TODO: use for limit search: http://api.worldbank.org/v2/countries/wld/indicators/CHICKEN
ONLY_WORLD_COUNTRY = [
    "GMC",
]

#http://databank.worldbank.org/data/download/WDI_excel.zip
DATASETS = {
    'GEM': {
        'name': 'Global Economic Monitor',
        'doc_href': 'http://data.worldbank.org/data-catalog/global-economic-monitor',
        'url': 'http://databank.worldbank.org/data/download/GemDataEXTR.zip',
        'filename': 'GemDataEXTR.zip',
    },
}

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

class WorldBankAPI(Fetcher):
    
    def __init__(self, **kwargs):
        super().__init__(provider_name='WORLDBANK', version=VERSION, **kwargs)
        
        self.provider = Providers(name=self.provider_name,
                                 long_name='World Bank',
                                 version=VERSION,
                                 region='World',
                                 website='http://www.worldbank.org/',
                                 terms_of_use='http://data.worldbank.org/summary-terms-of-use',
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
        self._available_countries_by_name = None

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
                
        self.for_delete.append(filepath)
                
        with open(filepath) as f: #, mode='rb'
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

    #@property
    def available_countries_by_name(self):
        if self._available_countries_by_name:
            return self._available_countries_by_name
        
        self._available_countries_by_name = OrderedDict()

        for page in self.download_json('countries'):
            for source in page[1]:
                self._available_countries_by_name[source['name']] = source
        
        return self._available_countries_by_name

    #@property
    def available_countries(self):
        if self._available_countries:
            return self._available_countries
        
        self._available_countries = {}
        
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

        dataset = Datasets(provider_name=self.provider_name,
                           dataset_code=dataset_code,
                           name=dataset_settings["name"],
                           fetcher=self)
        
        if dataset_code in DATASETS:
            dataset.series.data_iterator = ExcelData(dataset, DATASETS[dataset_code]["url"])
            dataset.doc_href = DATASETS[dataset_code]["doc_href"]
        else:
            dataset.last_update = clean_datetime()
            dataset.series.data_iterator = WorldBankAPIData(dataset, dataset_settings)
        
        return dataset.update_database()
    
class WorldBankAPIData(SeriesIterator):

    def __init__(self, dataset, settings):
        super().__init__(dataset)
        
        self.wb_id = settings["metadata"]["id"]
        
        if self.dataset_code in ONLY_WORLD_COUNTRY:
            self.available_countries = {"WLD": self.fetcher.available_countries().get("WLD")}
        else:
            self.available_countries = self.fetcher.available_countries()
            
        self.dataset.dimension_keys = ["indicator", "country", "frequency"]

        self.dataset.concepts["country"] = "Country"
        self.dataset.concepts["indicator"] = "Indicator"
        self.dataset.concepts["frequency"] = "Frequency"
        
        self.dataset.codelists["country"] = dict([(k, c["name"]) for k, c in self.available_countries.items()])
        self.dataset.codelists["indicator"] = {}
        self.dataset.codelists["frequency"] = {}
        
        self.dataset.set_dimension_country("country")
        
        self.obs_status = {"E": "estimate", "F": "forecast"}
        
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
                    self.dataset.last_update = clean_datetime()
                
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
        series["last_update"] = self.release_date
        series['frequency'] = self._search_frequency(datas[0])
        
        series['key'] = "%s.%s.%s" % (self.current_indicator["id"], 
                                      self.current_country, 
                                      series['frequency'])
        
        series['name'] = "%s - %s - %s" % (self.current_indicator["name"], 
                                           self.available_countries[self.current_country]["name"], 
                                           constants.FREQUENCIES_DICT[series["frequency"]])

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
                'value': str(point["value"]).replace("None", ""),
                'ordinal': get_ordinal_from_period(point["date"], freq=series['frequency']), #tmp value
                'period': point["date"],
            }
            if not value_found and value["value"] != "":
                value_found = True
            
            if "obs_status" in point:
                obs_status = point.get("obs_status")
                if obs_status and len(obs_status) > 0:
                    value["attributes"] = {"obs_status": obs_status}
                    if not "obs_status" in self.dataset.codelists:
                        self.dataset.codelists["obs_status"] = self.obs_status
                    if not "obs_status" in self.dataset.concepts:
                        self.dataset.concepts["obs_status"] = "Observation Status"
                    if not "obs_status" in self.dataset.attribute_keys:
                        self.dataset.attribute_keys.append("obs_status")
            
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
        
        #PATCH
        for v in series['values']:
            v.pop("ordinal")

        series['dimensions'] = {
            'country': self.current_country,
            'indicator': self.current_indicator["id"],
            'frequency': series["frequency"]
        }
        if not self.current_indicator["id"] in self.dataset.codelists['indicator']:
            self.dataset.codelists['indicator'][self.current_indicator["id"]] = self.current_indicator["name"]
        
        if not series["frequency"] in self.dataset.codelists['frequency']:
            self.dataset.codelists['frequency'][series["frequency"]] = constants.FREQUENCIES_DICT[series["frequency"]]
            
        series['attributes'] = None
        
        self.dataset.add_frequency(series["frequency"])

        return series

class ExcelData(SeriesIterator):
    
    def __init__(self, dataset, url, is_autoload=True):
        SeriesIterator.__init__(self, dataset)
        
        self.release_date = None
        self.url = url
        self.available_countries = self.fetcher.available_countries_by_name()
        self.countries_not_found = set()
        self.manual_countries = self._get_manual_countries()

        if not "country" in self.dataset.dimension_keys:
            self.dataset.dimension_keys.append("country")

        if not "country" in self.dataset.concepts:
            self.dataset.concepts["country"] = "Country"
        
        if not "country" in self.dataset.codelists:
            self.dataset.codelists["country"] = {}
            
        self.dataset.set_dimension_country("country")
        
        if is_autoload:
            self._load_file()
            
        self.rows = self._get_datas()

    def _get_manual_countries(self):
        """
        http://data.worldbank.org/developers/api-overview/income-level-queries
        http://api.worldbank.org/v2/incomeLevels : not completed
        http://api.worldbank.org/v2/countries?incomeLevel=LMC : countries for this zone
        """
        return {
            'Anguilla': 'AIA', # google
            'Bahamas': 'BHS',
            'Developing Countries': 'LDC',
            'East Asia & Pacific developing': 'EAP',
            'Europe & Central Asia developing': 'ECA',
            'Latin America & Caribbean developing': 'LAC',
            'Tanzania, United Rep.': 'TZA',
            'World (WBG members)': 'WLD',
            'High Income Countries': 'HIC',
            'High Income: OECD': 'OEC',
            'High Income: Non-OECD': 'NOC',
            'Low Income': 'LIC',
            'Slovakia': 'SVK',
            'Middle East & N. Africa developing': 'MNA',
            'Lao, PDR': 'LAO',
            'Moldova, Rep.': 'MDA',
            'Montserrat': 'MSR', # google
            'Netherlands Antilles': 'ANT', #http://www.iso.org/iso/fr/home/news_index/news_archive/news.htm?refid=Ref1383
            'South Asia developing': 'SAAS',
            'Sub-Saharan Africa developing': 'SSA',
            'Middle Income Countries': 'MIC',
            'Developing Asia': 'WIDUKIND-ASIA-DEV', 
        }

    def _load_file(self):

        filename = "data-%s.zip" % (self.dataset_code)
        download = Downloader(url=self.url, 
                              filename=filename,
                              store_filepath=self.get_store_path(),
                              use_existing_file=self.fetcher.use_existing_file,)
        self.filepath, response = download.get_filepath_and_response()
        
        if self.filepath:
            self.fetcher.for_delete.append(self.filepath)

        release_date_str = response.headers['Last-Modified']
        #Last-Modified: Tue, 05 Apr 2016 15:05:11 GMT            
        self.release_date = clean_datetime(datetime.strptime(release_date_str, 
                                                      "%a, %d %b %Y %H:%M:%S GMT"))

        if self.dataset.last_update and self.dataset.last_update >= self.release_date:
            comments = "update-date[%s]" % self.release_date
            raise errors.RejectUpdatedDataset(provider_name=self.provider_name,
                                              dataset_code=self.dataset_code,
                                              comments=comments)
            
        self.dataset.last_update = self.release_date

    def _get_datas(self):

        _zipfile = zipfile.ZipFile(self.filepath)

        for fname in _zipfile.namelist():
            info = _zipfile.getinfo(fname)
            
            #bypass directory
            if info.file_size == 0 or info.filename.endswith('/'):
                continue

            if 'Commodity Prices' in fname:
                logger.warning("bypass %s" % fname)
                continue
            
            #if not self.release_date:
            #    last_update = clean_datetime(datetime(*self.zipfile.getinfo(fname).date_time[0:6]))
                
            series_name = fname[:-5]
            logger.info("open excel file[%s] - series.name[%s]" % (fname, series_name))
            
            excel_book = xlrd.open_workbook(file_contents = _zipfile.read(fname))
            
            for sheet in excel_book.sheets():
                if sheet.name in ['Sheet1','Sheet2','Sheet3','Sheet4', 'Feuille1','Feuille2','Feuille3','Feuille4']:
                    continue
    
                periods = sheet.col_slice(0, start_rowx=2)
                start_period = periods[0].value
                end_period = periods[-1].value
                
                frequency = None
                start_date = None
                end_date = None
                
                if sheet.name == 'annual':    
                    frequency = 'A'
                    start_date = get_ordinal_from_period(str(int(start_period)), freq='A')
                    end_date = get_ordinal_from_period(str(int(end_period)), freq='A')
                    periods = [str(int(p.value)) for p in periods]
                elif sheet.name == 'quarterly':    
                    frequency = 'Q'
                    start_date = get_ordinal_from_period(start_period,freq='Q')
                    end_date = get_ordinal_from_period(end_period,freq='Q')
                    periods = [p.value for p in periods]
                elif sheet.name == 'monthly':    
                    frequency = 'M'
                    start_date = get_ordinal_from_period(start_period.replace('M','-'),freq='M')
                    end_date = get_ordinal_from_period(end_period.replace('M','-'),freq='M')
                    periods = [p.value.replace('M','-') for p in periods]
                #elif sheet.name == 'daily':    
                #    frequency = 'D'
                #    start_date = self._translate_daily_dates(start_period)
                #    end_date = self._translate_daily_dates(end_period)
                #    TODO: periods = [p.value for p in periods]
                else:
                    msg = {"provider_name": self.provider_name, 
                           "dataset_code": self.dataset_code,
                           "frequency": sheet.name}
                    raise errors.RejectFrequency(**msg)
                
                self.dataset.add_frequency(frequency)
            
                columns = iter(range(1, sheet.row_len(0)))
                
                for column in columns:
                    settings = {
                        "column": column,
                        "sheet": sheet,
                        "periods": periods,
                        "series_name": series_name,
                        "bson": {
                            "frequency": frequency,
                            "start_date": start_date,
                            "end_date": end_date,
                        }
                    }
                    yield settings, None
                

    def _translate_daily_dates(self,value):
        date = xlrd.xldate_as_tuple(value, self.excel_book.datemode)
        return pandas.Period(year=date[0], month=date[1], day=date[2], freq=self.frequency)
        
    def _get_country(self, col_header):
        
        country = None
        country_item = None
        if col_header in self.available_countries:
            country = self.available_countries[col_header]["id"]
            country_item = col_header
        else:
            for k, v in self.manual_countries.items():
                if k.lower() == col_header.lower(): 
                    country = v
                    country_item = k

        if not country:
            logger.error("country not found [%s]" % col_header)
            raise Exception("country not found [%s]" % col_header)

        if country and not country in self.dataset.codelists["country"]:
            self.dataset.codelists["country"][country] = country_item
        
        return country

    def build_series(self, settings):
        column = settings["column"]
        sheet = settings["sheet"]
        periods = settings["periods"]
        series_name = settings["series_name"]
        bson = settings["bson"]
            
        dimensions = {}
        
        col_header = sheet.cell_value(0, column)
        dimensions['country'] = self._get_country(col_header)
        
        values = []
        _values = [str(v) for v in sheet.col_values(column, start_rowx=2)]
        
        for i, v in enumerate(_values):            
            value = {
                'attributes': None,
                'period': str(periods[i]),
                'value': str(v).replace(",", ".")    
            }
            values.append(value)
        
        bson['values'] = values                
        bson['name'] = series_name + ' - ' + col_header + ' - ' + constants.FREQUENCIES_DICT[bson['frequency']]
        
        series_key = slugify(bson['name'], save_order=True)

        bson['provider_name'] = self.provider_name
        bson['dataset_code'] = self.dataset_code
        bson['key'] = series_key
        bson['attributes'] = None
        bson['dimensions'] = dimensions
        bson['last_update'] = self.dataset.last_update
        
        return bson
    
