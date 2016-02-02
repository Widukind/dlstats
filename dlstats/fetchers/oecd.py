# -*- coding: utf-8 -*-

from collections import OrderedDict
import logging
import time
import tempfile
import os

import json

from dlstats.utils import Downloader, clean_datetime
from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats import errors

VERSION = 2

CACHE_EXPIRE = 60 * 60 * 4 #4H

logger = logging.getLogger(__name__)

"""
Errors are not returned in the JSON format but HTTP status codes and messages are set according to the Web Services Guidelines
401 Unauthorized is returned if a non-authorised dataset is requested
"""

DATASETS = {
    'MEI': { 
        'name': 'Main Economics Indicators',
        'doc_href': 'http://www.oecd-ilibrary.org/economics/data/main-economic-indicators_mei-data-en',
        'sdmx_filter': "LOCATION"
    },
    'EO': { 
        'name': 'Economic Outlook',
        'doc_href': 'http://www.oecd.org/eco/outlook/',
        'sdmx_filter': "LOCATION"
    },
}

SDMX_DATA_HEADERS = {
    "Accept": "application/vnd.sdmx.draft-sdmx-json+json;version=2.1"
}

class OECD(Fetcher):
    
    def __init__(self, **kwargs):
        super().__init__(provider_name='OECD', **kwargs)
        
        if not self.provider:
            self.provider = Providers(name=self.provider_name, 
                                      long_name='Organisation for Economic Co-operation and Development',
                                      version=VERSION,
                                      region='world',
                                      website='http://www.oecd.org', 
                                      fetcher=self)

        if self.provider.version != VERSION:
            self.provider.update_database()

        self.cache_settings = self._get_cache_settings()

    def _get_cache_settings(self):

        tmp_filepath = os.path.abspath(os.path.join(tempfile.gettempdir(), 
                                                    self.provider_name))
        
        return {
            "cache_name": tmp_filepath, 
            "backend": 'sqlite', 
            "expire_after": CACHE_EXPIRE
        }

    def _load_datasets(self):
        
        for dataset in self.datasets_list():
            dataset_code = dataset["dataset_code"]
            try:
                self.upsert_dataset(dataset_code)
            except Exception as err:
                if isinstance(err, errors.MaxErrors):
                    raise
                logger.fatal("error for dataset[%s]: %s" % (dataset_code, str(err)))
        
    def load_datasets_first(self):
        start = time.time()
        logger.info("first load fetcher[%s] - START" % (self.provider_name))

        self._load_datasets()
        
        end = time.time() - start
        logger.info("first load fetcher[%s] - END - time[%.3f seconds]" % (self.provider_name, end))
        
    def load_datasets_update(self):
        start = time.time()
        logger.info("update fetcher[%s] - START" % (self.provider_name))

        self._load_datasets()
        
        end = time.time() - start
        logger.info("update fetcher[%s] - END - time[%.3f seconds]" % (self.provider_name, end))

    def build_data_tree(self, force_update=False):
        
        categories = []
        
        for category_code, dataset in DATASETS.items():
            cat = {
                "category_code": category_code,
                "name": dataset["name"],
                "doc_href": dataset["doc_href"],
                "datasets": [{
                    "name": dataset["name"], 
                    "dataset_code": category_code,
                    "last_update": None, 
                    "metadata": None
                }]
            }
            categories.append(cat)
        
        return categories

    def upsert_dataset(self, dataset_code, datas=None):
        
        start = time.time()
        
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        
        if not DATASETS.get(dataset_code):
            raise Exception("This dataset is unknown" + dataset_code)
        
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code, 
                           name=DATASETS[dataset_code]['name'], 
                           doc_href=DATASETS[dataset_code]['doc_href'],
                           last_update=clean_datetime(),
                           fetcher=self)
        
        fetcher_data = OECD_Data(dataset, 
                                 sdmx_filter=DATASETS[dataset_code]['sdmx_filter'],
                                 fetcher=self)
        dataset.series.data_iterator = fetcher_data
        result = dataset.update_database()

        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))
        
        return result


def json_dataflow(message_dict):
    
    _codes = {}
    
    _codes['header'] = message_dict.pop('header', None)
    
    _codes['dimension_keys'] = []
    _codes['attribute_dataset_keys'] = []
    _codes['attribute_observation_keys'] = []
    
    #Attention à TIME_PERIOD
    _codes['codelists'] = OrderedDict()
    _codes['concepts'] = {}

    for d in message_dict['structure']['dimensions']['observation']:
        dim_id = d['id']
        _codes['dimension_keys'].append(dim_id)
        _codes["concepts"][dim_id] = d['name'] 
        _codes['codelists'][dim_id] = OrderedDict([(x['id'], x['name']) for x in d['values']])
        
    ##UNIT, POWERCODE, REFERENCEPERIOD
    for d in message_dict['structure']['attributes']['dataSet']:
        attr_id = d['id']
        _codes['attribute_dataset_keys'].append(attr_id)
        _codes["concepts"][attr_id] = d['name'] 
        _codes['codelists'][attr_id] = OrderedDict([(x['id'], x['name']) for x in d['values']])

    #print("!!!attributes : ", list(message_dict['structure']['attributes'].keys()))
    #print("!!!attributes obs : ", message_dict['structure']['attributes']['observation'])
    ##TIME_FORMAT, OBS_STATUS
    for d in message_dict['structure']['attributes']['observation']:
        attr_id = d['id']
        _codes['attribute_observation_keys'].append(attr_id)
        _codes["concepts"][attr_id] = d['name'] 
        _codes['codelists'][attr_id] = OrderedDict([(x['id'], x['name']) for x in d['values']])
    
    return _codes

def json_data(message_dict):

    periods = message_dict['structure']['dimensions']['observation'][0] #TIME_PERIOD
    
    if periods["id"] != "TIME_PERIOD":
        raise Exception("First dimension observation is not TIME_PERIOD field")
    periods = [period['id'] for period in periods['values']]

    series_dimensions = message_dict['structure']['dimensions']['series']
    attributes = message_dict['structure']['attributes']
    series = message_dict['dataSets'][0]['series']

    series_list = []

    for key in series:
        
        series_key = {}
        series_attrs = {}

        dims = key.split(':')
        for dimension, position in zip(series_dimensions, dims):
            series_key[dimension['id']] = dimension['values'][int(position)]['id']

        attrs = message_dict['dataSets'][0]['series'][key]['attributes']        
        for attribute, position in zip(attributes['series'], attrs):
            if position is None: continue
            series_attrs[attribute['id']] = attribute['values'][int(position)]['id']
        
        observations = message_dict['dataSets'][0]['series'][key]['observations']
        
        _series = {
            "key_o": key,
            "dimensions": series_key,
            "attributes": series_attrs,
            "values": [],
        }

        for i, obs in enumerate(observations.values()):
            
            attr_obs = [position for position in obs[1:] if not position is None]
            attrs = {}
            if attr_obs:
                for attribute, position in zip(attributes['observation'], attr_obs):
                    if position is None: continue
                    attrs[attribute['id']] = attribute['values'][int(position)]['id']
                     
            _series["values"].append({
                "period": periods[i],                        
                "value": str(obs[0]),
                "attributes": attrs if attrs else None,
            })

        series_list.append(_series)
        
    return series_list

def load_dataflow(url, dataset_code):

    download = Downloader(url=url, filename="dataflow-%s.json" % dataset_code)
    filepath = download.get_filepath()
    
    with open(filepath) as fp:
        message_dict = json.load(fp, object_pairs_hook=OrderedDict)
    
    return json_dataflow(message_dict), filepath    


def load_data(url, dataset_code, cache_settings=None):
    
    download = Downloader(url=url, 
                          filename="data-%s.json" % dataset_code,
                          headers=SDMX_DATA_HEADERS,
                          cache=cache_settings)
    filepath, response = download.get_filepath_and_response()
    
    if response.status_code >= 400:
        return None, filepath, response.status_code, response
        #raise response.raise_for_status()

    """
    304 (No change) No change since the timestamp supplied in the If-Modified-Since header
    
    if response.status_code == HTTP_ERROR_NO_RESULT:
        continue
    elif response.status_code >= 400:
        raise response.raise_for_status()
    """
    with open(filepath) as fp:
        message_dict = json.load(fp, object_pairs_hook=OrderedDict)
    
    return json_data(message_dict), filepath, response.status_code, response
        
        

class OECD_Data(SeriesIterator):
    
    def __init__(self, dataset, sdmx_filter, fetcher=None):
        
        self.dataset = dataset
        self.sdmx_filter = sdmx_filter
        self.fetcher = fetcher
        
        self.provider_name = self.fetcher.provider_name
        self.dataset_code = dataset.dataset_code
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        self.release_date = self.dataset.last_update
                
        self.codes = OrderedDict()
        
        self.dimension_keys = []
        self.attribute_keys = []
        self.attribute_dataset_keys = []
        self.attribute_observation_keys = []
        
        self.codelists = {}
        self.concepts = {}
        
        self._load_dataflow()
        self.rows = self._load_data()
        
    def _get_url_dataflow(self):
        return "http://stats.oecd.org/sdmx-json/dataflow/%s" % self.dataset_code        

    def _get_url_data(self, flowkey):
        return "http://stats.oecd.org/sdmx-json/data/%s/%s" % (self.dataset_code, flowkey)        
        
    def _load_dataflow(self):

        url = self._get_url_dataflow()
        self.codes, filepath = load_dataflow(url, self.dataset_code)
        self.dataset.for_delete.append(filepath)
        
        self.dataset.concepts = self.codes["concepts"]
        self.dataset.codelists = self.codes["codelists"]

        self.codelists = self.dataset.codelists
        self.concepts = self.dataset.concepts

        self.dimension_keys = [k for k in self.codes["dimension_keys"] if k != "TIME_PERIOD"]
        self.dataset.dimension_keys = self.dimension_keys
        
        self.attribute_dataset_keys = self.codes["attribute_dataset_keys"]
        self.attribute_observation_keys = self.codes["attribute_observation_keys"]
        self.attribute_keys = self.attribute_dataset_keys + self.attribute_observation_keys
        self.dataset.attribute_keys = self.attribute_keys
        
        """
        TODO: ?
            for k, dimensions in self.dimension_list.get_dict().items():
                self.dataset.codelists[k] = dimensions

            for k, attributes in self.attribute_list.get_dict().items():
                self.dataset.codelists[k] = attributes
        
        """

    def _select_filter_dimension(self):

        position = self.dimension_keys.index(self.sdmx_filter)         
                
        dimension_values = list(self.codelists[self.sdmx_filter].keys())
        
        return (position, 
                len(self.dimension_keys), 
                self.sdmx_filter, 
                dimension_values)

    def _load_data(self):

        position, count_dimensions, _key, dimension_values = self._select_filter_dimension()
        
        for value in dimension_values:
            
            #if not value in ["ARG", "NMEC", "BRA"]:
            #    continue
                        
            sdmx_key = []
            for i in range(count_dimensions):
                if i == position:
                    sdmx_key.append(value)
                else:
                    sdmx_key.append(".")
            key = "".join(sdmx_key)

            url = self._get_url_data(key)
            
            rows, filepath, status_code, response = load_data(url, self.dataset_code, self.fetcher.cache_settings)
            self.dataset.for_delete.append(filepath)
            
            if status_code >= 400:
                msg = "http error for provider[%s] - dataset[%s] - url[%s] - code[%s] - reason[%s]"
                datas = (self.provider_name,self.dataset_code, url, 
                         status_code, response.reason)
                
                if status_code >= 400 and status_code < 500:
                    logger.warning(msg % datas)
                elif status_code >= 500:
                    logger.critical(msg % datas)
                
                continue

            if not rows:
                print("response.reason : ", response.reason, status_code)
                raise Exception("rows is None for url[%s]" % url)
            
            for row in rows:
                try:
                    yield row, None
                except Exception as err:
                    yield None, err
            
    def build_series(self, row):
        """Build one serie
        
        Return instance of :class:`dict`
        """
        
        series_key = ".".join([row["dimensions"][key] for key in self.dimension_keys])

        frequency = row['dimensions']['FREQUENCY']

        if not frequency in ['A', 'M', 'Q', 'W', 'D']:
            raise errors.RejectFrequency(provider_name=self.provider_name,
                                         dataset_code=self.dataset_code,
                                         frequency=frequency)
            
        #for key, item in bson['dimensions'].items():
        #    self.dimension_list.update_entry(key, item, item)
            
        for _row in row["values"]:
            _row["release_date"] = self.release_date
            _row["period_o"] = self.codelists["TIME_PERIOD"].get(_row["period"], _row["period"])
            _row["ordinal"] = self.fetcher.get_ordinal_from_period(_row["period"], 
                                                                   freq=frequency)

        _name = []
        for key in self.dimension_keys:
            value_key = row["dimensions"][key]
            value_name = self.codelists[key][value_key]
            _name.append(value_name)
        series_name = " - ".join(_name)
        
        bson = {'provider_name': self.provider_name,
                'dataset_code': self.dataset_code,
                'name': series_name,
                'key': series_key,
                'values': row["values"],
                'attributes': row['attributes'],
                'dimensions': row['dimensions'],
                'last_update': self.dataset.last_update,
                'start_date': row["values"][0]["ordinal"],
                'end_date': row["values"][-1]["ordinal"],
                'frequency': frequency}
        
        return bson
    
