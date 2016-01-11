# -*- coding: utf-8 -*-

from collections import OrderedDict
import logging
import time
from datetime import datetime
import tempfile
import os
from io import StringIO

#TODO: simplejson ou json
import json

import pandas
import requests

from dlstats.fetchers._commons import Fetcher, Providers, Datasets

VERSION = 1

logger = logging.getLogger(__name__)

DATASETS = {
    'MEI': { 
        'name': 'Main Economics Indicators',
        'doc_href': 'http://www.oecd-ilibrary.org/economics/data/main-economic-indicators_mei-data-en',
    },
    #'EO': { 
    #    'name': 'Economic Outlook',
    #    'doc_href': 'http://www.oecd.org/eco/outlook/',
    #},
}

class SDMXJson(object):
    
    def __init__(self, sdmx_url=None, agencyID=None, 
                 timeout=20, requests_client=None):
        
        self.sdmx_url = sdmx_url
        self.agencyID = agencyID
        self.timeout = timeout
        self.requests_client = requests_client
        
        self._codes = {}

    def query_rest(self,url):
        """Retrieve SDMX-json messages.

        :param url: The URL of the message.
        :type url: str
        :return: A dictionnary of the SDMX message
        """
        # Fetch data from the provider    
        logger.info('Requesting %s', url)
        client = self.requests_client or requests
        request = client.get(url, timeout=self.timeout)
        return json.load(StringIO(request.text), object_pairs_hook=OrderedDict)
        
    def metadata(self, flowRef):
        
        resource = 'metadata'
        url = '/'.join([self.sdmx_url, resource, flowRef])
        message_dict = self.query_rest(url)
        #message_dict['structure']['dimensions']['observation']
        self._codes['header'] = message_dict.pop('header', None)
        for code in message_dict['structure']['dimensions']['observation']:
            self._codes[code['name']] = [(x['id'],x['name']) for x in code['values']]
        return self._codes

    def raw_data(self, flowRef, key=None, startperiod=None, endperiod=None):

        code_lists = []         
        raw_dates = {}
        raw_values = {}
        raw_attributes = {}
        raw_codes = {}

        if key is None:
            key = 'all'
        resource = 'data'
        if startperiod and endperiod:
            query = '/'.join([resource, flowRef, key
                    + 'all?startperiod=' + startperiod
                    + '&endPeriod=' + endperiod
                    + '&dimensionAtObservation=TIME'])
        else:
            query = '/'.join([resource, flowRef, key,'all'])
        url = '/'.join([self.sdmx_url,query])
        message_dict = self.query_rest(url)
        
        dates = message_dict['structure']['dimensions']['observation'][0]
        dates = [node['name'] for node in dates['values']]
        series = message_dict['dataSets'][0]['series']
        dimensions = message_dict['structure']['dimensions']
        
        for dimension in dimensions['series']:
            dimension['keyPosition']
            dimension['id']
            dimension['name']
            dimension['values']

        for key in series:
            dims = key.split(':')
            code = ''
            for dimension, position in zip(dimensions['series'],dims):
                code = code + '.' + dimension['values'][int(position)]['id']
            code_lists.append((key, code))

        for key,code in code_lists:
            observations = message_dict['dataSets'][0]['series'][key]['observations']
            series_dates = [int(point) for point in list(observations.keys())]
            raw_dates[code] = [dates[position] for position in series_dates]
            raw_values[code] = [observations[key][0] for key in list(observations.keys())]
            raw_attributes[code] = [observations[key][1] for key in list(observations.keys())]
            raw_codes[code] = {}
            for code_,dim in zip(key.split(':'),message_dict['structure']['dimensions']['series']):
                raw_codes[code][dim['name']] = dim['values'][int(code_)]['id']

        return (raw_values, raw_dates, raw_attributes, raw_codes)
    

class OECD(Fetcher):
    
    def __init__(self, db=None, **kwargs):
        super().__init__(provider_name='OECD', db=db, **kwargs)
        self.provider_name = 'OECD'
        self.provider = Providers(name=self.provider_name, 
                                  long_name='Organisation for Economic Co-operation and Development',
                                  version=VERSION,
                                  region='world',
                                  website='http://www.oecd.org', 
                                  fetcher=self)

    def upsert_dataset(self, dataset_code, datas=None):
        
        start = time.time()
        
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        
        if not DATASETS.get(dataset_code):
            raise Exception("This dataset is unknown" + dataset_code)
        
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code, 
                           name=DATASETS[dataset_code]['name'], 
                           doc_href=DATASETS[dataset_code]['doc_href'],
                           fetcher=self)
        
        fetcher_data = OECD_Data(dataset)
        dataset.series.data_iterator = fetcher_data
        dataset.update_database()

        end = time.time() - start
        logger.info("upsert dataset[%s] - END-BEFORE-METAS - time[%.3f seconds]" % (dataset_code, end))

        self.update_metas(dataset_code)
        
        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))

    def datasets_list(self):
        return DATASETS.keys()

    def datasets_long_list(self):
        return [(key, dataset['name']) for key, dataset in DATASETS.items()]

    def upsert_all_datasets(self):
        start = time.time()
        logger.info("update fetcher[%s] - START" % (self.provider_name))
        
        for dataset_code in DATASETS.keys():
            self.upsert_dataset(dataset_code) 
        end = time.time() - start
        logger.info("update fetcher[%s] - END - time[%.3f seconds]" % (self.provider_name, end))
        
    def upsert_categories(self):
        
        data_tree = {'name': 'OECD',
                     'category_code': 'oecd_root',
                     'children': []}
        
        for dataset_code in DATASETS.keys():
            data_tree['children'].append({'name': DATASETS[dataset_code]['name'], 
                                          'category_code': dataset_code,
                                          'exposed': True,
                                          'children': None})

        self.provider.add_data_tree(data_tree)    

class OECD_Data():
    
    '''
    init -> load_data_from_sdmx() -> load_codes() -> __next__() 
    '''
    
    def __init__(self, dataset, limited_countries=None, is_autoload=True):
        
        self.dataset = dataset
        
        #TODO: limited countries
        self.limited_countries = limited_countries# or ['FRA']
        
        self.prepared = None
        
        self.codes = OrderedDict()
        
        self.countries = {}

        self.dimension_keys = []

        self.attribute_keys = []
        
        self.fp = None
                
        self.sdmx_client = SDMXJson(sdmx_url='http://stats.oecd.org/sdmx-json', 
                                    agencyID='OECD', 
                                    timeout=60 * 5, 
                                    requests_client=None)
                                    
        
        self.codes_loaded = False
        self.datas_loaded = False
        
        if is_autoload:
            self.load_data_from_sdmx()
    
    def load_codes(self):
        
        if self.codes_loaded:
            return
        
        codes = self.sdmx_client.metadata(self.dataset.dataset_code)
        
        header = codes.pop('header')
        
        #'2015-10-27T21:30:00.27625Z'
        self.prepared = datetime.strptime(header['prepared'], "%Y-%m-%dT%H:%M:%S.%fZ")
        
        #TODO: vérifier si mise à jour sinon bypass ?
        #Attention à timezone
        self.dataset.last_update = self.prepared

        #TODO: LOCATION
        for k, v in codes['Country']:
            self.countries[k] = v
        
        #TODO: TIME_PERIOD
        self.dimension_keys = [k for k in sorted(codes.keys()) if not k == 'Time']
        #TODO: self.attribute_keys = [k for k in codes['attributes'].keys() if not k == 'TIME_FORMAT']
        
        for key in self.dimension_keys:
            if not key in self.codes:                
                self.codes[key] = OrderedDict()

            for k, v in codes[key]:
                self.codes[key][k] = v
                
        self.codes_loaded = True
                
    def get_temp_file(self, filepath=None, mode='r'):
        if not filepath:
            tmpdir = tempfile.mkdtemp()                        
            filepath = os.path.abspath(os.path.join(tmpdir, "%s.json.txt" % self.dataset.dataset_code))
        
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("use temp filepath[%s]" % filepath)
            
        return filepath, open(filepath, mode=mode)
    
    def load_data_from_sdmx(self):
        
        if not self.codes_loaded:
            self.load_codes()
            
        if self.datas_loaded:
            return
        
        #TODO: utf ?
        filepath, self.fp = self.get_temp_file(mode='w')

        errors = 0
        #TODO: dataset settings ?
        max_errors = 3
        
        for country_code in self.countries.keys():
            
            if self.limited_countries and not country_code in self.limited_countries:
                continue
        
            logger.info("load for dataset[%s] - country[%s]" % (self.dataset.dataset_code, country_code))
            
            try:
                if errors >= max_errors:
                    raise Exception("Too many errors max[%s]" % max_errors)
                raw_values, raw_dates, raw_attributes, raw_codes = self.sdmx_client.raw_data(self.dataset.dataset_code,        
                                                                                  "%s..." % country_code)#,#startperiod="2014",endperiod="2015")
            except Exception as err:
                errors += 1
                logger.error("DATASET[%s] - COUNTRY[%s] - ERROR[%s]" % (self.dataset.dataset_code,
                                                                        country_code, 
                                                                        str(err)))
            
            for id in raw_codes.keys():
                #FIXME: pb fabrication ID sur sdmx
                row = {"id": id}
                row['dimensions'] = raw_codes[id]
                row['values'] = raw_values[id]
                row['periods'] = raw_dates[id]
                row['attributes'] = raw_attributes[id]
                row_str = "%s\n" % json.dumps(row)
                self.fp.write(row_str)
                
        self.fp.close()
        filepath, self.fp = self.get_temp_file(filepath=filepath, mode='r')    
        
        self.datas_loaded = True
    
    def __next__(self):
        row_str = next(self.fp)
        if row_str is None:
            #TODO: delete tmp file ?
            if self.fp and not self.fp.closed:
                self.fp.close()

            raise StopIteration()

        #TODO: exception ?
        #TODO: utf-8 ?        
        row = json.loads(row_str, object_pairs_hook=OrderedDict) 
        series = self.build_serie(row)
        return(series)

    def _patch_period(self, period, frequency):
        """Patch for bad implementation of period with OECD"""
        if frequency == "A":
            return period

        if frequency in ["M", "Q"]:
            freq = period.split("-")[0]
            value = period.split("-")[1]
            return "%s-%s" % (value, freq)
        else:
            raise Exception("Not implemented Frequency[%s]" % frequency)

    def build_serie(self, row):
        """Build one serie
        
        Return instance of :class:`dict`
        """

        values = [str(v) for v in row['values']]
        periods = row['periods']
        dimensions = row['dimensions']
        frequency = row['dimensions']['Frequency']

        #TODO: All none
        #row['attributes']
        #raw_attributes = self.raw_attributes[row_key]
        
        period_start = self._patch_period(periods[0], frequency)
        period_end = self._patch_period(periods[-1], frequency)
        start_date = pandas.Period(period_start, freq=frequency)
        end_date = pandas.Period(period_end, freq=frequency)
        
        new_dimensions = OrderedDict()
        #TODO: attributes = {}
        
        for d in sorted(dimensions.keys()):
            dim_short_id = dimensions[d]
            dim_long_id = self.codes[d][dim_short_id]
            new_dimensions[d] = self.dataset.dimension_list.update_entry(d, dim_short_id, dim_long_id)
        
        series_key = ".".join(new_dimensions.values())
        series_name = " - ".join([self.codes[d][v] for d, v in new_dimensions.items()])
        
        #dimensions = OrderedDict([(d, self.codes[d][v]) for d, v in new_dimensions.items()])
        dimensions = new_dimensions
        
        logger.debug("provider[%s] - dataset[%s] - serie[%s]" % (self.dataset.provider_name,
                                                                 self.dataset.dataset_code,
                                                                 series_key))

        data = {'provider_name': self.dataset.provider_name,
                'dataset_code': self.dataset.dataset_code,
                'name': series_name,
                'key': series_key,
                'values': values,
                'attributes': {},
                'dimensions': dimensions,
                'last_update': self.prepared,
                'start_date': start_date.ordinal,
                'end_date': end_date.ordinal,
                'frequency': frequency}
        return(data)
    
