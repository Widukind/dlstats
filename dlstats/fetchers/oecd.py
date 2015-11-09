# -*- coding: utf-8 -*-

import logging
import time
from datetime import datetime
import io
import tempfile
import os

#TODO: simplejson ou json
import json

import pandas
import sdmx

from dlstats.fetchers._commons import Fetcher, Categories, Providers, Datasets

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

class OECD(Fetcher):
    
    def __init__(self, db=None, es_client=None, **kwargs):
        super().__init__(provider_name='OECD', db=db, es_client=es_client, **kwargs)
        self.provider_name = 'OECD'
        self.provider = Providers(name=self.provider_name, 
                                  long_name='Organisation for Economic Co-operation and Development',
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
        
    def upsert_all_dataset(self):
        
        for dataset_code in DATASETS.keys():
            self.upsert_dataset(dataset_code) 
        
    def upsert_categories(self):
        
        for dataset_code in DATASETS.keys():
            document = Categories(provider=self.provider_name, 
                                  name=DATASETS[dataset_code]['name'], 
                                  categoryCode=dataset_code,
                                  exposed=True,
                                  fetcher=self)
            
            document.update_database()                            

class OECD_Data():
    
    def __init__(self, dataset, limited_countries=None, is_autoload=True):
        
        self.dataset = dataset
        
        #TODO: limited countries
        self.limited_countries = limited_countries# or ['AUS']
        
        self.prepared = None
        
        self.codes = {}
        
        self.countries = {}

        self.dimension_keys = []

        self.attribute_keys = []
        
        self.fp = None
                
        self.sdmx_client = sdmx.Repository('http://stats.oecd.org/sdmx-json','json', 
                                           '2_1', 
                                           'OECD', 
                                           timeout=60 * 5)
        
        self.codes_loaded = False
        self.datas_loaded = False
        
        if is_autoload:
            self.load_data_from_sdmx()
    
    def load_codes(self):
        
        if self.codes_loaded:
            return
        
        codes = self.sdmx_client.codes(self.dataset.dataset_code)
        
        header = codes.pop('header')
        
        #'2015-10-27T21:30:00.27625Z'
        self.prepared = datetime.strptime(header['prepared'], "%Y-%m-%dT%H:%M:%S.%fZ")
        self.dataset.last_update = self.prepared

        #TODO: LOCATION
        for k, v in codes['Country']:
            self.countries[k] = v
        
        #TODO: TIME_PERIOD
        self.dimension_keys = [k for k in codes.keys() if not k == 'Time']
        
        #TODO: self.attribute_keys = [k for k in codes['attributes'].keys() if not k == 'TIME_FORMAT']
        
        for key in self.dimension_keys:
            if not key in self.codes:                
                self.codes[key] = {}

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
            
            #URL :  http://stats.oecd.org/sdmx-json/data/MEI/FRA...all?startperiod=2014&endPeriod=2015&dimensionAtObservation=TIME
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
                row = {"id": id}
                row.update(raw_codes[id])
                row['values'] = raw_values[id]
                row['periods'] = raw_dates[id]
                row['attributes'] = raw_attributes[id]
                row_str = "%s\n" % json.dumps(row)
                #print("WRITE : ", row_str)
                self.fp.write(row_str)
                #json.dump(row, self.fp)
                
        self.fp.close()
        filepath, self.fp = self.get_temp_file(filepath=filepath, mode='r')    
        
        self.datas_loaded = True
    
    def __next__(self):
        row_str = next(self.fp)
        #print("READ : ", row_str)
        if row_str is None:
            #TODO: delete tmp file ?
            if self.fp and not self.fp.closed:
                self.fp.close()

            raise StopIteration()

        #TODO: exception ?
        #TODO: utf-8 ?        
        row = json.loads(row_str) 
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

        series_key = row['id']
        values = [str(v) for v in row['values']]
        frequency = row['Frequency']
        periods = row['periods']

        #TOSO: All none
        #row['attributes']
        #raw_attributes = self.raw_attributes[row_key]
        
        #print("ROW/periods: ", str(row), periods)
        
        period_start = self._patch_period(periods[0], frequency)
        period_end = self._patch_period(periods[-1], frequency)
        start_date = pandas.Period(period_start, freq=frequency)
        end_date = pandas.Period(period_end, freq=frequency)
                
        logger.debug("provider[%s] - dataset[%s] - serie[%s]" % (self.dataset.provider_name,
                                                                 self.dataset.dataset_code,
                                                                 series_key))

        dimensions = {}
        #TODO: attributes = {}
        
        for d in self.dimension_keys:
            dim_short_id = row[d]
            dim_long_id = self.codes[d][dim_short_id]
            dimensions[d] = self.dataset.dimension_list.update_entry(d, dim_short_id, dim_long_id)
        
        series_name = "-".join([row[d] for d in self.dimension_keys])

        data = {'provider': self.dataset.provider_name,
                'datasetCode': self.dataset.dataset_code,
                'name': series_name,
                'key': series_key,
                'values': values,
                'attributes': {},
                'dimensions': dimensions,
                'lastUpdate': self.prepared,
                'startDate': start_date.ordinal,
                'endDate': end_date.ordinal,
                'frequency': frequency}
        return(data)
    
def main():
    import sys
    print("WARNING : run main for testing only", file=sys.stderr)
    try:
        import requests_cache
        cache_filepath = os.path.abspath(os.path.join(tempfile.gettempdir(), 'dlstats_cache'))        
        requests_cache.install_cache(cache_filepath, backend='sqlite', expire_after=None)#=60 * 60) #1H
        print("requests cache in %s" % cache_filepath)
    except ImportError:
        pass
    
    from dlstats.mongo_client import mongo_client
    from dlstats.constants import ES_INDEX
    global ES_INDEX
    #ES_INDEX = "widukind_test"
    
    from elasticsearch import Elasticsearch, RequestsHttpConnection
    es_client = Elasticsearch([{'host': 'localhost', 'port': 9200}],
                              connection_class=RequestsHttpConnection, 
                              timeout=10, 
                              max_retries=5, 
                              use_ssl=False,
                              verify_certs=False,
                              sniff_on_start=True)
    
    print("use DB[%s] ELASTIC_INDEX[%s]" % ("widukind_test", ES_INDEX))

    db = mongo_client.widukind_test
    fetcher = OECD(db=db, es_client=es_client)    
    try:
        print("DELETE elasticsearch index")
        fetcher.es_client.indices.delete(index=ES_INDEX, ignore=[400, 404])
        #fetcher.es_client.indices.delete_template(name='*', ignore=404)        
    except Exception as err:
        print("DELETE index error : ", str(err))
    try:
        print("CREATE elasticsearch index")
        fetcher.es_client.indices.create(ES_INDEX, ignore=400)
    except Exception as err:
        print("CREATE index error : ", str(err))
    
    print("update provider...")        
    fetcher.provider.update_database()
    print("update categories...")        
    fetcher.upsert_categories()
    
    #print("update all datasets...")            
    #fetcher.upsert_all_dataset()
    
    print("update metas")        
    start = time.time()
    fetcher.update_metas('MEI')
    end = time.time() - start
    print("ES : %.3f" % end)
    
if __name__ == "__main__":
    main()
    
    
