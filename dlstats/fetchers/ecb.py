# -*- coding: utf-8 -*-

"""
217 series / secondes
"""

import time
from datetime import datetime
import logging

import requests
import pandas

from pandasdmx.api import Request

from dlstats.fetchers._commons import Fetcher, Datasets, Providers
from dlstats import constants
from collections import OrderedDict

HTTP_ERROR_LONG_RESPONSE = 413
HTTP_ERROR_NO_RESULT = 404
HTTP_ERROR_BAD_REQUEST = 400
HTTP_ERROR_SERVER_ERROR = 500

VERSION = 1

logger = logging.getLogger(__name__)

class ContinueRequest(Exception):
    pass

class ECB(Fetcher):
    
    def __init__(self, db=None, sdmx=None, **kwargs):        
        super().__init__(provider_name='ECB', db=db, **kwargs)
        
        self.provider = Providers(name=self.provider_name,
                                 long_name='European Central Bank',
                                 version=VERSION,
                                 region='Europe',
                                 website='http://www.ecb.europa.eu',
                                 fetcher=self)
        
        self.sdmx = sdmx or Request(agency=self.provider_name)
        self.sdmx.timeout = 90
        
        self._dataflows = None
        self._constraints = None
        self._categoryschemes = None
        self._categorisations = None
    
    def load_structure(self, force=False):
        
        if self._dataflows and not force:
            return
        
        """
        #http://sdw-wsrest.ecb.europa.eu/service/categoryscheme?references=parentsandsiblings
        categoryscheme_response = self.sdmx.get(resource_type='categoryscheme')
        logger.debug(categoryscheme_response.url)
        self._categoryschemes = categoryscheme_response.msg.categoryschemes
    
        #http://www.bdm.insee.fr/series/sdmx/categorisation
        categorisation_response = self.sdmx.get(resource_type='categorisation')
        logger.debug(categorisation_response.url)
        self._categorisations = categorisation_response.msg.categorisations
        """
    
        #http://sdw-wsrest.ecb.europa.eu/service/dataflow
        dataflows_response = self.sdmx.get(resource_type='dataflow', agency=self.provider_name)    
        logger.debug(dataflows_response.url)
        self._dataflows = dataflows_response.msg.dataflows

    def upsert_all_datasets(self):
        start = time.time()        
        logger.info("update fetcher[%s] - START" % (self.provider_name))
        
        self.load_structure(force=False)
        
        for dataset_code in self.datasets_list():
            try:
                self.upsert_dataset(dataset_code)
            except Exception as err:
                logger.fatal("error for dataset[%s]: %s" % (dataset_code, str(err)))

        end = time.time() - start
        logger.info("update fetcher[%s] - END - time[%.3f seconds]" % (self.provider_name, end))

    def datasets_list(self):
        #TODO: from Categories
        self.load_structure(force=False)
        return self._dataflows.keys()
        
    def datasets_long_list(self):
        #TODO: from Categories
        self.load_structure(force=False)
        return [(key, dataset.name.en) for key, dataset in self._dataflows.items()]

    def upsert_categories(self):
        return
    
    def upsert_dataset(self, dataset_code):

        self.load_structure(force=False)
        
        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        
        if not dataset_code in self._dataflows:
            raise Exception("This dataset is unknown" + dataset_code)
        
        dataflow = self._dataflows[dataset_code]
        
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code,
                           name=dataflow.name.en,
                           doc_href=None,
                           last_update=datetime.now(),
                           fetcher=self)
        
        #dataset_doc = self.db[constants.COL_DATASETS].find_one({"provider_name": self.provider_name,
        #                                                        "dataset_code": dataset_code})
        
        insee_data = ECB_Data(dataset=dataset,
                              #dataset_doc=dataset_doc, 
                              sdmx=self.sdmx)
        dataset.series.data_iterator = insee_data
        result = dataset.update_database()
        
        dataflow = None
        insee_data = None

        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))
        
        return result

class ECB_Data(object):
    
    def __init__(self, dataset=None, 
                 #dataset_doc=None, 
                 sdmx=None):
        """
        :param Datasets dataset: Datasets instance
        :param pandasdmx.model.DataflowDefinition dataflow: instance of DataflowDefinition
        :param RequestINSEE sdmx: SDMX Client  
        """        
        
        self.dataset = dataset
        #self.dataset_doc = dataset_doc
        self.provider_name = self.dataset.provider_name
        self.dataset_code = self.dataset.dataset_code
        self.sdmx = sdmx
        
        dataflows_response = self.sdmx.get(resource_type='dataflow', 
                                           agency=self.provider_name, 
                                           resource_id=self.dataset_code,
                                           memcache='dataflow' + self.dataset_code)    
        
        logger.debug(dataflows_response.url)
        
        self.dataflow = dataflows_response.msg.dataflows[self.dataset_code] 

        constraints = dataflows_response.msg.constraints
        
        constraints = [c for c in constraints.aslist() 
                            if c.constraint_attachment.id == self.dataset_code]
        
        self.constraint = None
        if len(constraints) == 1:
            self.constraint = constraints[0]
            
        self.cube_region = None
        if self.constraint:
            self.cube_region = self.constraint.cube_region.key_values
            
        self.dimension_list = self.dataset.dimension_list
        self.attribute_list = self.dataset.attribute_list
        
        self.dsd_id = self.dataflow.structure.id
        self.datastructure = self.sdmx.get(resource_type='datastructure', 
                                 resource_id=self.dsd_id,
                                 agency=self.dataset.provider_name,
                                 headers={"Accept-Encoding": "gzip, deflate"},
                                 params=dict(references='all'))
        
        #TODO: voir si necessaire avec dataflow + reference ???
        
        self.dsd = self.datastructure.msg.datastructures[self.dsd_id]
        
        self.dimensions = OrderedDict([(dim.id, dim) for dim in self.dsd.dimensions.aslist() if dim.id not in ['TIME', 'TIME_PERIOD']])
        self.dim_keys = list(self.dimensions.keys())
        self.dimension_list.set_dict(self.dimensions_to_dict())
        
        '''Selection de la dimension avec le plus de variantes'''
        self.dim_select_key = self.select_dimension_split()
        self.dim_select = self.dimensions[self.dim_select_key]
        self.dim_select_values = None
        
        if self.cube_region and self.dim_select_key in self.cube_region:
            self.dim_select_values = self.cube_region[self.dim_select_key]
        else:
            self.dim_select_values = self.dim_select.local_repr.enum.keys()

        print("self.dim_select_key : ", self.dim_select_key)
        print("self.dim_select_values : ", self.dim_select_values)
        
        self.rows = self.get_series(self.dataset_code)
        
    def get_dim_values_for_select(self):
        pass
        
    def dimensions_to_dict(self):
        _dimensions = {}
        
        for key in self.dim_keys:
            dim = self.dsd.dimensions[key]
            _dimensions[key] = OrderedDict()
            
            for dim_key, _dim in dim.local_repr.enum.items():
                try:
                    _dimensions[key][dim_key] = _dim.name.en
                except Exception:
                    _dimensions[key][dim_key] = _dim.name.fr
                
        return _dimensions
    
    def concept_name(self, concept_id):
        #TODO: pas normal, l'information devrait être dans la self.dsd
        return self.datastructure.conceptschemes.aslist()[0][concept_id].name.en
        
    def select_dimension_split(self):
        """Renvoi le nom de la dimension qui contiens le plus de valeur
        pour servir ensuite de filtre dans le chargement des données (...)
        
        """
        _dimensions = {}
        
        if self.cube_region:
            for dim_id, values in self.cube_region.items():
                _dimensions[len(values)] = dim_id
        else:
            for dim_id, dim in self.dimensions.items():
                _dimensions[len(dim.local_repr.enum.keys())] = dim_id
        
        select_value = max(_dimensions.keys())
        return _dimensions[select_value] 

    def is_valid_frequency(self, series):
        """
        Pb Frequency ECB:
        https://sdw-wsrest.ecb.europa.eu/service/dataflow/ECB/EXR?references=all
        https://sdw-wsrest.ecb.europa.eu/service/data/EXR/.ARS...
        A: Annual (2015)
        B: Business (Pas d'exemple)
        D: Daily (2000-01-13)
        E: Event (not supported)
        H: Half-yearly (2000-S2)
        M: Monthly (2000-02)
        N: Minutely (Pas d'exemple)
        Q: Quarterly (2000-Q2)
        S: Half Yearly, semester (value H exists but change to S in 2009, move from H to this new value to be agreed in ESCB context) (Pas d'exemple)
        W: Weekly (Pas d'exemple)
        """
        frequency = None
        valid = True
        
        if 'FREQ' in series.key._fields:
            frequency = series.key.FREQ
        elif 'FREQ' in series.attrib._fields:
            frequency = series.attrib.FREQ
        
        if not frequency:
            valid = False        
        elif not frequency in ["A", "D", "M", "Q", "W"]: #TODO: N (minute) ?
            valid = False
        
        if not valid:
            logger.warning("Not valid frequency[%s] - dataset[%s] - key[%s]" % (frequency, self.dataset_code, self.get_series_key(series)))
        
        return valid
    
    def is_updated(self, series):
        """Verify if series changes
        
        TODO: voir headers result
        
        'Last-Modified': 'Fri, 08 Jan 2016 04:35:48 GMT'
        
        REQUEST HEADERS  {'Accept': '*/*', 'Accept-Encoding': 'gzip, deflate', 'Connection': 'keep-alive', 'User-Agent': 'python-requests/2.9.1'}
        2016-01-08 05:35:42 dlstats.fetchers.ecb: [INFO] - http://sdw-wsrest.ecb.int/service/data/SST/..VPC....
        2016-01-08 05:35:42 dlstats.fetchers.ecb: [INFO] - {'Cache-Control': 'max-age=0, no-cache, no-store', 'Connection': 'keep-alive, Transfer-Encoding', 'Content-Encoding': 'gzip', 'Expires': 'Fri, 08 Jan 2016 04:35:49 GMT', 'Server': 'Apache-Coyote/1.1', 'Pragma': 'no-cache', 'Vary': 'Accept, Accept-Encoding', 'Date': 'Fri, 08 Jan 2016 04:35:49 GMT', 'Transfer-Encoding': 'chunked', 'Content-Type': 'application/xml', 'Last-Modified': 'Fri, 08 Jan 2016 04:35:48 GMT'}        
        
        > dans la requete: 2009-05-15T14:15:00+01:00
        updatedAfter=2009-05-15T14%3A15%3A00%2B01%3A00
        
        updatedAfter=2009-05-15T14:15:00+01:00
        
        >>> urllib.parse.quote("2009-05-15T14:15:00+01:00")
        '2009-05-15T14%3A15%3A00%2B01%3A00'
        
        If-Modified-Since header
        
        >>> str(datetime.now())
        '2015-12-26 08:21:15.987529'
        """
        raise NotImplementedError()

    def get_series(self, dataset_code):
        """Load series for current dataset
        """

        def _request(key):
            return self.sdmx.get(resource_type='data', 
                                 resource_id=dataset_code,
                                 agency=self.dataset.provider_name,
                                 key=key,
                                 #headers={"Accept-Encoding": "gzip, deflate"}, 
                                 )
            
        try:
            codes = self.dim_select_values
            
            for code in codes:
                key = {self.dim_select_key: code}
                try:
                    _data = _request(key)
                    logger.info(_data.url)
                    logger.info(_data.http_headers)
                    for s in _data.msg.data.series:
                        if self.is_valid_frequency(s):
                            yield s
                        else:
                            yield
                except requests.exceptions.HTTPError as err:
                    if err.response.status_code == HTTP_ERROR_NO_RESULT:
                        continue
                    else:
                        raise
                                            
        except Exception as err:
            logger.error(str(err))
            raise
        
    def __next__(self):          
        try:      
            _series = next(self.rows)
            if not _series:
                raise StopIteration()
        except ContinueRequest:
            _series = next(self.rows)
            
        bson = self.build_series(_series)
        return bson

    def get_series_key(self, series):
        return ".".join(series.key._asdict().values())

    def get_series_name(self, series):
        if 'TITLE_COMPL' in series.attrib._fields:
            return series.attrib.TITLE_COMPL
        elif 'TITLE' in series.attrib._fields:
            return series.attrib.TITLE
        else:
            #FIXME: Non ?
            return "-".join(series.key._asdict().values())
    
    def get_series_frequency(self, series):
        if 'FREQ' in series.key._fields:
            return series.key.FREQ
        elif 'FREQ' in series.attrib._fields:
            return series.attrib.FREQ
        else:
            raise Exception("Not FREQ field in series.key or series.attrib")
    
    def get_last_update(self, series):
        return self.dataset.last_update
    
    def debug_series(self, series, bson):
        if logger.isEnabledFor(logging.DEBUG):
            import json
            try:
                _debug = {
                    "dataset_code": self.dataset_code,
                    "key": bson['key'],
                    "last_update": str(self.dataset.last_update),
                    "dimensions_keys": self.dim_keys,
                    "series.attrib": list(series.attrib._asdict().items()),
                    "series.key": list(series.key._asdict().items()),
                }            
                logger.debug(json.dumps(_debug))
            except Exception as err:
                print("JSON EXCEPTION : ", err)
    
    def get_attributes(self, series):
        attributes = {}
        
        #TODO: self.attribute_list         
        for obs in series.obs(with_values=False, with_attributes=True, reverse_obs=False):
            for key, value in obs.attrib._asdict().items():
                if not key in attributes:
                    attributes[key] = []
                attributes[key].append(value)
        return attributes
    
    def build_series(self, series):
        """
        :param series: Instance of pandasdmx.model.Series
        """
        bson = {}
        bson['provider_name'] = self.provider_name
        bson['dataset_code'] = self.dataset_code
        bson['key'] = self.get_series_key(series)
        bson['name'] = self.get_series_name(series)
        bson['last_update'] = self.get_last_update(series)
        
        self.debug_series(series, bson)
        
        bson['frequency'] = self.get_series_frequency(series)
        bson['attributes'] = self.get_attributes(series)
            
        dimensions = dict(series.key._asdict())
        
        for d, value in series.attrib._asdict().items():
            if d in ['TITLE', 'TITLE_COMPL']:
                continue
            dim_short_id = value
            dim_long_id = self.concept_name(d)
            dimensions[d] = self.dimension_list.update_entry(d, dim_short_id, dim_long_id)

        bson['dimensions'] = dimensions
        
        _dates = [o.dim for o in series.obs(with_values=False, with_attributes=False, reverse_obs=False)]
        bson['start_date'] = pandas.Period(_dates[0], freq=bson['frequency']).ordinal
        bson['end_date'] = pandas.Period(_dates[-1], freq=bson['frequency']).ordinal
        
        bson['values'] = [str(o.value) for o in series.obs(with_values=True, with_attributes=False, reverse_obs=False)]

        return bson

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, filename="ecb.log", 
                        format='%(asctime)s %(name)s: [%(levelname)s] - %(message)s')

    cache_options = dict(backend='sqlite', 
                         expire_after=None,
                         location="C:/temp/cepremap/fetchers-sources/ECB/cache")
    
    sdmx = Request(agency='ECB', cache=cache_options)
    
    ecb = ECB(sdmx=sdmx)
    
    ecb.db[constants.COL_CATEGORIES].remove({"provider": "ECB"})
    ecb.db[constants.COL_DATASETS].remove({"provider": "ECB"})
    ecb.db[constants.COL_SERIES].remove({"provider": "ECB"})
    
    #ecb.upsert_all_datasets()
    
    #ecb.upsert_categories()
    ecb.upsert_dataset('EXR')
    
    
    
