# -*- coding: utf-8 -*-

import time
import urllib
from datetime import datetime
import logging

import requests
import pandas

from pandasdmx.api import Request

from dlstats.fetchers._commons import Fetcher, Categories, Datasets, Providers
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
        self.sdmx.set_timeout(90)
        
        self._dataflows = None
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
    
        #http://sdw-wsrest.ecb.europa.eu/service/dataflow?references=all
        #TODO: timeout !!!
        dataflows_response = self.sdmx.get(resource_type='dataflow', agency=self.provider_name, params=dict(references=None))    
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
        
        dataset_doc = self.db[constants.COL_DATASETS].find_one({"provider_name": self.provider_name,
                                                                "dataset_code": dataset_code})
        
        insee_data = ECB_Data(dataset=dataset,
                              dataset_doc=dataset_doc, 
                              dataflow=dataflow, 
                              sdmx=self.sdmx)
        dataset.series.data_iterator = insee_data
        result = dataset.update_database()
        
        dataflow = None
        insee_data = None

        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))
        
        return result

class ECB_Data(object):
    
    def __init__(self, dataset=None, dataset_doc=None, dataflow=None, sdmx=None):
        """
        :param Datasets dataset: Datasets instance
        :param pandasdmx.model.DataflowDefinition dataflow: instance of DataflowDefinition
        :param RequestINSEE sdmx: SDMX Client  
        """        
        self.cpt = 0
        
        self.dataset = dataset
        self.dataset_doc = dataset_doc
        """
        self.last_update = None
        if self.dataset_doc:
            self.last_update = self.dataset_doc["last_update"]
        """
        self.provider_name = self.dataset.provider_name
        self.dataset_code = self.dataset.dataset_code
        self.dataflow = dataflow
        self.sdmx = sdmx
        
        self.dimension_list = self.dataset.dimension_list
        self.attribute_list = self.dataset.attribute_list
        
        self.dsd_id = dataflow.structure.id
        self.dsd = self.sdmx.get(resource_type='datastructure', 
                                 resource_id=self.dsd_id,
                                 agency=self.dataset.provider_name,
                                 headers={"Accept-Encoding": "gzip, deflate"},
                                 params=dict(references='all')).msg.datastructures[self.dsd_id]

        self.dimensions = OrderedDict([(dim.id, dim) for dim in self.dsd.dimensions.aslist() if dim.id not in ['TIME', 'TIME_PERIOD']])
        self.dim_keys = list(self.dimensions.keys())
        self.dimension_list.set_dict(self.dimensions_to_dict())
        
        '''Selection de la dimension avec le moins de variantes'''
        self.dim_select = self.select_short_dimension()
        
        self.current_series = {}
        
        self.rows = self.get_series(self.dataset_code)
        
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
        return self.datastructure.msg.conceptschemes.aslist()[0][concept_id].name.en
        
    def select_short_dimension(self):
        """Renvoi le nom de la dimension qui contiens le moins de valeur
        pour servir ensuite de filtre dans le chargement des données (...)
        """
        _dimensions = {}        
        
        for dim_id, dim in self.dimensions.items():
            _dimensions[dim_id] = len(dim.local_repr.enum.keys())
        
        _key = min(_dimensions, key=_dimensions.get)
        return self.dimensions[_key]

    def is_valid_frequency(self, series):
        """
        http://www.bdm.insee.fr/series/sdmx/codelist/FR1/CL_FREQ
        S: Semestrielle
        B: Bimestrielle
        I: Irregular
        """
        frequency = None
        valid = True
        
        if 'FREQ' in series.key._fields:
            frequency = series.key.FREQ
        elif 'FREQ' in series.attrib._fields:
            frequency = series.attrib.FREQ
        
        if not frequency:
            valid = False        
        elif frequency in ["S", "B", "I"]:
            valid = False
        
        if not valid:
            logger.warning("Not valid frequency[%s] - dataset[%s] - idbank[%s]" % (frequency, self.dataset_code, series.attrib.IDBANK))
        
        return valid
    
    def is_updated(self, series):
        """Verify if series changes
        
        > dans la requete: 2009-05-15T14:15:00+01:00
        updatedAfter=2009-05-15T14%3A15%3A00%2B01%3A00
        
        updatedAfter=2009-05-15T14:15:00+01:00
        
        >>> urllib.parse.quote("2009-05-15T14:15:00+01:00")
        '2009-05-15T14%3A15%3A00%2B01%3A00'
        
        If-Modified-Since header
        
        >>> str(datetime.now())
        '2015-12-26 08:21:15.987529'
        
        
        """
        if not self.last_update:
            return True
        
        _is_updated = datetime.strptime(series.attrib.LAST_UPDATE, "%Y-%m-%d") > self.last_update

        if not _is_updated and logger.isEnabledFor(logging.INFO):
            logger.info("bypass series updated dataset_code[%s] - idbank[%s]" % (self.dataset_code, series.attrib.IDBANK))
        
        return _is_updated

    def get_series(self, dataset_code):
        """Load series for current dataset

        #TODO: traduire:
        
        1er appel: sans filtrage sur les dimensions
        
        Appel suivant seulement si le 1er appel échoue avec un code HTTP_ERROR_LONG_RESPONSE
        
        Boucle sur les valeurs de le plus petite dimensions pour charger en plusieurs fois les series du dataset
        
        """

        def _request(key=''):
            return self.sdmx.get(resource_type='data', 
                                 resource_id=dataset_code,
                                 agency=self.dataset.provider_name,
                                 key=key,
                                 headers={"Accept-Encoding": "gzip, deflate"}, 
                                 )
            
        try:
            ''' First call - all series '''
            _data = _request()
            for s in _data.msg.data.series:
                if self.is_valid_frequency(s):#TODO: and self.is_updated(s):
                    yield s                        
        except requests.exceptions.HTTPError as err:

            if err.response.status_code == HTTP_ERROR_LONG_RESPONSE:
                ''' Next calls if first call fail '''                
                codes = list(self.dim_select.local_repr.enum.keys())
                dim_select_id = str(self.dim_select.id)
                
                for code in codes:
                    key = {dim_select_id: code}
                    try:
                        _data = _request(key)
                        for s in _data.msg.data.series:
                            if self.is_valid_frequency(s):#TODO: and self.is_updated(s):
                                yield s
                    except requests.exceptions.HTTPError as err:
                        if err.response.status_code == HTTP_ERROR_NO_RESULT:
                            continue
                        else:
                            raise

            elif err.response.status_code == HTTP_ERROR_NO_RESULT:
                ''' Not error - this series are not datas '''
                raise ContinueRequest("No result")
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

    def get_series_name(self, series):
        if 'TITLE_COMPL' in series.attrib._fields:
            return series.attrib.TITLE_COMPL
        elif 'TITLE' in series.attrib._fields:
            return series.attrib.TITLE
        else:
            return "-".join(series.key._asdict().values())
        
    def build_series(self, series):
        """
        :param series: Instance of pandasdmx.model.Series
        
        Serie Name     M-U2-N-V-L30-A-1-U2-2300-Z01-E
        Serie Key     M.U2.N.V.L30.A.1.U2.2300.Z01.E        
        """
        bson = {}
        bson['provider_name'] = self.provider_name
        bson['dataset_code'] = self.dataset_code
        bson['key'] = ".".join(series.key._asdict().values())
        bson['name'] = self.get_series_name(series)
        bson['last_update'] = self.dataset.last_update
                
        if logger.isEnabledFor(logging.DEBUG):
            import json
            try:
                _debug = {
                    "dataset_code": self.dataset_code,
                    "key": bson['key'],
                    "last_update": str(self.dataset.last_update),
                    "dimensions_keys": self.dim_keys,
                    "series.attrib": series.attrib._asdict().items(),
                    "series.key": series.key._asdict().items(),
                }            
                logger.debug(json.dumps(_debug))
            except:
                pass

        if 'FREQ' in series.key._fields:
            bson['frequency'] = series.key.FREQ
        elif 'FREQ' in series.attrib._fields:
            bson['frequency'] = series.attrib.FREQ
        else:
            raise Exception("Not FREQ field in series.key or series.attrib")
        
        attributes = {}
        for obs in series.obs(with_values=False, with_attributes=True, reverse_obs=False):
            for key, value in obs.attrib._asdict().items():
                if not key in attributes:
                    attributes[key] = []
                attributes[key].append(value)
        
        bson['attributes'] = attributes
            
        dimensions = dict(series.key._asdict())
        
        for d, value in series.attrib._asdict().items():
            if d in ['TITLE', 'TITLE_COMPL']:
                continue
            dim_short_id = value
            dim_long_id = self.concept_name(d)            
            dimensions[d] = self.dimension_list.update_entry(d, dim_short_id, dim_long_id)

        bson['dimensions'] = dimensions
        
        _dates = [o.dim for o in series.obs(with_values=False, with_attributes=False, reverse_obs=False)]
        bson['start_date'] = pandas.Period(_dates[-1], freq=bson['frequency']).ordinal
        bson['end_date'] = pandas.Period(_dates[0], freq=bson['frequency']).ordinal
        
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
    
    
    
