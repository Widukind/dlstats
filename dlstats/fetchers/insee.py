# -*- coding: utf-8 -*-

"""
TODO: Voir code agence: FR1 au lieu de INSEE dans les requests
"""

import time
import urllib
from datetime import datetime
import logging

import requests
import pandas
from lxml import etree

from pandasdmx.api import Request

from dlstats.fetchers._commons import Fetcher, Datasets, Providers
from dlstats import constants
from collections import OrderedDict

HTTP_ERROR_LONG_RESPONSE = 413
HTTP_ERROR_NO_RESULT = 404
HTTP_ERROR_BAD_REQUEST = 400
HTTP_ERROR_SERVER_ERROR = 500

logger = logging.getLogger(__name__)

class ContinueRequest(Exception):
    pass

def TODO_parse_agenda(self):
    """Parse agenda of new releases and schedule jobs"""
    
    #TODO: calendrier: RSS 2.0
    
    DATEEXP = re.compile("(January|February|March|April|May|June|July|August|September|October|November|December)[ ]+\d+[ ]*,[ ]+\d+[ ]+\d+:\d+")
    url = 'http://www.insee.fr/en/publics/presse/agenda.asp'
    agenda = BeautifulSoup(urllib.request.urlopen(url))
    ul = agenda.find('div',id='contenu').find('ul','liens')
    for li in ul.find_all('li'):
        href = li.find('a')['href']
        groups = parse_theme(href)
        text = li.find('p','info').string
        date = datetime.datetime.strptime(DATEEXP.match(text).group(),'%B %d, %Y %H:%M')
        print(date)

def TODO_parse_theme(self,url):
    """Find updated code groups"""
    
    #    url = "http://localhost:8800/insee/industrial_production.html"
    theme = BeautifulSoup(urllib.request.urlopen(url))
    p = theme.find('div',id='savoirplus').find('p')
    groups = []
    for a in p.find_all('a'):
        groups += [a.string[1:]]
    return groups

class INSEE(Fetcher):
    
    def __init__(self, db=None, sdmx=None, **kwargs):        
        super().__init__(provider_name='INSEE', db=db, **kwargs)
        
        self.provider = Providers(name=self.provider_name,
                                 long_name='National Institute of Statistics and Economic Studies',
                                 region='France',
                                 website='http://www.insee.fr',
                                 fetcher=self)
        
        self.sdmx = sdmx or Request(agency='INSEE')
        
        self._dataflows = None
        self._categoryschemes = None
        self._categorisations = None
    
    def load_structure(self, force=False):
        
        if self._dataflows and not force:
            return
        
        """
        #http://www.bdm.insee.fr/series/sdmx/categoryscheme
        categoryscheme_response = self.sdmx.get(resource_type='categoryscheme', params={"references": None})
        logger.debug(categoryscheme_response.url)
        self._categoryschemes = categoryscheme_response.msg.categoryschemes
    
        #http://www.bdm.insee.fr/series/sdmx/categorisation
        categorisation_response = self.sdmx.get(resource_type='categorisation')
        logger.debug(categorisation_response.url)
        self._categorisations = categorisation_response.msg.categorisations
        """
    
        #http://www.bdm.insee.fr/series/sdmx/dataflow
        dataflows_response = self.sdmx.get(resource_type='dataflow')    
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
        raise NotImplementedError()
        
        self.load_structure(force=False)
        
        def walk_category(category, 
                          categorisation, 
                          dataflows, 
                          name=None, 
                          category_code=None):
            
            if name is None:
                name = category['name']
            
            if category_code is None:
                category_code = category['id']
            
            children_ids = []
            
            if 'subcategories' in category:
                
                for subcategory in category['subcategories']:
                    
                    children_ids.append(walk_category(subcategory, 
                                                      categorisation, 
                                                      dataflows))
                
                category = Categories(provider_name=self.provider_name,
                                      name=name,
                                      category_code=category_code,
                                      children=children_ids,
                                      doc_href=None,
                                      last_update=datetime.now(),
                                      exposed=False,
                                      fetcher=self)
                
                return category.update_database()
                
            else:
                for df_id in categorisation[category['id']]:
                    
                    category = Categories(provider_name=self.provider_name,
                                          name=dataflows[df_id][2]['en'],
                                          category_code=category['id'],
                                          children=children_ids,
                                          doc_href=None,
                                          last_update=datetime.now(),
                                          exposed=False,
                                          fetcher=self)
                    
                    return category.update_database()

        walk_category(self._categoryscheme, 
                      self._categorisation, 
                      self._dataflow,
                      name='root',
                      category_code='INSEE_root')

    
    def upsert_dataset(self, dataset_code):

        self.load_structure(force=False)
        
        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        
        if not dataset_code in self._dataflows:
            raise Exception("This dataset is unknown: %s" % dataset_code)
        
        dataflow = self._dataflows[dataset_code]
        
        #cat = self.db[constants.COL_CATEGORIES].find_one({'category_code': dataset_code})
        #dataset.name = cat['name']
        #dataset.doc_href = cat['doc_href']
        #dataset.last_update = cat['last_update']

        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code,
                           name=dataflow.name.en,
                           doc_href=None,
                           last_update=datetime.now(), #TODO:
                           fetcher=self)
        
        dataset_doc = self.db[constants.COL_DATASETS].find_one({'provider_name': self.provider_name,
                                                                "dataset_code": dataset_code})
        
        insee_data = INSEE_Data(dataset=dataset,
                                dataset_doc=dataset_doc, 
                                dataflow=dataflow, 
                                sdmx=self.sdmx)
        dataset.series.data_iterator = insee_data
        result = dataset.update_database()
        
        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))
        
        """
        > IDBANK:  A définir dynamiquement sur site ?
        doc_href d'une serie: http://www.bdm.insee.fr/bdm2/affichageSeries?idbank=001694226
        > CODE GROUPE: Balance des Paiements mensuelle - Compte de capital
        http://www.bdm.insee.fr/bdm2/choixCriteres?codeGroupe=1556
        """
        return result

class INSEE_Data(object):
    
    def __init__(self, dataset=None, dataset_doc=None, dataflow=None, sdmx=None):
        """
        :param Datasets dataset: Datasets instance
        :param pandasdmx.model.DataflowDefinition dataflow: instance of DataflowDefinition
        :param RequestINSEE sdmx: SDMX Client  
        """        
        self.cpt = 0
        
        self.dataset = dataset
        self.dataset_doc = dataset_doc
        self.last_update = None
        if self.dataset_doc:
            self.last_update = self.dataset_doc["last_update"]
        #    self.dataset.last_update = self.last_update
        
        self.provider_name = self.dataset.provider_name
        self.dataset_code = self.dataset.dataset_code
        self.dataflow = dataflow
        self.sdmx = sdmx
        
        self.dimension_list = self.dataset.dimension_list
        self.attribute_list = self.dataset.attribute_list
        
        self.datastructure = self.sdmx.get(resource_type='datastructure', 
                                           resource_id=self.dataset_code,
                                           headers=None)
        
        #TODO: simplifier
        self.dsd = self.datastructure.msg.datastructures[self.dataset_code]    

        self.dimensions = {} # array of pandasdmx.model.Dimension
        for dim in self.dsd.dimensions.aslist():
            if dim.id not in ['TIME', 'TIME_PERIOD']:
                self.dimensions[dim.id] = dim
            
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
        """
        if not self.last_update:
            return True
        
        series_updated = datetime.strptime(series.attrib.LAST_UPDATE, "%Y-%m-%d")
        _is_updated = series_updated > self.last_update

        if not _is_updated and logger.isEnabledFor(logging.INFO):
            logger.info("bypass updated dataset_code[%s][%s] - idbank[%s][%s]" % (self.dataset_code,
                                                                                 self.last_update, 
                                                                                 series.attrib.IDBANK,
                                                                                 series_updated))
        
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
                                 headers=None,
                                 key=key)
            
        try:
            ''' First call - all series '''
            _data = _request()
            for s in _data.msg.data.series:
                if self.is_valid_frequency(s):# and self.is_updated(s):
                    yield s
                else:
                    yield            
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
                            if self.is_valid_frequency(s):# and self.is_updated(s):
                                yield s
                            else:
                                yield
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

    def get_series_key(self, series):
        return series.attrib.IDBANK

    def get_series_name(self, series):
        return series.attrib.TITLE #TODO: english

    def get_series_frequency(self, series):
        if 'FREQ' in series.key._fields:
            return series.key.FREQ
        elif 'FREQ' in series.attrib._fields:
            return series.attrib.FREQ

        raise Exception("Not FREQ field in series.key or series.attrib")
    
    def get_last_update(self, series):
        return datetime.strptime(series.attrib.LAST_UPDATE, "%Y-%m-%d")
    
    def debug_series(self, series, bson):
        if logger.isEnabledFor(logging.DEBUG):
            import json
            try:
                _debug = {
                    "dataset_code": self.dataset_code,
                    "key": bson['key'],
                    "last_update": str(self.last_update),
                    "dimensions_keys": self.dim_keys,
                    "attrib": series.attrib._asdict().items(),
                    "series.key": series.key._asdict().items(),
                }            
                logger.debug(json.dumps(_debug))
            except:
                pass
            
    def fixe_frequency(self, bson):
        #TODO: T equal Trimestrial for INSEE
        if bson['frequency'] == "T":
            logger.warning("Replace T frequency by Q - dataset[%s] - idbank[%s]" % (self.dataset_code, bson['key']))
            bson['frequency'] = "Q"
            
    def get_attributes(self, series):
        attributes = {}
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
        self.fixe_frequency(bson)
        
        bson['attributes'] = self.get_attributes(series)
            
        dimensions = dict(series.key._asdict())
        
        for d, value in series.attrib._asdict().items():
            if d in ['IDBANK', 'LAST_UPDATE', 'TITLE']:
                continue
            dim_short_id = value
            #TODO: le name doit se trouver ailleurs ou les extraires et les stocker dans le code
            #http://www.bdm.insee.fr/series/sdmx/codelist/FR1/CL_FREQ
            #http://www.bdm.insee.fr/series/sdmx/codelist/FR1/CL_UNIT_MULT
            dim_long_id = self.concept_name(d)            
            dimensions[d] = self.dimension_list.update_entry(d, dim_short_id, dim_long_id)

        bson['dimensions'] = dimensions
        
        '''INSEE ordered dates (desc) - 2015 -> 2000'''
        _dates = [o.dim for o in series.obs(with_values=False, with_attributes=False, reverse_obs=False)]
        bson['start_date'] = pandas.Period(_dates[-1], freq=bson['frequency']).ordinal
        bson['end_date'] = pandas.Period(_dates[0], freq=bson['frequency']).ordinal
        
        bson['values'] = []
        for o in series.obs(with_values=True, with_attributes=False, reverse_obs=True):
            if str(o.value).lower() == "nan":
                bson['values'].append("NAN")
            else:
                bson['values'].append(str(o.value))
        
        return bson

