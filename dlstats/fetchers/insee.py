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

from pandasdmx.reader.sdmxml import Reader
from pandasdmx.utils import namedtuple_factory
from pandasdmx.api import Request

from dlstats.fetchers._commons import Fetcher, Categories, Datasets, Providers
from dlstats import constants
from collections import OrderedDict

HTTP_ERROR_LONG_RESPONSE = 413
HTTP_ERROR_NO_RESULT = 404
HTTP_ERROR_BAD_REQUEST = 400
HTTP_ERROR_SERVER_ERROR = 500

logger = logging.getLogger(__name__)

class ContinueRequest(Exception):
    pass

class ReaderINSEE(Reader):

    Reader._nsmap.update({
        'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
        'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
        'generic': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic',
    })
    
    def series_key(self, sdmxobj):
        #tmp patch for "-" in series dimensions name
        series_key_id = self._paths['series_key_id_path'](sdmxobj._elem)
        series_key_id = ",".join(series_key_id).replace("-", "_").split(",")
        series_key_values = self._paths[
            'series_key_values_path'](sdmxobj._elem)
        SeriesKeyTuple = namedtuple_factory('SeriesKey', series_key_id)
        return SeriesKeyTuple._make(series_key_values)
    
class RequestINSEE(Request):
    
    HEADERS = {
        'structure': None,
        'datas': {"Accept": "application/vnd.sdmx.genericdata+xml;version=2.1"}
    }
    
    Request._agencies['INSEE'] = {'name': 'INSEE', 
                                  'url': 'http://www.bdm.insee.fr/series/sdmx'}

    def _get_reader(self):
        return ReaderINSEE(self)
    

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
        
        self.sdmx = sdmx or RequestINSEE(agency=self.provider_name)
        
        self._dataflows = None
        self._categoryschemes = None
        self._categorisations = None
    
    def load_structure(self, force=False):
        
        if self._dataflows and not force:
            return
        
        #http://www.bdm.insee.fr/series/sdmx/categoryscheme
        categoryscheme_response = self.sdmx.get(resource_type='categoryscheme', params={"references": None})
        logger.debug(categoryscheme_response.url)
        self._categoryschemes = categoryscheme_response.msg.categoryschemes
    
        #http://www.bdm.insee.fr/series/sdmx/categorisation
        categorisation_response = self.sdmx.get(resource_type='categorisation')
        logger.debug(categorisation_response.url)
        self._categorisations = categorisation_response.msg.categorisations
    
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
                raise 

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
                          categoryCode=None):
            
            if name is None:
                name = category['name']
            
            if categoryCode is None:
                categoryCode = category['id']
            
            children_ids = []
            
            if 'subcategories' in category:
                
                for subcategory in category['subcategories']:
                    
                    children_ids.append(walk_category(subcategory, 
                                                      categorisation, 
                                                      dataflows))
                
                category = Categories(provider=self.provider_name,
                                      name=name,
                                      categoryCode=categoryCode,
                                      children=children_ids,
                                      docHref=None,
                                      lastUpdate=datetime.now(),
                                      exposed=False,
                                      fetcher=self)
                
                return category.update_database()
                
            else:
                for df_id in categorisation[category['id']]:
                    
                    category = Categories(provider=self.provider_name,
                                          name=dataflows[df_id][2]['en'],
                                          categoryCode=category['id'],
                                          children=children_ids,
                                          docHref=None,
                                          lastUpdate=datetime.now(),
                                          exposed=False,
                                          fetcher=self)
                    
                    return category.update_database()

        walk_category(self._categoryscheme, 
                      self._categorisation, 
                      self._dataflow,
                      name='root',
                      categoryCode='INSEE_root')

    
    def upsert_dataset(self, dataset_code):

        self.load_structure(force=False)
        
        start = time.time()
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        
        if not dataset_code in self._dataflows:
            raise Exception("This dataset is unknown" + dataset_code)
        
        dataflow = self._dataflows[dataset_code]
        
        #cat = self.db[constants.COL_CATEGORIES].find_one({'categoryCode': dataset_code})
        #dataset.name = cat['name']
        #dataset.doc_href = cat['docHref']
        #dataset.last_update = cat['lastUpdate']

        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code,
                           name=dataflow.name.en,
                           doc_href=None,
                           last_update=None,
                           fetcher=self)
        
        insee_data = INSEE_Data(dataset=dataset, 
                                dataflow=dataflow, 
                                sdmx=self.sdmx)
        dataset.series.data_iterator = insee_data
        result = dataset.update_database()

        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))
        
        """
        > IDBANK:  A définir dynamiquement sur site ?
        docHref d'une serie: http://www.bdm.insee.fr/bdm2/affichageSeries?idbank=001694226
        > CODE GROUPE: Balance des Paiements mensuelle - Compte de capital
        http://www.bdm.insee.fr/bdm2/choixCriteres?codeGroupe=1556
        """
        return result

class INSEE_Data(object):
    
    def __init__(self, dataset=None, dataflow=None, sdmx=None):
        """
        :param Datasets dataset: Datasets instance
        :param pandasdmx.model.DataflowDefinition dataflow: instance of DataflowDefinition
        :param RequestINSEE sdmx: SDMX Client  
        """        
        self.cpt = 0
        
        self.dataset = dataset
        
        self.provider_name = self.dataset.provider_name
        self.dataset_code = self.dataset.dataset_code
        self.dataflow = dataflow
        self.sdmx = sdmx
        
        self.dimension_list = self.dataset.dimension_list
        self.attribute_list = self.dataset.attribute_list
        
        self.datastructure = self.sdmx.get(resource_type='datastructure', 
                                           resource_id=self.dataset_code)
        
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
                                 key=key, 
                                 headers=self.sdmx.HEADERS['datas'])
            
        try:
            ''' First call - all series '''
            _data = _request()
            for s in _data.msg.data.series:
                if self.is_valid_frequency(s):
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
                            if self.is_valid_frequency(s):
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
        
    def __iter__(self):
        return self

    def __next__(self):          
        try:      
            _series = next(self.rows)
            if not _series:
                raise StopIteration()
        except ContinueRequest:
            _series = next(self.rows)
            
        bson = self.build_series(_series)
        return bson

    def build_series(self, series):
        """
        :param series: Instance of pandasdmx.model.Series
        """
        bson = {}
        bson['provider'] = self.provider_name
        bson['datasetCode'] = self.dataset_code
        bson['key'] = series.attrib.IDBANK
        bson['name'] = series.attrib.TITLE #TODO: english
        bson['lastUpdate'] = datetime.strptime(series.attrib.LAST_UPDATE, "%Y-%m-%d")
        
        #TODO: update dataset.last_update for all series ?
        self.dataset.last_update = bson['lastUpdate']
        
        if logger.isEnabledFor(logging.DEBUG):
            import json
            try:
                _debug = {
                    "dataset_code": self.dataset_code,
                    "key": bson['key'],
                    "last_update": str(self.dataset.last_update),
                    "dimensions_keys": self.dim_keys,
                    "attrib": series.attrib._asdict().items(),
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
        
        #TODO: T equal Trimestrial for INSEE
        if bson['frequency'] == "T":
            logger.warning("Replace T frequency by Q - dataset[%s] - idbank[%s]" % (self.dataset_code, bson['key']))
            bson['frequency'] = "Q"
        
        attributes = {}
        for obs in series.obs(with_values=False, with_attributes=True, reverse_obs=True):
            for key, value in obs.attrib._asdict().items():
                if not key in attributes:
                    attributes[key] = []
                attributes[key].append(value)
        
        bson['attributes'] = attributes
            
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
        
        _dates = [o.dim for o in series.obs(False, False, True)]
        bson['startDate'] = pandas.Period(_dates[0], freq=bson['frequency']).ordinal
        bson['endDate'] = pandas.Period(_dates[-1], freq=bson['frequency']).ordinal
        
        #pprint(bson)

        bson['values'] = [str(o.value) for o in series.obs(with_values=True, with_attributes=False, reverse_obs=True)]
        
        return bson

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, filename="insee.log", 
                        format='%(asctime)s %(name)s: [%(levelname)s] - %(message)s')

    import tempfile
    tmpdir = tempfile.mkdtemp(prefix="sdmx-insee")
    
    #tofile="C:/temp/cepremap/fetchers-sources/INSEE/xml/datastructure-%s.xml" % self.dataset_code

    cache_options = dict(backend='sqlite', 
                         expire_after=None,
                         location="C:/temp/cepremap/fetchers-sources/INSEE/xml/cache")
    
    sdmx = RequestINSEE(agency='INSEE', cache=cache_options)
    
    insee = INSEE(sdmx=sdmx)
    
    insee.db[constants.COL_DATASETS].remove({"provider": "INSEE"})
    insee.db[constants.COL_SERIES].remove({"provider": "INSEE"})
    
    #insee.upsert_all_datasets()
    
    #insee.upsert_categories()
    insee.upsert_dataset('IPC-1998-COICOP')
    
