# -*- coding: utf-8 -*-

"""
http://www.bdm.insee.fr/bdm2/statique.action?page=sdmx

Chez INSEE:
    BDM: http://www.bdm.insee.fr/bdm2/index?request_locale=fr
    Bulletin statistique: http://www.insee.fr/fr/bases-de-donnees/bsweb/

The BDM is the main time-series and indices database and provides a large set of socioeconomic statistics.
     la Banque de données macro-économiques (BDM)

Limit BDM: http://www.insee.fr/bdm/fiches/wadl.xml

500 séries identifiées par leur idbank MAX
    http://www.bdm.insee.fr/series/sdmx/data/SERIES_BDM/001565183+001690224+000067677?startPeriod=2010

2000 séries en une seule requête pour les series par Groupe
    http://www.bdm.insee.fr/series/sdmx/data/CNA-2010-ERE-A88/..VAL?lastNObservations=3

Lorsque plusieurs formats sont possibles pour une même ressource, le choix se fera par négociation de contenu HTTP (utilisation de l’en-tête Accept de la requête).

La compression de la réponse est également proposée si le client la demande dans sa requête (en-tête Accept-Encoding: gzip).


codes = sdmx.ecb.codes(self.key_family)

        repo.categories:
            http://www.bdm.insee.fr/series/sdmx/categoryscheme
        repo.dataflows():
            http://www.bdm.insee.fr/series/sdmx/dataflow/INSEE/all/latest
        repo.codes():    
            http://www.bdm.insee.fr/series/sdmx/datastructure/INSEE/DSD_???
        repo.raw_data()
            datas = repo.raw_data('IPI-2010-A21', '')
            http://www.bdm.insee.fr/series/sdmx/data/IPI-2010-A21/
            
        application/vnd.sdmx.genericdata+xml;version=2.1: format GenericData
        application/vnd.sdmx.structurespecificdata+xml;version=2.1: format StructureSpecificData
        par défaut si non précisé: format StructureSpecificData
            
         
Exemples pour le groupe sur l'IPI base 2010 niveau A21 :
    /data/IPI-2010-A21 : groupe complet (20 séries)
    /data/IPI-2010-A21/M.B+C+D.CVS-CJO : seulement 3 indices mensuels cvs-cjo pour les sections B, C et D
    /data/IPI-2010-A21/A..BRUT : seu            

file:///C:/temp/cepremap/fetchers-sources/INSEE/insee/insee/IPI-2010-A21.xml
file:///V:/git/cepremap/src/dlstats/sdmx_test1.xml

http://apprendre-python.com/page-xml-python-xpath

http://www.bdm.insee.fr/bdm2/statique.action?request_locale=en&page=sdmx
http://www.bdm.insee.fr/bdm2/statique.action?page=sdmx
http://www.insee.fr/bdm/fiches/doc-ws-sdmx.pdf

DSD: Dans la BDM, il y a une DSD pour chaque groupe de séries (soit environ 400), 
Dimensions: Il y en a entre 1 et 6 suivant le dataflow
attributs : Il y a 10 attributs maximum au niveau d’une série.
            la périodicité n’est ajoutée que s’il n’y a pas de dimension SDMX équivalente
            
            FREQ - périodicité - une lettre : A, T, M, B ou S

http://www.bdm.insee.fr/series/sdmx/dataflow/INSEE

sdmx_tree_iterator = lxml.etree.iterparse('sdmx_test1.xml', events=['end','start-ns'])
nsmap = {}
for event, element in sdmx_tree_iterator:
    if event == 'start-ns':
        ns, url = element
        if len(ns) > 0:
            nsmap[ns] = url
    else:
        break

>>> nsmap
{'footer': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message/footer', 'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common', 'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message', 'xsi': 'http://www.w3.org/2001/XMLSchema-instance', 'generic': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic'}

100: La requête ne fournit aucun résultat     : 404
140: La syntaxe de la requête est invalide    : 400
500: Erreur interne au serveur                : 500
510: La réponse est trop volumineuse, il faut limiter la quantité d’informations demandée: 413
"""

import time
import os
import tempfile
import urllib
from datetime import datetime
import logging

from pprint import pprint

import requests

import pandas

from pandasdmx import Request
from pandasdmx.reader.sdmxml import Reader

from dlstats.fetchers._commons import Fetcher, Categories, Datasets, Providers
from dlstats import constants
from collections import OrderedDict

HTTP_ERRORS = {
    '404': "No result",
    '400': "Bad request",
    '500': "Server error",
    '413': "Response too long. Split request",
}


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
    
class RequestINSEE(Request):
    
    HEADERS = {
        'structure': None,
        'datas': {"Accept": "application/vnd.sdmx.genericdata+xml;version=2.1"}
        #"application/vnd.sdmx.genericdata+xml;version=2.1"
        #"application/vnd.sdmx.generictimeseriesdata+xml;version=2.1"
    }
    
    Request._agencies['INSEE'] = {'name': 'INSEE', 
                                  'url': 'http://www.bdm.insee.fr/series/sdmx'}

    def _get_reader(self):
        return ReaderINSEE(self)

def parse_agenda(self):
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

def parse_theme(self,url):
    """Find updated code groups"""
    
    #    url = "http://localhost:8800/insee/industrial_production.html"
    theme = BeautifulSoup(urllib.request.urlopen(url))
    p = theme.find('div',id='savoirplus').find('p')
    groups = []
    for a in p.find_all('a'):
        groups += [a.string[1:]]
    return groups

"""
class RepositoryINSEE(Repository):
    REQUESTS_HEADERS = {
        'user-agent': 'dlstats - https://github.com/Widukind/dlstats',
        'Accept': 'application/vnd.sdmx.genericdata+xml;version=2.1'
    }
    
    def __init__(self, 
                 sdmx_url='http://www.bdm.insee.fr/series/sdmx',
                 format='xml', version='2_1', agencyID='INSEE',
                 timeout=60*5, requests_client=None):
        Repository.__init__(self, sdmx_url=sdmx_url, format=format, version=version, agencyID=agencyID, timeout=timeout, requests_client=requests_client)
        
        self.category_scheme_url = 'http://www.bdm.insee.fr/series/sdmx/categoryscheme'
    
    def query_rest_xml(self, url, filepath=None):
        # Fetch data from the provider    
        sdmx.logger.info('Requesting %s', url)
        client = self.requests_client or requests
        
        response = client.get(url, timeout=self.timeout, 
                             headers=self.REQUESTS_HEADERS,
                             stream=True)

        if response.status_code == requests.codes.ok:
            #response_str = request.text.encode('utf-8')
            
            from pprint import pprint as pp
            print("---------------------------------------------------------")
            pp(response.headers)
            print(response.status_code, response.reason, response.url)
            #print("size: ", len(response_str))
            print("---------------------------------------------------------")
            
            filepath = filepath or os.path.abspath(os.path.join(tempfile.mkdtemp(prefix="sdmx"), "sdmx.xml"))
            
            with open(filepath, 'wb') as fp:
                for chunk in response.iter_content(): 
                    fp.write(chunk)
            
            return filepath
        else:
            raise ValueError("Error getting client({})".format(response.status_code))
        
        #return lxml.etree.fromstring(response_str)

        #response = requests.get('http://www.bdm.insee.fr/series/sdmx/data/IPI-2010-A21/', headers=headers, stream=True)
        #lxml.etree.iterparse
"""

class INSEE(Fetcher):
    
    def __init__(self, db=None):        
        
        super().__init__(provider_name='INSEE', db=db)
        
        self.provider = Providers(name=self.provider_name,
                                 long_name='National Institute of Statistics and Economic Studies',
                                 region='France',
                                 website='http://www.insee.fr',
                                 fetcher=self)
        
        """
        cache_name='cache', backend=None, expire_after=None,
                 allowable_codes=(200,), allowable_methods=('GET',),
                 session_factory=CachedSession, **backend_options
                 )
        """
        cache_options = dict(backend='sqlite', 
                             expire_after=None,
                             location="C:/temp/cepremap/fetchers-sources/INSEE/xml/cache")
        
        self.sdmx = RequestINSEE(agency='INSEE', 
                                 cache=cache_options)
        
        self.tmpdir = tempfile.mkdtemp(prefix="sdmx-insee")
        
        self._dataflows = None
        self._categoryschemes = None
        self._categorisations = None
    
    def load_structure(self, force=False):
        
        if self._dataflows and not force:
            return
        
        categoryscheme_fp = os.path.abspath(os.path.join(self.tmpdir, 'categoryscheme.xml'))
        categorisation_fp = os.path.abspath(os.path.join(self.tmpdir, 'categorisation.xml'))
        dataflow_fp = os.path.abspath(os.path.join(self.tmpdir, 'dataflow.xml'))
        print("dataflow_fp : ", dataflow_fp)
        
        #http://www.bdm.insee.fr/series/sdmx/categoryscheme
        categoryscheme_response = self.sdmx.get(resource_type='categoryscheme', tofile=categoryscheme_fp, params={"references": None})
        logger.debug(categoryscheme_response.url)
        self._categoryschemes = categoryscheme_response.msg.categoryschemes
    
        #http://www.bdm.insee.fr/series/sdmx/categorisation
        categorisation_response = self.sdmx.get(resource_type='categorisation', tofile=categorisation_fp)
        logger.debug(categorisation_response.url)
        self._categorisations = categorisation_response.msg.categorisations
    
        #http://www.bdm.insee.fr/series/sdmx/dataflow
        dataflows_response = self.sdmx.get(resource_type='dataflow', tofile=dataflow_fp)    
        logger.debug(dataflows_response.url)
        self._dataflows = dataflows_response.msg.dataflows

    def upsert_all_datasets(self):
        start = time.time()        
        logger.info("update fetcher[%s] - START" % (self.provider_name))
        
        self.load_structure(force=False)
        
        for dataset_code in self._dataflows:
            self.upsert_dataset(dataset_code) 

        end = time.time() - start
        logger.info("update fetcher[%s] - END - time[%.3f seconds]" % (self.provider_name, end))

    def datasets_list(self):
        #TODO: from DB
        self.load_structure(force=False)
        return self._dataflows.keys()
        
    def datasets_long_list(self):
        #TODO: from DB
        self.load_structure(force=False)
        return [(key, dataset.name.en) for key, dataset in self._dataflows.items()]

    @property
    def categories(self):
        """
        FIXME: récupérer version non FR
        
        Renvoi dict avec ces clés:
            ['name', 'subcategories', 'id']
            
            >>> categories['name']
            'DataFlows categorisation'
            >>> categories['id']
            'CLASSEMENT_DATAFLOWS'            

            >>> categories['subcategories'][0]
            {'name': 'National accounts (GDP, consumption...)', 'subcategories': [{'name': 'Annual national accounts', 'subcategories':
        
        http://www.bdm.insee.fr/series/sdmx/categoryscheme:
            response.headers:
            {'Content-Type': 'application/xml', 'Server': 'INSEE', 'Content-Encoding': 'gzip', 'Date': 'Tue, 17 Nov 2015 15:08:22 GMT', 'Transfer-Encoding': 'chunked', 'X-Frame-Options': 'DENY'}
            
        #Dataflow: structure permettant de classer les datasets
        >>> d = repo.dataflows('CONSO-MENAGES')
        200 OK http://www.bdm.insee.fr/series/sdmx/dataflow/INSEE/all/latest
        
        >>> len(d)
        629
        """
        return self.sdmx_repo.categories

    @property
    def categorisation(self):
        return self.sdmx_repo.categorisation

    def upsert_categories(self):
        
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
        
        start = time.time()

        self.load_structure(force=False)
        
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        
        if not dataset_code in self._dataflows:
            raise Exception("This dataset is unknown" + dataset_code)
        
        dataflow = self._dataflows[dataset_code]
        
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code,
                           name=dataflow.name.en,
                           doc_href=None,
                           last_update=None,
                           fetcher=self)
        #cat = self.db[constants.COL_CATEGORIES].find_one({'categoryCode': dataset_code})
        #dataset.name = cat['name']
        #dataset.doc_href = cat['docHref']
        #dataset.last_update = cat['lastUpdate']
        insee_data = INSEE_Data(dataset=dataset, 
                                dataflow=dataflow, 
                                sdmx=self.sdmx, 
                                tmpdir=self.tmpdir)
        dataset.series.data_iterator = insee_data
        dataset.update_database()

        end = time.time() - start
        logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))
        
        """
        >>> list(d['datas'][0].data.series)[0].attrib.IDBANK
        '001694226'
        
        > A définir dynamiquement sur site ?
        docHref d'une serie: http://www.bdm.insee.fr/bdm2/affichageSeries?idbank=001694226

        http://www.bdm.insee.fr/bdm2/affichageSeries?idbank=001694226
        &codeGroupe=1007
        
        > Balance des Paiements mensuelle - Compte de capital
        http://www.bdm.insee.fr/bdm2/choixCriteres?codeGroupe=1556
        """

class INSEE_Data(object):
    
    def __init__(self, dataset=None, dataflow=None, sdmx=None, tmpdir=None):
        
        self.cpt = 0
        
        self.dataset = dataset
        
        self.provider_name = self.dataset.provider_name
        self.dataset_code = self.dataset.dataset_code
        self.dataflow = dataflow
        self.sdmx = sdmx
        
        self.dimension_list = self.dataset.dimension_list
        self.attribute_list = self.dataset.attribute_list
        
        self.datastructure = self.sdmx.get(resource_type='datastructure', 
                                           resource_id=self.dataset_code,
                                           tofile="C:/temp/cepremap/fetchers-sources/INSEE/xml/datastructure-%s.xml" % self.dataset_code)
        
        self.dsd = self.datastructure.msg.datastructures[self.dataset_code]    

        self.dimensions = {}
        for dim in self.dsd.dimensions.aslist():
            if dim.id not in ['TIME', 'TIME_PERIOD']:
                self.dimensions[dim.id] = dim
            
        self.dim_keys = list(self.dimensions.keys())
        self.dimension_list.set_dict(self.dimensions_to_dict())
        
        '''Selection de la dimension avec le moins de variantes'''
        self.dim_select = self.select_short_dimension()
        
        self.current_series = {}
        
        self.rows = self.get_series(self.dataset_code)
        
        """
        >>> d['datastructure'].msg.codelists.keys()
        dict_keys(['CL_COMPTE', 'CL_NI00541'])
        
        >>> d['datastructure'].msg.codelists.aslist()[0].keys()
        dict_keys(['D', 'S', 'B', 'C'])        
        
        >>> d['dsd'].dimensions.keys()
        dict_keys(['COMPTE', 'TIME_PERIOD', 'INSTRUMENT'])        
        
        >>> d['datastructure'].msg.conceptschemes.aslist()[0]['IDBANK'].name.en
        'Numerical identifier used in the BDM website'
        
        >>> d['dsd'].dimensions.aslist()[0].concept.name.en
        'Nature of the transaction'
        >>> d['dsd'].dimensions.aslist()[0].concept.id
        'COMPTE'
        
        d['dsd'].dimensions['COMPTE'].concept.name
        
        >>> list(d['dsd'].dimensions['COMPTE'].local_repr.enum.values())[0].name.en
        'Debits'
        
        >>> list(d['dsd'].dimensions['COMPTE'].local_repr.enum.keys())
        ['D', 'S', 'B', 'C']
        
        list(d['dsd'].dimensions['INSTRUMENT'].local_repr.enum.keys())
        
        list(d['dsd'].dimensions.aslist()[1].local_repr.enum.items())[0]        

        >>> list(d['dsd'].dimensions['COMPTE'].local_repr.enum.values())
        [Code | D | Debits, Code | S | Balance, Code | B | Balance (credits minus debits), Code | C | Credits]
        
        >>> d['dsd'].dimensions.aslist()
        [<pandasdmx.model.Dimension object at 0x042EA7F0>, <pandasdmx.model.Dimension object at 0x042EA850>, <pandasdmx.model.TimeDimension object at 0x042EA890>]        
        
        >>> list(d['dsd'].dimensions['COMPTE'].local_repr.enum.values())[0].name.en
        'Debits'        
        
        >>> d['datastructure'].msg.codelists.aslist()[0].keys()
        dict_keys(['D', 'S', 'B', 'C'])
        >>> d['datastructure'].msg.codelists.aslist()[0].values()
        dict_values([Code | D | Debits, Code | S | Balance, Code | B | Balance (credits minus debits), Code | C | Credits])
        
        Codelist id="CL_NI00541" : c'est l'énumération de INSTRUMENT ! 
           Code id="014"
               Current account - Goods - General merchandise - Adjustments
        
        Codelist id="CL_COMPTE"
        
        Concept id="INSTRUMENT"
        """
        
        #TODO: préférences dans la selection des fonctions

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

        def _request(key=''):
            return self.sdmx.get(resource_type='data', 
                                  resource_id=dataset_code,
                                  key=key, 
                                  tofile="data.xml",
                                  headers=self.sdmx.HEADERS['datas'])
            
        try:
            _data = _request()
            for s in _data.msg.data.series:
                if self.is_valid_frequency(s):
                    yield s                        
        except requests.exceptions.HTTPError as err:

            if err.response.status_code == HTTP_ERROR_LONG_RESPONSE:
                
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
        bson['name'] = series.attrib.TITLE #TODO: en
        bson['lastUpdate'] = datetime.strptime(series.attrib.LAST_UPDATE, "%Y-%m-%d")
        self.dataset.last_update = bson['lastUpdate']

        print(self.dataset_code, bson['key'], self.dataset.last_update, self.dim_keys, series.attrib, series.key)

        if 'FREQ' in series.key._fields:
            bson['frequency'] = series.key.FREQ
        elif 'FREQ' in series.attrib._fields:
            bson['frequency'] = series.attrib.FREQ
        else:
            raise Exception("Not FREQ field in series.key or series.attrib")
        
        if bson['frequency'] == "S":
            raise Exception("Semestrial period not implemented")
        
        #self.attribute_list.set_dict(attributes)
        attributes = {}
        for obs in series.obs(with_values=False, with_attributes=True, reverse_obs=True):
            for key, value in obs.attrib._asdict().items():
                if not key in attributes:
                    attributes[key] = []
                attributes[key].append(value)
        
        """
        #http://www.bdm.insee.fr/series/sdmx/dataflow/FR1/BPM6-CCAPITAL?references=all
        <str:Concept id="OBS_STATUS" urn="urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=FR1:CONCEPTS_INSEE(1.0).OBS_STATUS"><com:Name xml:lang="fr">Statut de l'observation</com:Name><com:Name xml:lang="en">Observation Status</com:Name></str:Concept>
        
        >>> d['datastructure'].msg.datastructures.aslist()[0].measures.aslist()[0].concept.name.en
        'Observation Value'
        
        >>> d['datastructure'].msg.datastructures.aslist()[0].measures.keys()
        dict_keys(['OBS_VALUE'])                        
        """
        """
        attributes = {}
        for a, value in _attributes.items():
            attr_short_id = value
            attr_long_id = self.concept_name(a)
            #update_entry(dim_name,dim_short_id,dim_long_id))
            attributes[a] = self.attribute_list.update_entry(a, attr_short_id, attr_long_id)        
        """

        bson['attributes'] = attributes
            
        dimensions = dict(series.key._asdict())
        """
        for d in self.dim_keys:
            dim = self.dimensions[d]
            dim_short_id = d
            dim_long_id = dim.concept.name.en
            dimensions[d] = self.dimension_list.update_entry(d, dim_short_id, dim_long_id)
        """
        
        """
        >>> series.attrib._fields
        ('FREQ', 'IDBANK', 'TITLE', 'LAST_UPDATE', 'UNIT_MEASURE', 'UNIT_MULT', 'REF_AREA', 'DECIMALS', 'TIME_PER_COLLECT')
        
                "FREQ" : [
                    ["M", "Frequency"]
                ],                
                'freq': [
                    ['A', 'Annual'],
                    ['S', 'Half-yearly, semester'],
                ]
                
        >>> d['dsd'].attributes.keys()
        dict_keys(['IDBANK', 'UNIT_MEASURE', 'REF_AREA', 'DECIMALS', 'LAST_UPDATE', 'UNIT_MULT', 'TITLE', 'BASE_PER', 'TIME_PER_COLLECT', 'EMBARGO_TIME', 'FREQ', 'OBS_STATUS'])
        
        >>> d['dsd'].attributes['DECIMALS'].concept.name.en
        'Decimals'
        
        > title FR:
        IDBANK="001694169" TITLE="Balance des paiements - Débit - Compte de capital - Transferts en capital - Secteurs hors administrations publiques - Remises de dettes - Données brutes"
        
        > db.series.find({provider: "INSEE", key: "001694169"}, {releaseDates: 0, values: 0, attributes: 0})[0]
        {
                "_id" : ObjectId("567955842d4b252cd4dabbc8"),
                "endDate" : 549,
                "provider" : "INSEE",
                "dimensions" : {
                        "FREQ" : "M",
                        "UNIT_MULT" : "6",
                        "UNIT_MEASURE" : "EUR",
                        "TIME_PER_COLLECT" : "PERIODE",
                        "REF_AREA" : "FE",
                        "COMPTE" : "D",
                        "DECIMALS" : "0",
                        "INSTRUMENT" : "170"
                },
                "datasetCode" : "BPM6-CCAPITAL",
                "name" : "Balance des paiements - Débit - Compte de capital - Transferts en capital - Secteurs hors administrations publiques - Remises de dettes - Données brutes",
                "key" : "001694169",
                "startDate" : 456,
                "frequency" : "M"
        }        
    
        > db.datasets.find({provider: "INSEE"})[0].dimensionList.UNIT_MEASURE
            >  Ok, on peut retrouver "UNIT_MEASURE" : "EUR", par le dataset:
            [ [ "EUR", "Unit" ] ]
        
        > Balance des paiements
        <str:Category id="BALANCE-PAIEMENTS" urn="urn:sdmx:org.sdmx.infomodel.categoryscheme.Category=FR1:CLASSEMENT_DATAFLOWS(1.0).ECHANGES-EXT.BALANCE-PAIEMENTS">
            <com:Name xml:lang="fr">Balance des paiements</com:Name>
            <com:Name xml:lang="en">Balance of payments</com:Name>
        </str:Category>
        
        > Débit 
        ?
        
        > Compte de capital - Transferts en capital
        <str:Code id="004" urn="urn:sdmx:org.sdmx.infomodel.codelist.Code=FR1:CL_NI00541(1.0).004">
            <com:Name xml:lang="fr">Compte de capital - Transferts en capital</com:Name>
            <com:Name xml:lang="en">Capital account - Capital transfers</com:Name>
        </str:Code>
        
        > Secteurs hors administrations publiques
        
        > Remises de dettes
        
        > Données brutes
                        
        """
        for d, value in series.attrib._asdict().items():
            if d in ['IDBANK', 'LAST_UPDATE', 'TITLE']:
                continue
            dim_short_id = value
            dim_long_id = self.concept_name(d)            
            dimensions[d] = self.dimension_list.update_entry(d, dim_short_id, dim_long_id)

        """
        dimensions = OrderedDict([(key.lower(), value) for key,value in series.attrib.items() if key != 'TIME_FORMAT'])
        ItemsView(OrderedDict([('FREQ', 'M'), ('IDBANK', '001694163'), ('TITLE', 'Balance des paiements - Débit - Compte de capital - Données brutes'), ('LAST_UPDATE', '2015-12-11'), ('UNI
        T_MEASURE', 'EUR'), ('UNIT_MULT', '6'), ('REF_AREA', 'FE'), ('DECIMALS', '0'), ('TIME_PER_COLLECT', 'PERIODE')]))
        >>> list(s.attrib._asdict().items())
        [('FREQ', 'M'), ('IDBANK', '001694163'), ('TITLE', 'Balance des paiements - Débit - Compte de capital - Données brutes'), ('LAST_UPDATE', '2015-12-11'), ('UNIT_MEASURE', 'EUR'), ('
        UNIT_MULT', '6'), ('REF_AREA', 'FE'), ('DECIMALS', '0'), ('TIME_PER_COLLECT', 'PERIODE')]                
        """

        bson['dimensions'] = dimensions
        
        """
        >>> data.attrib
        Attrib(
            FREQ='M', 
            IDBANK='001694099', 
            TITLE='Balance des paiements - Crédit - Compte de capital - Données brutes', 
            LAST_UPDATE='2015-12-11', 
            UNIT_MEASURE='EUR', 
            UNIT_MULT='6', 
            REF_AREA='FE', 
            DECIMALS='0', 
            TIME_PER_COLLECT='PERIODE'
        )
        OBSTATUS:
            A: valeur définitive
            P: provisoire
            SD: semi-définitive
            R: révisée
            E: estimée
            O: manquant        
        
        http://www.bdm.insee.fr/series/sdmx/codelist/FR1/CL_OBS_STATUS
        
        obs(self, with_values=True, with_attributes=True, reverse_obs=False)
        >>> obs_zip = iter(zip(*s.obs(str, '', True)))
        >>> obs_dim = next(obs_zip)
        >>> obs_dim
        ('2008-01', '2008-02', '2008-03', '2008-04', '2008-05', ...)        
        
        for obs in s.obs(True, True, True): print(obs.dim, obs.value, obs.attrib.OBS_STATUS)
        2013-03 43 A
        2013-04 86 A
        2013-05 29 A        
        """
        
        _dates = [o.dim for o in series.obs(False, False, True)]
        bson['startDate'] = pandas.Period(_dates[0], freq=bson['frequency']).ordinal
        bson['endDate'] = pandas.Period(_dates[-1], freq=bson['frequency']).ordinal
        
        #pprint(bson)

        bson['values'] = [str(o.value) for o in series.obs(with_values=True, with_attributes=False, reverse_obs=True)]
        
        return bson

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, filename="insee.log", 
                        format='%(asctime)s %(name)s: [%(levelname)s] - %(message)s')
    
    insee = INSEE()
    
    insee.db[constants.COL_DATASETS].remove({"provider": "INSEE"})
    insee.db[constants.COL_SERIES].remove({"provider": "INSEE"})
    
    insee.upsert_all_datasets()
    
    #insee.upsert_categories()
    #insee.upsert_dataset('IPC-1998-COICOP')
    """
    dlstats fetchers datasets -f INSEE | awk '{ print "\""$1"\","}'    
    """
    _datasets = [
        "IPI-1985-NAP100",
        "CNA-2005-TOF-FLUX",
        "ENQ-CONJ-MENAGES",
        "IPPI-2010-ENS-RGP",
        "IPC-1998-ISJ",
        "LOGEMENT-RESPRINC-TUU",
        "REVENUS-MEN-DEP",
        "DEFAILLANCES-ENT-REG-ENS-ANC",
        "ICA-CS-2010-JZ",
        "IPC-1998-DOM",
        "IPPI-2005-EXT-RGP",
        "CREATIONS-ENT-REG-CVS-ANC",
        "CNT-2005-CPB-RGP",
        "ICA-CS-2010-D46",
        "CNA-2005-ERE-A17",
        "ICA-IC-1995-ENS",
        "ENQ-CONJ-SERV-ACT9",
        "ENQ-CONJ-ART-BAT-TRAVAUX",
        "IPI-1985-NAP600-RGP",
        "ICA-CS-1990-NAF4",
        "ENQ-CONJ-ACT-IND-TRIM-ENT-RGP",
        "ICE-1995-NES16-5ZONES",
        "IPC-1990-RGP-ALIM",
        "CNT-2005-CSI-EMP",
        "ICE-2005-CTCI",
        "IPC-1990-COICOP-NONALIM",
        "ICA-CS-2010-HZ",
        "ENQ-CONJ-INV-IND-REVISION",
        "ENQ-CONJ-IND-BAT-TRAVAUX",
        "CNA-2010-PIB",                 
    ]
    #insee.upsert_dataset("BPM6-CCAPITAL")
    """
    for d in _datasets:
        try:
            insee.upsert_dataset(d)
        except Exception as err:
            logger.error("dataset[%s] - error[%s]" % (d, str(err)))
            raise
    """
    
    """
    COLLECT='PERIODE') SeriesKey(QUESTION='DET', CLIENTELE='EN', CORRECTION='CVS')
    ENQ-CONJ-IND-BAT-CLIENTS 001586816 2015-10-22 00:00:00 ['CLIENTELE', 'QUESTION', 'CORRECTION'] Attrib(FREQ='T', IDBANK='001586816', TITLE="Enquête mensuelle de conjoncture dans l'i
    ndustrie du bâtiment - Évolution des délais de paiement - Ensemble - Série brute", LAST_UPDATE='2015-10-22', UNIT_MEASURE='PCT', UNIT_MULT='0', REF_AREA='FM', DECIMALS='0', TIME_PE
    R_COLLECT='PERIODE') SeriesKey(QUESTION='DET', CLIENTELE='EN', CORRECTION='BRUT')
    URL :  http://www.bdm.insee.fr/series/sdmx/datastructure/INSEE/ENQ-CONJ-TRES-IND-INFLUENCE-CR
    URL :  http://www.bdm.insee.fr/series/sdmx/data/ENQ-CONJ-TRES-IND-INFLUENCE-CR
    Traceback (most recent call last):
      File "v:\python3\Lib\runpy.py", line 170, in _run_module_as_main
        "__main__", mod_spec)
      File "v:\python3\Lib\runpy.py", line 85, in _run_code
        exec(code, run_globals)
      File "V:\git\cepremap\src\dlstats\dlstats\fetchers\insee.py", line 838, in <module>
        insee.upsert_all_datasets()
      File "V:\git\cepremap\src\dlstats\dlstats\fetchers\insee.py", line 277, in upsert_all_datasets
        self.upsert_dataset(dataset_code)
      File "V:\git\cepremap\src\dlstats\dlstats\fetchers\insee.py", line 411, in upsert_dataset
        dataset.update_database()
      File "V:\git\cepremap\src\dlstats\dlstats\fetchers\_commons.py", line 321, in update_database
        schemas.dataset_schema(self.bson)
      File "c:\temp\cepremap\venv-dlstats\lib\site-packages\voluptuous.py", line 333, in __call__
        return self._compiled([], data)
      File "c:\temp\cepremap\venv-dlstats\lib\site-packages\voluptuous.py", line 631, in validate_dict
        return base_validate(path, iteritems(data), out)
      File "c:\temp\cepremap\venv-dlstats\lib\site-packages\voluptuous.py", line 467, in validate_mapping
        raise MultipleInvalid(errors)
    voluptuous.MultipleInvalid: expected datetime for dictionary value @ data['lastUpdate']    
    """
