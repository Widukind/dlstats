# -*- coding: utf-8 -*-

import hashlib
import logging
from collections import OrderedDict, deque
from datetime import datetime
import re

from lxml import etree

from widukind_common import errors
from widukind_common.debug import timeit

from dlstats.utils import Downloader, clean_datetime, get_ordinal_from_period, get_datetime_from_period

logger = logging.getLogger(__name__)

path_name_lang = etree.XPath("./*[local-name()='Name'][@xml:lang=$lang]")

path_ref = etree.XPath("./*[local-name()='Ref']")

REGEX_DATE_P3M = re.compile(r"(.*)-Q(.*)")
REGEX_DATE_P1D = re.compile(r"(\d\d\d\d)(\d\d)(\d\d)")

def xml_get_name(element):
    names = path_name_lang(element, lang="en")
    if not names: 
        names = path_name_lang(element, lang="fr")
    if names:
        return names[0].text
    else:
        return None

def get_nsmap(iterator):
    nsmap = {}
    for event, element in iterator:
        if event == 'start-ns':
            ns, url = element            
            if len(ns) > 0:
                nsmap[ns] = url
        else:
            break
    return nsmap

SPECIAL_DATE_FORMATS = ['P1Y', 'P3M', 'P1M', 'P1D']

def parse_special_date(period, time_format, dataset_code=None):
    if (time_format == 'P1Y'):
        return (period, 'A')
    elif (time_format == 'P3M'):
        m = re.match(REGEX_DATE_P3M, period)
        return (m.groups()[0]+'Q'+m.groups()[1], 'Q')
    elif (time_format == 'P1M'):
        return (period, 'M')
    elif (time_format == 'P1D'):
        m = re.match(REGEX_DATE_P1D, period)
        return ('-'.join(m.groups()),'D')
    else:
        msg = 'TIME FORMAT[%s] for DATASET[%s] not recognized' % (time_format, dataset_code)
        logger.critical(msg)
        raise Exception(msg)    

def get_key_for_dimension(count_dimensions, position, dimension_value):
    sdmx_key = []
    for i in range(count_dimensions):
        if i == position:
            sdmx_key.append(dimension_value)
        else:
            sdmx_key.append(".")
    return "".join(sdmx_key)

def select_dimension(dimension_keys, dimensions, choice="avg"):
    """Renvoi le nom de la dimension qui contiens le nombre de valeures demandés 
    pour servir ensuite de filtre dans le chargement des données (..A.)
    en tenant compte du nombre de dimension et de la position 
    """
    _dimensions = {}
    _min = None
    _max = None
    _avg = None
    
    import statistics
    
    if not dimension_keys or not dimensions:
        return 0, None, []
    
    counts = [len(values) for values in dimensions.values()]
    average = int(statistics.mean(counts))
    
    #for key, values in dimensions.items():
    for key in dimension_keys:
        values = dimensions[key]
        count = len(values)
        if count == 0:
            continue
        
        if not _min:
            _min = (key, count)
            _max = (key, count)
            _avg = (key, count)
            continue
        
        if count < _min[1]:
            _min = (key, count)
        if count > _max[1]:
            _max = (key, count)
        
        if count >= average and count <= average:
            _avg = (key, count)
        
    if choice == "max":
        _key = _max[0]
    elif choice == "min":        
        _key = _min[0]
    else:
        _key = _avg[0]
        
    position = dimension_keys.index(_key)
    dimension_values = list(dimensions[_key])
    return position, _key, dimension_values

#TODO: url diff for data and structure
SDMX_PROVIDERS = {
    'ECB': {
        'name': 'European Central Bank',
        'url': 'http://sdw-wsrest.ecb.int/service',
        'resources': {
            'data': {
                'headers': {},
            },
        },
        'namespaces': {
            'codelist': {
                'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
                'mes': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
                'str': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
                'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
            },
            'datastructure': {
                'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
                'mes': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
                'str': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
                'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
            },                                   
        }
    },
    'INSEE': {
        'name': 'INSEE',
        'url': 'http://www.bdm.insee.fr/series/sdmx',
        'resources': {
            #'data': {
            #    'headers': {'Accept': 'application/vnd.sdmx.genericdata+xml;version=2.1'},
            #},
        },
        'namespaces': {
            'codelist': {
                'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
                'mes': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
                'str': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
            }            
        }
    }
}

class XMLSDMX:

    def __init__(self, 
                 sdmx_url=None, agencyID=None, data_headers={}, structure_headers={},
                 client=None, cache=None, store_filepath=None, use_existing_file=False,  
                 **http_cfg):
        
        self.sdmx_url = sdmx_url
        self.agencyID = agencyID
        if not self.sdmx_url:
            self.sdmx_url = SDMX_PROVIDERS[self.agencyID]["url"]        
        self.data_headers = data_headers
        self.structure_headers = structure_headers
        self.store_filepath = store_filepath
        self.use_existing_file = use_existing_file

    def query_rest(self, url, **kwargs):
        logger.info('Requesting %s', url)
        filename = "%s.xml" % hashlib.sha224(url.encode("utf-8")).hexdigest()
        download = Downloader(url, filename=filename,
                              store_filepath=self.store_filepath,
                              use_existing_file=self.use_existing_file, 
                              **kwargs)
        filepath, response = download.get_filepath_and_response()
        if response:
            return filepath, url, response.headers, response.status_code
        return filepath, url, None, None

    def codelist(self, cl_code=None, headers={}, url=None, references=None):
        if not url:
            url = "%s/codelist/%s/%s" % (self.sdmx_url, self.agencyID, cl_code)
            if references:
                url = "%s?references=" % (url, references)
        _source, _final_url, _headers, _status_code = self.query_rest(url, headers=headers)
        return _source

class XMLSDMX_2_1(XMLSDMX):
    pass
    

def dataset_converter(xml, dataset_code, dsd_id=None):
    bson = {}
    
    if not dsd_id:    
        dsd_id = xml.get_dsd_id(dataset_code)
        
    bson["provider_name"] = xml.provider_name
    bson["dataset_code"] = dataset_code
    bson["dsd_id"] = dsd_id
    bson["codelists"] = {}

    dimension_keys = xml.dimension_keys_by_dsd[dsd_id]
    attribute_keys = xml.attribute_keys_by_dsd[dsd_id]

    dimensions = xml.dimensions_by_dsd[dsd_id]
    attributes = xml.attributes_by_dsd[dsd_id]
    
    bson["dimension_keys"] = dimension_keys
    bson["attribute_keys"] = attribute_keys
    bson["dimensions"] = {}
    bson["attributes"] = {}
    
    bson["name"] = xml.get_dataset_name(dsd_id)
    #bson["name"] = xml.get_dataset_name(dataset_code)
    #TODO: si not ?
    #bson["name"] = xml.dataflows.get(dsd_id)    
    
    for key in dimension_keys:
        if dimensions[key]["enum"]:
            bson["codelists"][key] = dict(dimensions[key]["enum"].items())            
        else:
            bson["codelists"][key] = {}
        bson["dimensions"][key] = bson["codelists"][key]

    for key in attribute_keys:
        if attributes[key]["enum"]:
            bson["codelists"][key] = dict(attributes[key]["enum"].items())
        else:
            bson["codelists"][key] = {}
        bson["attributes"][key] = bson["codelists"][key]
    
    bson["concepts"] = {}
    for key in bson["codelists"].keys():
        if key in xml.concepts:
            bson["concepts"][key] = xml.concepts[key]["name"]    
    
    return bson

def get_dimensions_from_dsd(xml_dsd=None, provider_name=None, dataset_code=None, dsd_id=None):
    
    dataset = dataset_converter(xml_dsd, dataset_code, dsd_id=dsd_id)
    
    dimension_keys = dataset["dimension_keys"]
    
    dimensions = {}
    for key in dimension_keys:
        if key in dataset["codelists"]:
            dimensions[key] = dataset["codelists"][key]
        else:
            msg = "dimension key[%s] is not in codelists - provider[%s] - dataset[%s]"
            logger.warning(msg % (key, provider_name, dataset_code))
            dimensions[key] = {}
    return dimension_keys, dimensions

class XMLStructureBase:
    
    TAGS_MAP = {
        'structure': 'structure'
    }
    
    def __init__(self, 
                 provider_name=None,
                 field_time_dimension="TIME_PERIOD",
                 sdmx_client=None):
        
        self.provider_name = provider_name
        self.field_time_dimension = field_time_dimension

        self.sdmx_client = sdmx_client

        self.nsmap = {}

        self.agencies = OrderedDict()
        self.categories = OrderedDict()
        self.categorisations = OrderedDict()
        self.categorisations_dataflows = OrderedDict()
        self.categorisations_categories = OrderedDict()
        self.dataflows = OrderedDict()
        self.datastructures = OrderedDict()
        
        self.codelists = OrderedDict()
        self.concepts = OrderedDict()

        self.dimension_keys_by_dsd = OrderedDict()
        self.attribute_keys_by_dsd = OrderedDict()
        self.dimensions_by_dsd = OrderedDict()
        self.attributes_by_dsd = OrderedDict()
        
        self.annotations = []
        self.last_update = None        
        
    def fixtag(self, ns, tag):
        ns = self.TAGS_MAP.get(ns, ns)
        return '{' + self.nsmap[ns] + '}' + tag

    def process_agency(self, element):
        raise NotImplementedError()

    def process_dsd_id(self, element):
        raise NotImplementedError()

    def process_dataset_name(self, element):
        raise NotImplementedError()
    
    def get_dsd_id(self, dataflow_key):
        return self.dataflows[dataflow_key].get('dsd_id')

    def get_dataset_name(self, dataflow_key):
        return self.dataflows[dataflow_key].get('name')
    
    def process_dataflow(self, element):
        raise NotImplementedError()

    def process_category(self, element):
        raise NotImplementedError()
    
    def process_categorisation(self, element):
        raise NotImplementedError()
    
    def process_concept(self, element):
        raise NotImplementedError()

    def process_codelist(self, element):
        raise NotImplementedError()

    def process_datastructure(self, element):
        raise NotImplementedError()

    def process_dimension(self, element):
        raise NotImplementedError()

    def process_attribute(self, element):
        raise NotImplementedError()
    
    def process_last_update(self, element):
        pass

    def _iter_parent_category(self, category):
        parents = []
        parents_keys = []
        if category["parent"]:
            parent_id = category["parent"]
            parent = self.categories[parent_id]
            parents_keys.append(parent_id)
            parents.append(parent)
            result = self._iter_parent_category(parent)
            parents_keys.extend(result[0])
            parents.extend(result[1])
        return parents_keys, parents
    
    def iter_parent_category(self, category):
        parents_keys, parents = self._iter_parent_category(category)
        parents_keys.reverse()
        parents.reverse()
        return parents_keys, parents

    def _iter_parent_category_id(self, category):
        """Return array of recursive parents id
        """
        parents_keys = []
        if category["parent"]:
            parent_id = category["parent"]
            parents_keys.append(parent_id)
            parents_keys.extend(self._iter_parent_category_id(self.categories[parent_id]))
        return parents_keys
    
    def iter_parent_category_id(self, category):
        parents_keys = self._iter_parent_category_id(category)
        parents_keys.reverse()
        return parents_keys

    def process(self, filepath):
        raise NotImplementedError()

class XMLStructure_1_0(XMLStructureBase):
    """Parsing SDMX 1.0
    """
    
    def get_name_element(self, element):
        return element[0].text

    def process_dsd_id(self, element):
        """
        <structure:KeyFamily id="CCOUT" agency="FRB">
        """
        _id = element.xpath('//message:Header/message:ID/text()', namespaces=self.nsmap)[0]
        short_id = element.attrib.get('id')
        return "%s-%s" % (_id, short_id)

    def process_dataset_name(self, element):
        """
        Normal: self.get_name_element(element)
        
        For FED: compose HEADER NAME + Name text in KeyFamily 
        
        <structure:KeyFamily id="CCOUT" agency="FRB">
            <structure:Name>Consumer Credit Outstanding</structure:Name>        
        """
        name = element.xpath('//message:Header/message:Name/text()', namespaces=self.nsmap)[0]
        return "%s - %s" % (name, self.get_name_element(element))
    
    def process_datastructure(self, element):

        """
        TODO:
        <structure:TimeDimension concept="TIME_PERIOD"/>
        <structure:PrimaryMeasure concept="OBS_VALUE"/>
        """

        ds_name = self.process_dataset_name(element)
        _id = self.process_dsd_id(element)
        
        if not _id in self.datastructures:
            self.datastructures[_id] = {
                "id": _id,
                "name": ds_name,
                'attrs': dict(element.attrib),
            }

        if not _id in self.dataflows:
            self.dataflows[_id] = {
                "id": _id,
                "name": ds_name,
                'attrs': dict(element.attrib),
                "dsd_id": _id,
            }
            
        for child in element.xpath(".//*[local-name()='Dimension']"):
            self.process_dimension(child, _id)
            
        for child in element.xpath(".//*[local-name()='Attribute']"):
            self.process_attribute(child, _id)
            
        annotations = element.xpath(".//*[local-name()='Annotation']")
        self.annotations = annotations
        
        element.clear()
    
    def process_concept(self, element):

        _id = element.attrib.get('id')
        
        if not _id in self.concepts:
            self.concepts[_id] = {
                'id': _id,
                'name': self.get_name_element(element),
                "attrs": dict(element.attrib)
            }

        element.clear()
        
    def process_codelist(self, element):

        _id = element.get('id')
        
        if not _id in self.codelists:
            self.codelists[_id] = {
                "id": _id,
                'name': self.get_name_element(element), 
                "enum": OrderedDict(),
                "attrs": dict(element.attrib)
            }
        
        for child in element.getchildren():
            if child.tag == self.fixtag("structure", "Code"):
                key = child.get("value")
                if not key in self.codelists[_id]["enum"]:
                    #<structure:Description /> 
                    self.codelists[_id]["enum"][key] = child[0].text
        
        element.clear()
        
    def get_concept_ref_id(self, element): 
        return element.attrib.get('concept')   

    def fixe_codelist_fed(self, codelist):
        """
        Error FED - dataset CP
        """
        if codelist and codelist == "CL_UNT":
            logger.warning("fixe codelist[%s] for provider[%s]" % (codelist,
                                                                   self.provider_name))
            return "CL_UNIT"
        return codelist
        
    def process_dimension(self, element, dsd_id):
        
        if not dsd_id in self.dimensions_by_dsd:
            self.dimensions_by_dsd[dsd_id] = OrderedDict()

        if not dsd_id in self.dimension_keys_by_dsd:
            self.dimension_keys_by_dsd[dsd_id] = []
        
        _id = self.get_concept_ref_id(element)
        
        if not _id in self.dimensions_by_dsd[dsd_id]:
            codelist = element.attrib.get('codelist')
            #FIXME: Specific FED
            if self.provider_name == "FED":
                codelist = self.fixe_codelist_fed(codelist)
            
            if codelist:
                name = self.codelists[codelist]["name"]
                values = self.codelists[codelist]["enum"]
            else:
                name = _id
                values = {}
                        
            self.dimensions_by_dsd[dsd_id][_id] = {
                "id": _id,
                "name": name,                                
                "enum": values,
                "attrs": dict(element.attrib)
            }
            self.dimension_keys_by_dsd[dsd_id].append(_id)
                
        element.clear()

    def process_attribute(self, element, dsd_id):

        if not dsd_id in self.attributes_by_dsd:
            self.attributes_by_dsd[dsd_id] = OrderedDict()

        if not dsd_id in self.attribute_keys_by_dsd:
            self.attribute_keys_by_dsd[dsd_id] = []

        _id = self.get_concept_ref_id(element)
        
        if not _id in self.attributes_by_dsd[dsd_id]:
            codelist = element.attrib.get('codelist')
            codelist = self.fixe_codelist_fed(codelist)
            
            if codelist:
                name = self.codelists[codelist]["name"]
                values = self.codelists[codelist]["enum"]
            else:
                name = _id
                values = {}
                            
            self.attributes_by_dsd[dsd_id][_id] = {
                "id": _id,
                "name": name,                                
                "enum": values,
                "attrs": dict(element.attrib)
            }
            self.attribute_keys_by_dsd[dsd_id].append(_id)
                
        element.clear()    
    
    def process(self, filepath):
        
        tree_iterator = etree.iterparse(filepath, events=['end', 'start-ns'])
        
        self.nsmap = get_nsmap(tree_iterator)
        
        for event, element in tree_iterator:
            if event == 'end':
                if element.tag == self.fixtag('structure', 'CodeList'):
                    self.process_codelist(element)
                elif element.tag == self.fixtag('structure', 'Concept'):
                    self.process_concept(element)
                elif element.tag == self.fixtag("structure", "KeyFamily"):
                    self.process_datastructure(element)

class XMLStructure_2_0(XMLStructure_1_0):
    """Parsing SDMX 2.0
    """
    
    NSMAP = {'common': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/common',
             'compact': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/compact',
             'cross': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/cross',
             'generic': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic',
             'query': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/query',
             'structure': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure',
             'utility': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/utility',
             'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
             'message': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message'}

    def get_name_element(self, element):
        return xml_get_name(element)

    def process_dsd_id(self, element):
        return element.attrib.get('id').replace("_DSD", "").strip()

    def process_dataset_name(self, element):
        return self.get_name_element(element).replace("_DSD", "").strip()

    def get_concept_ref_id(self, element): 
        return element.attrib.get('conceptRef')   

    def process(self, filepath):
        
        tree_iterator = etree.iterparse(filepath, events=['end', 'start-ns'])

        if not self.NSMAP:
            self.nsmap = get_nsmap(tree_iterator)
        else:
            self.nsmap = self.NSMAP

        for event, element in tree_iterator:
            if event == 'end':
                
                if element.tag == self.fixtag('structure', 'CodeList'):
                    self.process_codelist(element)
                elif element.tag == self.fixtag('structure', 'Concept'):
                    self.process_concept(element)
                elif element.tag == self.fixtag("structure", "KeyFamily"):
                    self.process_datastructure(element)
                    self.process_last_update(element)
                    element.clear()
                    
    def process_last_update(self, element):
        if self.annotations:
            try:
                date_str = self.annotations[0].xpath(".//*[local-name()='AnnotationText']/text()")
                if date_str:
                    #'01/27/2016'
                    self.last_update = datetime.strptime(date_str[0], "%m/%d/%Y")
            except:
                pass
    
class XMLStructure_2_1(XMLStructure_2_0):
    """Parsing SDMX 2.1 structure
    """
    
    TAGS_MAP = {
        'structure': 'str'
    }
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        if not self.sdmx_client:
            self.sdmx_client = XMLSDMX_2_1(sdmx_url=SDMX_PROVIDERS[self.provider_name]["url"], 
                                           agencyID=self.provider_name)
    
    def get_codelist(self, cl_code):
        """If not cl_code self.codelists, load with remote SDMX
        """
        if not cl_code in self.codelists or not self.codelists.get(cl_code):
            logger.warning("codelist not found [%s] for provider[%s]" % (cl_code, self.provider_name))
            
            try:
                source = self.sdmx_client.codelist(cl_code=cl_code)
                tree = etree.parse(source)
                #TODO: namespace ?            
                namespaces = tree.getroot().nsmap
                #TODO: attention str://
                codelists = tree.xpath('.//str:Codelist', namespaces=namespaces)
                for code_list in codelists:
                    self.process_codelist(code_list)
            except Exception as err:
                msg = "sdmx error for loading codelist[%s] - provider[%s] - error[%s]" % (cl_code, self.provider_name, str(err))
                logger.critical(msg)
                raise Exception(msg)
        
        return self.codelists[cl_code]    

    def process_agency(self, element):
        """
        <str:Agency id="SDMX" urn="urn:sdmx:org.sdmx.infomodel.base.Agency=SDMX">
            <com:Name xml:lang="en">SDMX</com:Name>
        </str:Agency>
        """
        _id = element.attrib.get('id')
        self.agencies[_id] = {
            'id': _id,
            'name': xml_get_name(element),
            'attrs': dict(element.attrib)
        }
        element.clear()

    def process_dataflow(self, element):

        if element.getparent().tag != self.fixtag("structure", "Dataflows"):
            return

        _id = element.attrib.get('id')
        
        if not _id in self.dataflows:
            dataflow = element.xpath(".//Ref[@class='DataStructure']")[0]
            self.dataflows[_id] = {
                "id": _id,
                "name": xml_get_name(element),
                'attrs': dict(element.attrib),
                "dsd_id": dataflow.attrib.get('id'),
            }
        
        element.clear()
    
    def process_category(self, element):

        _id = element.attrib.get('id')

        if not _id in self.categories:
            self.categories[_id] = {
                'id': _id,
                'name': xml_get_name(element),
                'attrs': dict(element.attrib),
                'parent': None
            }
        for child in element.iterchildren(self.fixtag("str", "Category")):
            child_id = child.attrib.get('id')
            self.categories[child_id] = {
                'id': child_id,
                'name': xml_get_name(child),
                'attrs': dict(child.attrib),
                'parent': _id
            }
            child.clear()
    
    def process_categorisation(self, element):
        _id = element.attrib.get('id')
        
        dataflow = element.xpath(".//Ref[@class='Dataflow']")[0]
        category = element.xpath(".//Ref[@class='Category']")[0]
        
        dataflow = dict(dataflow.attrib)
        category = dict(category.attrib)
        
        self.categorisations[_id] = {
            "id": _id,
            "name": xml_get_name(element),
            "dataflow": dataflow,
            "category": category
        }
        
        dataflow_id = dataflow["id"]

        if not dataflow["id"] in self.categorisations_dataflows:
            self.categorisations_dataflows[dataflow_id] = []
        if not _id in self.categorisations_dataflows[dataflow_id]:
            self.categorisations_dataflows[dataflow_id].append(_id)
        
        category_id = category["id"]
        if not category["id"] in self.categorisations_categories:
            self.categorisations_categories[category_id] = []
        if not _id in self.categorisations_categories[category_id]:
            self.categorisations_categories[category_id].append(_id)
        
        element.clear()

    def process_codelist(self, element):

        _id = element.get('id')

        if not _id in self.codelists:
            self.codelists[_id] = {
                "id": _id,
                'name': self.get_name_element(element), 
                "enum": OrderedDict(),
                "attrs": dict(element.attrib)
            }
            
            for child in element.getchildren():
                if child.tag == self.fixtag("structure", "Code"):
                    key = child.get("id")
                    self.codelists[_id]["enum"][key] = self.get_name_element(child)
        
        element.clear()
        
    def process_datastructure(self, element):
        """
        TODO:
        <structure:TimeDimension concept="TIME_PERIOD"/>
        <structure:PrimaryMeasure concept="OBS_VALUE"/>        
        """

        _id = element.attrib.get('id')

        if not _id in self.datastructures:
            
            self.datastructures[_id] = {
                "id": _id,
                "name": xml_get_name(element),
                'attrs': dict(element.attrib),
            }
            
        if not _id in self.dataflows:
            
            self.dataflows[_id] = {
                "id": _id,
                "name": xml_get_name(element),
                'attrs': dict(element.attrib),
                "dsd_id": _id,
            }
            
        for child in element.xpath(".//*[local-name()='Dimension']"):
            self.process_dimension(child, _id)
            
        for child in element.xpath(".//*[local-name()='Attribute']"):
            self.process_attribute(child, _id)
        
        element.clear()
        
    def process_dimension(self, element, dsd_id):
        
        if element.getparent().tag != self.fixtag("structure", "DimensionList"):
            return

        if not dsd_id in self.dimensions_by_dsd:
            self.dimensions_by_dsd[dsd_id] = OrderedDict()

        if not dsd_id in self.dimension_keys_by_dsd:
            self.dimension_keys_by_dsd[dsd_id] = []
        
        concept = element.xpath(".//Ref[@class='Concept']")[0]
        _id = concept.attrib.get('id')
        
        codelist = element.xpath(".//Ref[@class='Codelist']")

        if codelist:
            codelist = codelist[0].attrib.get('id')
            codelist = self.get_codelist(codelist)
            values = codelist["enum"]
            name = codelist["name"]
        else:
            values = {}
            name = self.concepts[_id]

        self.dimensions_by_dsd[dsd_id][_id] = {
            "id": _id,
            "name": name,                                
            "enum": values,
            "attrs": dict(element.attrib)
        }
        self.dimension_keys_by_dsd[dsd_id].append(_id)
        
        element.clear()
        
    def process_attribute(self, element, dsd_id):

        if element.getparent().tag != self.fixtag("structure", "AttributeList"):
            return
        
        if not dsd_id in self.attributes_by_dsd:
            self.attributes_by_dsd[dsd_id] = OrderedDict()

        if not dsd_id in self.attribute_keys_by_dsd:
            self.attribute_keys_by_dsd[dsd_id] = []

        concept = element.xpath(".//Ref[@class='Concept']")[0]
        _id = concept.attrib.get('id')
        
        codelist = element.xpath(".//Ref[@class='Codelist']")
        if codelist:
            codelist = codelist[0].attrib.get('id')
            codelist = self.get_codelist(codelist)
            values = codelist["enum"]
            name = codelist["name"]
        else:
            values = {}
            name = self.concepts[_id]
            
        self.attributes_by_dsd[dsd_id][_id] = {
            "id": _id,
            "name": name,                                
            "enum": values,
            "attrs": dict(element.attrib)
        }
        self.attribute_keys_by_dsd[dsd_id].append(_id)
        
        element.clear()    
        
    def process(self, filepath):
        
        tree_iterator = etree.iterparse(filepath, events=['end', 'start-ns'])
        
        self.nsmap = get_nsmap(tree_iterator)
        
        for event, element in tree_iterator:
            if event == 'end':
                
                #TODO: OrganisationSchemes

                if element.tag == self.fixtag("structure", "Agency"):
                    self.process_agency(element)
                
                elif element.tag == self.fixtag("structure", "Category"):                    
                    self.process_category(element)

                elif element.tag == self.fixtag("structure", "Categorisation"):
                    self.process_categorisation(element)

                elif element.tag == self.fixtag("structure", "Dataflow"):
                    self.process_dataflow(element)

                elif element.tag == self.fixtag("structure", "Codelist"):
                    self.process_codelist(element)

                elif element.tag == self.fixtag("structure", "Concept"):
                    self.process_concept(element)
                    
                elif element.tag == self.fixtag("structure", "DataStructure"):
                    self.process_datastructure(element)
                                
            #element.clear()

@timeit("xml_utils.series_converter_v2", stats_only=True)
def series_converter_v2(bson, xml):
    
    bson.pop("series_keys", None)
    
    bson["attributes"] = dict(bson.pop("series_attributes", {}))
    bson["values"] = bson.pop("observations")
    
    last_update = bson.pop("last_update")
    if not last_update:
        last_update = clean_datetime()
    bson["last_update"] = last_update
        
    return bson

SERIES_CONVERTERS = {
    "dlstats_v2": series_converter_v2,
}

class XMLDataBase:
    
    NS_TAG_DATA = None
    PROVIDER_NAME = None
    XMLStructureKlass = None
    
    def __init__(self, 
                 provider_name=None,
                 dataset_code=None,
                 dsd_id=None,
                 ns_tag_data=None,
                 field_frequency="FREQ", 
                 field_obs_time_period="TIME_PERIOD",
                 field_obs_value="OBS_VALUE",
                 dimension_keys=None,
                 dsd_filepath=None, 
                 xml_dsd=None,
                 frequencies_supported=None, 
                 frequencies_rejected=None,
                 series_converter="dlstats_v2"):
        
        self.provider_name = provider_name or self.PROVIDER_NAME
        self.dataset_code = dataset_code
        self.dsd_id = dsd_id
        self.dimension_keys = dimension_keys or []
        self.dsd_filepath = dsd_filepath
        self.xml_dsd = xml_dsd
        self.dimensions = {}
        
        if not self.xml_dsd and self.XMLStructureKlass and self.dsd_filepath:
            self.xml_dsd = self.XMLStructureKlass(provider_name=self.provider_name)
            self.xml_dsd.process(dsd_filepath)
            self.dsd_id = self.xml_dsd.get_dsd_id(self.dataset_code)
        
        if self.xml_dsd:
            
            if not self.dsd_id:
                raise Exception("required dsd_id parameter")
            
            self.dimension_keys, self.dimensions = get_dimensions_from_dsd(xml_dsd=self.xml_dsd, 
                                                                           provider_name=self.provider_name, 
                                                                           dataset_code=self.dataset_code,
                                                                           dsd_id=self.dsd_id)

        if not self.dimension_keys:
            raise Exception("required dimension_keys")
        
        if not self.dimensions:
            raise Exception("required dimensions")
        
        self.frequencies_supported = frequencies_supported
        self.frequencies_rejected = frequencies_rejected

        self.field_frequency = field_frequency
        self.field_obs_time_period = field_obs_time_period
        self.field_obs_value = field_obs_value

        self.series_converter = SERIES_CONVERTERS[series_converter]

        self.nsmap = {}
        self.tree_iterator = None
        
        self._ns_tag_data = ns_tag_data
                
    @property    
    def ns_tag_data(self):
        if self._ns_tag_data:
            return self._ns_tag_data
        else:
            return self.NS_TAG_DATA
        
    def _get_nsmap(self, tree_iterator):
        return get_nsmap(tree_iterator)

    @timeit("xml_utils.XMLDatabase._load_data", stats_only=True)        
    def _load_data(self, filepath):
        self.tree_iterator = etree.iterparse(filepath, events=['end', 'start-ns'])
        self.nsmap = self._get_nsmap(self.tree_iterator)

    @timeit("xml_utils.XMLDatabase.fixtag", stats_only=True)
    def fixtag(self, ns, tag):
        if not ns in self.nsmap:
            msg = "Namespace not found[%s] - tag[%s] - provider[%s] - nsmap[%s]"
            raise Exception(msg %(ns, tag, self.provider_name, self.nsmap))
        return '{' + self.nsmap[ns] + '}' + tag

    @timeit("xml_utils.XMLDatabase.is_series_tag", stats_only=True)
    def is_series_tag(self, element):
        return element.tag == self.fixtag(self.ns_tag_data, 'Series')

    @timeit("xml_utils.XMLDatabase.process", stats_only=True)
    def process(self, filepath):
        
        self._load_data(filepath)
        
        for event, element in self.tree_iterator:
            if event == 'end':
                
                if self.is_series_tag(element):
                    try:
                        yield self.one_series(element), None
                    except errors.RejectFrequency as err:
                        yield None, err
                    except errors.RejectEmptySeries as err:
                        yield None, err
                    finally:
                        element.clear()

    def get_dimensions(self, series):
        if self.dimension_keys:
            return OrderedDict([(k, v) for k, v in series.attrib.items() if k in self.dimension_keys])
        else:
            return OrderedDict([(k, v) for k, v in series.attrib.items()])

    def get_attributes(self, series):
        if self.dimension_keys:
            return OrderedDict([(k, v) for k, v in series.attrib.items() if not k in self.dimension_keys])
        else:
            return {}

    def get_name(self, series, dimensions, attributes):
        values = []
        if self.dimensions:
            for key in self.dimension_keys:
                search_value = dimensions[key]
                if key in self.dimensions and search_value in self.dimensions[key]: 
                    values.append(self.dimensions[key][search_value])
                else:
                    values.append(search_value)
        else:
            values = [dimensions[key] for key in self.dimension_keys]
        
        return " - ".join(values)

    def get_key(self, series, dimensions, attributes):
        return ".".join([dimensions[key] for key in self.dimension_keys])

    def fixe_frequency(self, frequency, series, dimensions, attributes):
        return frequency

    def valid_frequency(self, frequency, series, dimensions):
        msg = {"provider_name": self.provider_name, 
               "dataset_code": self.dataset_code,
               "frequency": frequency}
        
        if self.frequencies_supported and not frequency in self.frequencies_supported:
            raise errors.RejectFrequency(**msg)
        
        if self.frequencies_rejected and frequency in self.frequencies_rejected:
            raise errors.RejectFrequency(**msg)
        
        return True
    
    @timeit("xml_utils.XMLDatabase.get_frequency", stats_only=True)
    def get_frequency(self, series, dimensions, attributes):
        frequency = self.search_frequency(series, dimensions, attributes)
        frequency = self.fixe_frequency(frequency, series, dimensions, attributes)
        '''Valid after transform in fixe_frequency'''
        self.valid_frequency(frequency, series, dimensions)
        return frequency

    @timeit("xml_utils.XMLDatabase.search_frequency", stats_only=True)
    def search_frequency(self, series, dimensions, attributes):
        if self.field_frequency in dimensions:
            return dimensions[self.field_frequency]
        elif attributes and self.field_frequency in attributes:
            return attributes[self.field_frequency]
        return series.attrib[self.field_frequency]

    def get_observations(self, series, frequency):
        raise NotImplementedError()

    def get_last_update(self, series, dimensions, attributes, bson=None):
        return None

    def start_date(self, series, frequency, observations=[], bson=None):
        _date = observations[0]["period"]
        return get_ordinal_from_period(_date, freq=frequency)

    def end_date(self, series, frequency, observations=[], bson=None):
        _date = observations[-1]["period"]
        return get_ordinal_from_period(_date, freq=frequency)

    @timeit("xml_utils.XMLDatabase.finalize_bson", stats_only=True)
    def finalize_bson(self, bson):
        return self.series_converter(bson, self)

    def build_series(self, series):
        raise NotImplementedError()

    @timeit("xml_utils.XMLDatabase.one_series", stats_only=True)
    def one_series(self, series):
        bson = self.build_series(series)
        return self.finalize_bson(bson)

class XMLDataMixIn:

    def get_observations(self, series, frequency):
        """
        element: <data:Series>
        
        <data:Series FREQ="Q" s_adj="NSA" unit="I10" na_item="NULC_HW" geo="AT" TIME_FORMAT="P3M">
            <data:Obs TIME_PERIOD="1996-Q1" OBS_VALUE="89.4" />
            <data:Obs TIME_PERIOD="1996-Q2" OBS_VALUE="89.9" />
        </data:Series>
        
        """
        observations = deque()
        for obs in series.iterchildren():

            item = {"period": None, "value": None, "attributes": {}}
        
            localname = etree.QName(obs.tag).localname
                
            #if obs.tag == self.fixtag(self.ns_tag_data, 'Obs'):
            if localname == "Obs":
                item["period"] = obs.attrib["TIME_PERIOD"]

                #TODO: value manquante
                item["value"] = obs.attrib.get("OBS_VALUE", "")
                
                for key, value in obs.attrib.items():
                    if not key in ['TIME_PERIOD', 'OBS_VALUE']:
                        item["attributes"][key] = value
                
                observations.append(item)
            
                obs.clear()

        return list(observations)
    
    def build_series(self, series):
        dimensions = self.get_dimensions(series)
        attributes = self.get_attributes(series)
        frequency = self.get_frequency(series, dimensions, attributes)
        observations = self.get_observations(series, frequency)
        
        if not observations:
            msg = {"provider_name": self.provider_name, 
                   "dataset_code": self.dataset_code}            
            raise errors.RejectEmptySeries(**msg)                
        
        bson = {
            "provider_name": self.provider_name,
            "dataset_code": self.dataset_code,
            "dimensions": dimensions,
            "frequency": frequency,
            "name": self.get_name(series, dimensions, attributes),
            "key": self.get_key(series, dimensions, attributes), 
            "observations": observations,
            "series_keys": {},
            "series_attributes": attributes,
        }
        bson["start_date"] = self.start_date(series, frequency, observations, bson)
        bson["end_date"] = self.end_date(series, frequency, observations, bson)
        bson["last_update"] = self.get_last_update(series, dimensions, attributes, bson)
        
        return bson

class XMLData_1_0(XMLDataMixIn, XMLDataBase):
    """SDMX 1.0
    http://www.SDMX.org/resources/SDMXML/schemas/v1_0/message
    """

    NS_TAG_DATA = None
    XMLStructureKlass = XMLStructure_1_0

    def is_series_tag(self, element):
        localname = etree.QName(element.tag).localname
        return localname == 'Series'
    
class XMLData_1_0_FED(XMLData_1_0):
    """
    TODO: si je stocke FREQ: 129 dans series, il faut retrouver 129 dans les codeslists["FREQ"]
    """

    PROVIDER_NAME = "FED"
    NS_TAG_DATA = "frb"
     
    _frequency_map = {
        "8": "D",
        #"9": "", #Business day
        "16": "W-SUN", # Weekly (Sunday)
        "17": "W-MON", # Weekly (Monday)
        "18": "W-TUE", # Weekly (Tuesday)    
        "19": "W-WED", # Weekly (Wednesday)
        "20": "W-THU", # Weekly (Thursday)
        "21": "W-FRI", # Weekly (Friday) 
        "22": "W-SAT", # Weekly (Saturday) 
        #"67": "", # Bi-Weekly (AWednesday)
        "129": "M",
        "162": "Q",
        "203": "A", 
    }
    
    MAP_DSD_ID = {
        "OTHER": "Z.1",
        "Z1": "Z.1",
    }    
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.frequencies_supported:
            self.frequencies_supported = list(self._frequency_map.values())

    def _get_nsmap(self, iterator):
        return {'common': 'http://www.SDMX.org/resources/SDMXML/schemas/v1_0/common',
                'frb': 'http://www.federalreserve.gov/structure/compact/common',
                'message': 'http://www.SDMX.org/resources/SDMXML/schemas/v1_0/message',
                'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}
        
    def process(self, filepath):
        
        self._load_data(filepath)
        
        for event, element in self.tree_iterator:
            
            if event == 'end':

                if element.tag == self.fixtag("frb", "DataSet"):
                    
                    dataset = element
                
                    _id = element.xpath('//message:Header/message:ID/text()', 
                                        namespaces=self.nsmap)[0]

                    if _id in self.MAP_DSD_ID:
                        _id = self.MAP_DSD_ID[_id]

                    short_id = dataset.attrib.get('id')
                    long_id = "%s-%s" % (_id, short_id)
                    
                    if not long_id == self.dsd_id:
                        dataset.clear()
                        continue
                    
                    for child in dataset.getchildren():
                        if self.is_series_tag(child):
                            try:
                                yield self.one_series(child), None
                            except errors.RejectFrequency as err:
                                yield (None, err)
                            except errors.RejectEmptySeries as err:
                                yield (None, err)
                            finally:
                                child.clear()
                        else:
                            child.clear()
                    
                    dataset.clear()
    
    def get_name(self, series, dimensions, attributes):
        try:
            annotations = series.xpath(".//frb:Annotations/common:Annotation/common:AnnotationText/text()", namespaces=self.nsmap)
            if annotations:
                if len(annotations) == 2:
                    return annotations[1]
                return annotations[0]
        except Exception as err:
            logger.warning("fed annotations error[%s]" % str(err))

        return super().get_name(series, dimensions, attributes)
        
    def get_key(self, series, dimensions, attributes):
        if 'SERIES_NAME' in attributes:
            return attributes['SERIES_NAME']
        elif 'SERIES_NAME' in dimensions:
            return dimensions['SERIES_NAME']
        return series.attrib['SERIES_NAME']

    def fixe_frequency(self, frequency, series, dimensions, attributes):
        if frequency in self._frequency_map:
            return self._frequency_map[frequency]
        return frequency

class XMLCompactData_2_0(XMLDataMixIn, XMLDataBase):

    NS_TAG_DATA = "data"
    XMLStructureKlass = XMLStructure_2_0

class XMLCompactData_2_0_DESTATIS(XMLCompactData_2_0):

    NS_TAG_DATA = "ns1"
    PROVIDER_NAME = "DESTATIS"

class XMLCompactData_2_0_EUROSTAT(XMLCompactData_2_0):

    PROVIDER_NAME = "EUROSTAT"    

    def start_date(self, series, frequency, observations=[], bson=None):
        time_format = series.attrib.get('TIME_FORMAT')
        if not time_format or not time_format in SPECIAL_DATE_FORMATS:
            return super().start_date(series, frequency, observations=observations, bson=bson)

        period = observations[0]["period"]
        (date_string, freq) = parse_special_date(period, time_format, self.dataset_code)
        return get_ordinal_from_period(date_string, freq=freq)
        
    def end_date(self, series, frequency, observations=[], bson=None):
        time_format = series.attrib.get('TIME_FORMAT')
        if not time_format or not time_format in SPECIAL_DATE_FORMATS:
            return super().start_date(series, frequency, observations=observations, bson=bson)

        period = observations[-1]["period"]        
        (date_string, freq) = parse_special_date(period, time_format, self.dataset_code)
        return get_ordinal_from_period(date_string, freq=freq)

class XMLGenericData_2_0(XMLDataBase):
    """SDMX 2.0 application/vnd.sdmx.genericdata+xml;version=2.1
    """

    NS_TAG_DATA = "common"
    XMLStructureKlass = XMLStructure_2_0

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.field_frequency = "FREQUENCY"                

    def is_series_tag(self, element):
        return etree.QName(element.tag).localname == 'Series'

    def _get_values(self, element):
        """
        <SeriesKey>
            <Value concept="LOCATION" value="AUT"/>
            <Value concept="SUBJECT" value="PRMNTO01"/>
        </SeriesKey>        
        """
        d = OrderedDict()
        for value in element.iterchildren():
            key = value.attrib["concept"]
            value =value.attrib["value"]
            d[key] = value
        return d
    
    def get_observations(self, series, frequency):
        
        observations = deque()
        
        for element in series.xpath("./*[local-name()='Obs']"):

            item = {"period": None, "value": None, "attributes": {}}
            
            for child in element.getchildren():
                
                if etree.QName(child.tag).localname == "Time":
                    item["period"] = child.text
                
                elif etree.QName(child.tag).localname == 'ObsValue':
                    #TODO: valeur manquante
                    item["value"] = child.attrib["value"]
                
                #TODO:
                elif etree.QName(child.tag).localname == 'Attributes':
                    """
                    <Attributes><Value concept="OBS_STATUS" value="M"/></Attributes>                    
                    AUS.LCEATT02.ST.Q                
                    """ 
                    for key, value in self._get_values(child).items():
                        item["attributes"][key] = value
                
                child.clear()
            
            observations.append(item)
            element.clear()
            
        return list(observations)

    def get_dimensions(self, series):
        _dimensions = series.xpath("./*[local-name()='SeriesKey']")[0]
        
        dimensions = self._get_values(_dimensions)
        if self.dimension_keys:
            return OrderedDict([(k, v) for k, v in dimensions.items() if k in self.dimension_keys])
        else:
            return OrderedDict([(k, v) for k, v in dimensions.items()])

    def get_attributes(self, series):
        _attributes = series.xpath("./*[local-name()='Attributes']")[0]

        attributes = self._get_values(_attributes)        
        if self.dimension_keys:
            return OrderedDict([(k, v) for k, v in attributes.items() if not k in self.dimension_keys])
        else:
            return {}

    def build_series(self, series):
        dimensions = self.get_dimensions(series)
        attributes = self.get_attributes(series)
        frequency = self.get_frequency(series, dimensions, attributes)
        observations = self.get_observations(series, frequency)

        if not observations:
            msg = {"provider_name": self.provider_name, 
                   "dataset_code": self.dataset_code}            
            raise errors.RejectEmptySeries(**msg)                
        
        bson = {
            "provider_name": self.provider_name,
            "dataset_code": self.dataset_code,
            "dimensions": dimensions,
            "frequency": frequency,
            "name": self.get_name(series, dimensions, attributes),
            "key": self.get_key(series, dimensions, attributes), 
            "observations": observations,
            "series_keys": dimensions,
            "series_attributes": attributes,
        }

        bson["start_date"] = self.start_date(series, frequency, observations, bson)
        bson["end_date"] = self.end_date(series, frequency, observations, bson)
        bson["last_update"] = self.get_last_update(series, dimensions, attributes, bson)
        
        return bson
    
class XMLGenericData_2_0_OECD(XMLGenericData_2_0):
    
    PROVIDER_NAME = "OECD"    
    
class XMLGenericData_2_1(XMLDataBase):
    """SDMX 2.1 application/vnd.sdmx.genericdata+xml;version=2.1
    """

    NS_TAG_DATA = "generic"
    XMLStructureKlass = XMLStructure_2_1
    
    def _get_values(self, element):
        d = OrderedDict()
        for value in element.iterchildren():
            key = value.attrib["id"]
            value =value.attrib["value"]
            d[key] = value
        return d
    
    def get_observations(self, series, frequency):
        
        observations = deque()
        
        for element in series.xpath("child::%s:Obs" % self.ns_tag_data, 
                                    namespaces=self.nsmap):

            item = {"period": None, "value": None, "attributes": {}}
            for child in element.getchildren():
                
                if child.tag == self.fixtag(self.ns_tag_data, 'ObsDimension'):
                    item["period"] = child.attrib["value"]
                
                elif child.tag == self.fixtag(self.ns_tag_data, 'ObsValue'):
                    #TODO: valeur manquante
                    item["value"] = child.attrib["value"]
                
                elif child.tag == self.fixtag(self.ns_tag_data, 'Attributes'):
                    for key, value in self._get_values(child).items():
                        item["attributes"][key] = value
                
                child.clear()
            
            observations.append(item)
            element.clear()
            
        return list(observations)

    def get_dimensions(self, series):
        _dimensions = series.xpath("child::%s:SeriesKey" % self.ns_tag_data, 
                                    namespaces=self.nsmap)[0]
        dimensions = self._get_values(_dimensions)        
        if self.dimension_keys:
            return OrderedDict([(k, v) for k, v in dimensions.items() if k in self.dimension_keys])
        else:
            return OrderedDict([(k, v) for k, v in dimensions.items()])

    def get_attributes(self, series):
        _attributes = series.xpath("child::%s:Attributes" % self.ns_tag_data, 
                                  namespaces=self.nsmap)[0]
        attributes = self._get_values(_attributes)        
        if self.dimension_keys:
            return OrderedDict([(k, v) for k, v in attributes.items() if not k in self.dimension_keys])
        else:
            return {}

    def build_series(self, series):
        dimensions = self.get_dimensions(series)
        attributes = self.get_attributes(series)
        frequency = self.get_frequency(series, dimensions, attributes)
        observations = self.get_observations(series, frequency)

        if not observations:
            msg = {"provider_name": self.provider_name, 
                   "dataset_code": self.dataset_code}            
            raise errors.RejectEmptySeries(**msg)                
        
        bson = {
            "provider_name": self.provider_name,
            "dataset_code": self.dataset_code,
            "dimensions": dimensions,
            "frequency": frequency,
            "name": self.get_name(series, dimensions, attributes),
            "key": self.get_key(series, dimensions, attributes), 
            "observations": observations,
            "series_keys": dimensions,
            "series_attributes": attributes,
        }
        bson["start_date"] = self.start_date(series, frequency, observations, bson)
        bson["end_date"] = self.end_date(series, frequency, observations, bson)
        bson["last_update"] = self.get_last_update(series, dimensions, attributes, bson)
        
        return bson

class DataMixIn_ECB:
    """Common class for ECB datas
    """

    """
    TODO: http://sdw-wsrest.ecb.int/service/contentconstraint/ECB/EXR2_CONSTRAINTS
    """
    
    PROVIDER_NAME = "ECB"
    
    _frequencies_supported = ["A", "M", "Q", "W", "D", "S"]
    
    def get_name(self, series, dimensions, attributes):
        if "TITLE_COMPL" in attributes:
            return attributes["TITLE_COMPL"]
        else:
            logger.warning("Not TITLE_COMPL field for provider[%s] - dataset[%s]" % (self.provider_name,
                                                                                     self.dataset_code))
        if attributes.get("TITLE"):
            return attributes.get("TITLE")
        
        return super().get_name(series, dimensions, attributes)
    
    @timeit("xml_utils.DataMixIn_ECB.fixe_frequency", stats_only=True)
    def fixe_frequency(self, frequency, series, dimensions, attributes):
        if frequency == "H": #Half Yearly (semestriel)
            frequency = "S"
            #logger.warning("Replace H frequency by S - dataset[%s]" % self.dataset_code)
        return frequency
    

class DataMixIn_INSEE:
    """Common class for INSEE datas
    """
    
    PROVIDER_NAME = "INSEE"

    #_frequencies_rejected = ["S", "B", "I"]
    _frequencies_supported = ["A", "M", "Q", "W", "D"]

    """    
    def _dim_attrs(self,  dimensions, attributes):
        attributes_keys = "|".join(list(attributes.keys())) if attributes else ""
        dimensions_keys = "|".join(list(dimensions.keys())) if dimensions else ""
        return "DIMENSIONS[%s] - ATTRIBUTES[%s]" % (attributes_keys, dimensions_keys)
    """
    
    """
    USE title English by concat dimension + add title ?    
    def get_name(self, series, dimensions, attributes):
        if "TITLE" in attributes:            
            return attributes["TITLE"]
        elif "TITLE" in dimensions:
            return dimensions["TITLE"]

        return super().get_name(series, dimensions, attributes)
    """
    
    def get_key(self, series, dimensions, attributes):
        return attributes["IDBANK"]

    @timeit("xml_utils.DataMixIn_INSEE.fixe_frequency", stats_only=True)
    def fixe_frequency(self, frequency, series, dimensions, attributes):
        if frequency == "T":
            #TODO: T equal Trimestrial for INSEE
            frequency = "Q"
            #idbank = self.get_key(series, dimensions, attributes)
            #logger.warning("Replace T frequency by Q - dataset[%s] - idbank[%s]" % (self.dataset_code, idbank))

        return frequency

    @timeit("xml_utils.DataMixIn_INSEE.get_last_update", stats_only=True)
    def get_last_update(self, series, dimensions, attributes, bson=None):
        #TODO: normalize
        return datetime.strptime(attributes["LAST_UPDATE"], "%Y-%m-%d")

    @timeit("xml_utils.DataMixIn_INSEE.finalize_bson", stats_only=True)
    def finalize_bson(self, bson):
        """Insee Fixe for reverse dates and values
        """
        bson = super().finalize_bson(bson)
        
        start_date = bson["end_date"]
        end_date = bson["start_date"]
        bson["end_date"] = end_date
        bson["start_date"] = start_date
        bson["values"].reverse()

        return bson

class XMLGenericData_2_1_ECB(DataMixIn_ECB, XMLGenericData_2_1):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.frequencies_supported:
            self.frequencies_supported = self._frequencies_supported

class XMLGenericData_2_1_INSEE(DataMixIn_INSEE, XMLGenericData_2_1):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.frequencies_supported:
            self.frequencies_supported = self._frequencies_supported
    
class XMLSpecificData_2_1(XMLDataBase):
    """SDMX 2.1 application/vnd.sdmx.structurespecificdata+xml;version=2.1    
    """

    XMLStructureKlass = XMLStructure_2_1

    @timeit("xml_utils.XMLSpecificData_2_1.is_series_tag", stats_only=True)
    def is_series_tag(self, element):
        return etree.QName(element.tag).localname == 'Series'
    
    @timeit("xml_utils.XMLSpecificData_2_1.get_observations", stats_only=True)
    def get_observations(self, series, frequency):
        
        observations = deque()

        for observation in series.iterchildren():

            item = {"period": None, "value": None, "attributes": {}}
            
            item["period"] = observation.attrib[self.field_obs_time_period]
            item["value"] = observation.attrib[self.field_obs_value]
            
            for key, value in observation.attrib.items():
                if not key in [self.field_obs_time_period, self.field_obs_value]:
                    item["attributes"][key] = value
            
            observation.clear()
            
            observations.append(item)
            
        return list(observations)
    
    @timeit("xml_utils.XMLSpecificData_2_1.build_series", stats_only=True)
    def build_series(self, series):
        """
        :series ElementTree: Element from lxml        
        """
        dimensions = self.get_dimensions(series)
        attributes = self.get_attributes(series)
        frequency = self.get_frequency(series, dimensions, attributes)
        observations = self.get_observations(series, frequency)

        if not observations:
            msg = {"provider_name": self.provider_name, 
                   "dataset_code": self.dataset_code}            
            raise errors.RejectEmptySeries(**msg)                
        
        bson = {
            "provider_name": self.provider_name,
            "dataset_code": self.dataset_code,
            "dimensions": dimensions,
            "frequency": frequency,
            "name": self.get_name(series, dimensions, attributes),
            "key": self.get_key(series, dimensions, attributes), 
            "observations": observations,
            "series_keys": {},
            "series_attributes": attributes,
        }
        bson["start_date"] = self.start_date(series, frequency, observations, bson)
        bson["end_date"] = self.end_date(series, frequency, observations, bson)
        bson["last_update"] = self.get_last_update(series, dimensions, attributes, bson)
        
        return bson

class XMLSpecificData_2_1_ECB(DataMixIn_ECB, XMLSpecificData_2_1):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.frequencies_supported:
            self.frequencies_supported = self._frequencies_supported

class XMLSpecificData_2_1_INSEE(DataMixIn_INSEE, XMLSpecificData_2_1):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.frequencies_supported:
            self.frequencies_supported = self._frequencies_supported

XML_STRUCTURE_KLASS = {
    "XMLStructure_1_0": XMLStructure_1_0,
    "XMLStructure_2_0": XMLStructure_2_0,
    "XMLStructure_2_1": XMLStructure_2_1,
    "XMLData_1_0": XMLData_1_0,
    "XMLData_1_0_FED": XMLData_1_0_FED,
    "XMLGenericData_2_0": XMLGenericData_2_0,
    "XMLGenericData_2_0_OECD": XMLGenericData_2_0_OECD,
    "XMLCompactData_2_0": XMLCompactData_2_0,
    "XMLCompactData_2_0_EUROSTAT": XMLCompactData_2_0_EUROSTAT,
    "XMLCompactData_2_0_DESTATIS": XMLCompactData_2_0_DESTATIS,
    "XMLGenericData_2_1": XMLGenericData_2_1,
    "XMLGenericData_2_1_ECB": XMLGenericData_2_1_ECB,
    "XMLGenericData_2_1_INSEE": XMLGenericData_2_1_INSEE,
    "XMLSpecificData_2_1": XMLSpecificData_2_1,
    "XMLSpecificData_2_1_ECB": XMLSpecificData_2_1_ECB,
    "XMLSpecificData_2_1_INSEE": XMLSpecificData_2_1_INSEE,
}

