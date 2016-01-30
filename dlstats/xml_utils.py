# -*- coding: utf-8 -*-

from pprint import pprint
import logging
from collections import deque, OrderedDict
from datetime import datetime
import re

from lxml import etree
import pandas

from dlstats import remote
from dlstats import errors

logger = logging.getLogger(__name__)

path_name_lang = etree.XPath("./*[local-name()='Name'][@xml:lang=$lang]")

path_ref = etree.XPath("./*[local-name()='Ref']")

REGEX_DATE_P3M = re.compile(r"(.*)-Q(.*)")
#REGEX_DATE_P3M = re.compile(r"(\d+)-Q(\d)")
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


class XMLSDMX_2_1:

    def __init__(self, 
                 sdmx_url=None, agencyID=None, data_headers={}, structure_headers={},
                 client=None, cache=None, 
                 **http_cfg):
        
        self.sdmx_url = sdmx_url
        self.agencyID = agencyID
        if not self.sdmx_url:
            self.sdmx_url = SDMX_PROVIDERS[self.agencyID]["url"]        
        self.data_headers = data_headers
        self.structure_headers = structure_headers
        self.client = client or remote.REST(cache=cache, http_cfg=http_cfg)

    def query_rest(self, url, **kwargs):
        logger.info('Requesting %s', url)
        source, final_url, headers, status_code = self.client.get(url, **kwargs)
        return source, final_url, headers, status_code
    
    def codelist(self, cl_code=None, headers={}, url=None, references=None):
        if not url:
            url = "%s/codelist/%s/%s" % (self.sdmx_url, self.agencyID, cl_code)
            if references:
                url = "%s?references=" % (url, references)
        logger.info('Requesting %s', url)
        source, final_url, headers, status_code = self.query_rest(url, headers=headers)
        return source

def dataset_converter_v1(xml, dataset_code):
    bson = {}
    bson["provider_name"] = xml.provider_name
    bson["dataset_code"] = dataset_code
    bson["attribute_list"] = OrderedDict()
    bson["dimension_list"] = OrderedDict()
    
    try:
        bson["name"] = xml.get_dataset_name(dataset_code)
    except Exception as err:
        logger.warning("get_dataset_name() error[%s] for dataset[%s]" % (str(err), dataset_code))
    
    for key in xml.dimension_keys:
        bson["dimension_list"][key] = [(k, v) for k, v in xml.dimensions[key]["enum"].items()]

    for key in xml.attribute_keys:
        bson["attribute_list"][key] = [(k, v) for k, v in xml.attributes[key]["enum"].items()]
        
    return bson

def dataset_converter_v2(xml, dataset_code):
    bson = {}
    bson["provider_name"] = xml.provider_name
    bson["dataset_code"] = dataset_code
    bson["codelists"] = {}
    bson["dimension_keys"] = xml.dimension_keys
    bson["attribute_keys"] = xml.attribute_keys
    
    bson["name"] = None
    """
    try:
        bson["name"] = xml.get_dataset_name(dataset_code)
    except Exception as err:
        logger.warning("get_dataset_name() error[%s] for dataset[%s]" % (str(err), dataset_code))
    """
    
    for key in xml.dimension_keys:
        if xml.dimensions[key]["enum"]:
            bson["codelists"][key] = dict(xml.dimensions[key]["enum"].items())
        else:
            bson["codelists"][key] = {}

    for key in xml.attribute_keys:
        if xml.attributes[key]["enum"]:
            bson["codelists"][key] = dict(xml.attributes[key]["enum"].items())
        else:
            bson["codelists"][key] = {}
    
    bson["concepts"] = {}
    for key, value in xml.concepts.items():
        bson["concepts"][key] = value["name"]
    
    return bson

DATASETS_CONVERTERS = {
    "dlstats_v1": dataset_converter_v1,
    "dlstats_v2": dataset_converter_v2,
}


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

        self.dimension_keys = []
        self.attribute_keys = []
        
        self.agencies = OrderedDict()
        self.categories = OrderedDict()
        self.categorisations = OrderedDict()
        self.categorisations_dataflows = OrderedDict()
        self.categorisations_categories = OrderedDict()
        self.dataflows = OrderedDict()
        
        self.dimensions = OrderedDict()
        self.attributes = OrderedDict()
        self.codelists = OrderedDict()
        self.concepts = OrderedDict()
        
    def fixtag(self, ns, tag):
        ns = self.TAGS_MAP.get(ns, ns)
        return '{' + self.nsmap[ns] + '}' + tag

    def get_dsd_id(self, dataflow_key):
        raise NotImplementedError()

    def get_dataset_name(self, dataflow_key):
        raise NotImplementedError()
        
    def process_agency(self, element):
        raise NotImplementedError()

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

    def process_dimension(self, element):
        raise NotImplementedError()

    def process_attribute(self, element):
        raise NotImplementedError()

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
    
    def process_concept(self, element):
        """
        <structure:Concept id="FREQ" agency="FRB">
            <structure:Name>Frequency</structure:Name>
        </structure:Concept>
        """
        _id = element.attrib.get('id')
        if not _id in self.concepts:
            self.concepts[_id] = {
                'id': _id,
                'name': self.get_name_element(element),
                "attrs": dict(element.attrib)
            }
        #TODO: clear
        
    def process_codelist(self, element):
        """
        <structure:CodeList id="CL_OBS_STATUS" agency="FRB">
            <structure:Name>Observation Status</structure:Name>
            <structure:Code value="A">
                <structure:Description>Normal</structure:Description>
            </structure:Code>
        </structure:CodeList>        
        """
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
                    self.codelists[_id]["enum"][key] = child[0].text
        
        element.clear()
        
    def process_dimension(self, element):
        """
        <structure:Dimension concept="CREDTYP" codelist="CL_CCOUT_CREDTYP"/>
        
        > Data pour TERMS:
            <frb:DataSet id="TERMS" xsi:schemaLocation="http://www.federalreserve.gov/structure/compact/G19_TERMS G19_TERMS.xsd">
                <kf:Series CURRENCY="USD" FREQ="129" ISSUE="COMBANK" SERIES_NAME="RIFLPBCIANM48_N.M" TERMS="NEWCAR" UNIT="Percent" UNIT_MULT="1">
                    ....observations
        
        > Data pour 
            <frb:DataSet id="CCOUT" xsi:schemaLocation="http://www.federalreserve.gov/structure/compact/G19_CCOUT G19_CCOUT.xsd">
                <kf:Series CREDTYP="TOTAL" CURRENCY="USD" DATAREP="PCTCHG" FREQ="129" HOLDER="ALL" SA="SA" SERIES_NAME="DTCTL_@%A_BA.M" UNIT="Percent" UNIT_MULT="1">
                    ....observations
        
        > Structure dimensions/attributes pour : CCOUT
            <structure:KeyFamily id="CCOUT" agency="FRB">
                <structure:Name>Consumer Credit Outstanding</structure:Name>
                <structure:Components>
                    <structure:Dimension concept="CREDTYP" codelist="CL_CCOUT_CREDTYP"/>
                    ...        
        
        > Structure dimensions/attributes pour : TERMS
            <structure:KeyFamily id="TERMS" agency="FRB">
                <structure:Name>Terms of Credit Outstanding</structure:Name>
                <structure:Components>
                    <structure:Dimension concept="ISSUE" codelist="CL_ISSUE"/>
                    ...        
        """
        _id = element.attrib.get('concept')
        
        codelist = element.attrib.get('codelist')
        if codelist:
            name = self.codelists[codelist]["name"]
            values = self.codelists[codelist]["enum"]
        else:
            name = _id
            values = {}
        
        if not _id in self.dimensions:
            self.dimensions[_id] = {
                "id": _id,
                "name": name,                                
                "enum": values,
                "attrs": dict(element.attrib)
            }
            self.dimension_keys.append(_id)
                
        element.clear()
        
    def process_attribute(self, element):
        """
        <structure:Attribute concept="UNIT" codelist="CL_UNIT" attachmentLevel="Series" assignmentStatus="Mandatory"/>
        """

        _id = element.attrib.get('concept')
        
        if not _id in self.attributes:
            codelist = element.attrib.get('codelist')
            if codelist:
                name = self.codelists[codelist]["name"]
                values = self.codelists[codelist]["enum"]
            else:
                name = _id
                values = {}
                            
            self.attributes[_id] = {
                "id": _id,
                "name": name,                                
                "enum": values,
                "attrs": dict(element.attrib)
            }
            self.attribute_keys.append(_id)
                
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
                elif element.tag == self.fixtag("structure", "Dimension"):
                    self.process_dimension(element)
                elif element.tag == self.fixtag("structure", "Attribute"):
                    self.process_attribute(element)
    

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

    def process_dimension(self, element):
        """
        <structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="FREQ" codelistAgency="SDMX" codelist="CL_FREQ" isFrequencyDimension="true"/>
        """

        _id = element.attrib.get('conceptRef')
        
        if not _id in self.dimensions:
            codelist = element.attrib.get('codelist')
            if codelist:
                name = self.codelists[codelist]["name"]
                values = self.codelists[codelist]["enum"]
            else:
                name = _id
                values = {}
            
            self.dimensions[_id] = {
                "id": _id,
                "name": name,                                
                "enum": values,
                "attrs": dict(element.attrib)
            }
            
            self.dimension_keys.append(_id)

        element.clear()    

    def process_attribute(self, element):
        """
        <structure:Attribute 
            conceptRef="TIME_FORMAT" 
            conceptSchemeRef="CONCEPTS" 
            conceptSchemeAgency="EUROSTAT" 
            codelistAgency="SDMX" 
            codelist="CL_TIME_FORMAT" 
            attachmentLevel="Series" 
            assignmentStatus="Mandatory">
        </structure:Attribute>
        
        """
        _id = element.attrib.get('conceptRef')
        
        if not _id in self.attributes:
        
            codelist = element.attrib.get('codelist')
            if codelist:
                name = self.codelists[codelist]["name"]
                values = self.codelists[codelist]["enum"]
            else:
                name = _id
                values = None
                            
            self.attributes[_id] = {
                "id": _id,
                "name": name,                                
                "enum": values,
                "attrs": dict(element.attrib)
            }
            
            self.attribute_keys.append(_id)
            
        element.clear()    

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
                elif element.tag == self.fixtag("structure", "Dimension"):
                    self.process_dimension(element)
                elif element.tag == self.fixtag("structure", "Attribute"):
                    self.process_attribute(element)
                """
                TODO: <structure:TimeDimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="TIME_PERIOD"><structure:TextFormat textType="String"></structure:TextFormat></structure:TimeDimension>
                """
    
class XMLStructure_2_1(XMLStructure_2_0):
    """Parsing SDMX 2.1 datastructure only. with resource ID and references=all
    
    ex: https://sdw-wsrest.ecb.europa.eu/service/datastructure/ECB/ECB_EXR1?references=all
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
        if not cl_code in self.codelists:
            logger.warning("codelist not found [%s] for provider[%s]" % (cl_code, self.provider_name))
            
            try:
                source = self.sdmx_client.codelist(cl_code=cl_code)
                tree = etree.parse(source)            
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

    def get_dsd_id(self, dataflow_key):
        return self.dataflows[dataflow_key].get('dsd_id')

    def get_dataset_name(self, dataflow_key):
        return self.dataflows[dataflow_key].get('name')
    
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
        """
        <str:Dataflow id="IPI-2010-A21" urn="urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=FR1:IPI-2010-A21(1.0)" agencyID="FR1" version="1.0">
            <com:Name xml:lang="fr">Indice de la production industrielle (base 2010) - NAF niveau A21</com:Name>
            <com:Name xml:lang="en">Industrial production index (base 2010) - NAF level A21</com:Name>
            <str:Structure>
                <Ref id="IPI-2010-A21" version="1.0" agencyID="FR1" package="datastructure" class="DataStructure"/>
            </str:Structure>
        </str:Dataflow>        
        """

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
        """
        <str:Categorisation id="CAT_IPI-2010_IPI-2010-A21" urn="urn:sdmx:org.sdmx.infomodel.categoryscheme.Categorisation=FR1:CAT_IPI-2010_IPI-2010-A21(1.0)" agencyID="FR1" version="1.0">
            <com:Name xml:lang="fr">Association entre la catégorie IPI-2010 et le dataflow IPI-2010-A21</com:Name>
            <com:Name xml:lang="en">Association between category IPI-2010 and dataflow IPI-2010-A21</com:Name>
            <str:Source>
                <Ref id="IPI-2010-A21" version="1.0" agencyID="FR1" package="datastructure" class="Dataflow"/>
            </str:Source>
            <str:Target>
                <Ref id="IPI-2010" maintainableParentID="CLASSEMENT_DATAFLOWS" maintainableParentVersion="1.0" agencyID="FR1" package="categoryscheme" class="Category"/>
            </str:Target>
        </str:Categorisation>        
        """
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
        """
        <str:Codelist id="CL_COLLECTION" urn="urn:sdmx:org.sdmx.infomodel.codelist.Codelist=ECB:CL_COLLECTION(1.0)" agencyID="ECB" version="1.0">
            <com:Name xml:lang="en">Collection indicator code list</com:Name>
            <str:Code id="A" urn="urn:sdmx:org.sdmx.infomodel.codelist.Code=ECB:CL_COLLECTION(1.0).A">
                <com:Name xml:lang="en">Average of observations through period</com:Name>
            </str:Code>
        </str:Codelist>        
        """
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
        
    def process_dimension(self, element):

        if element.getparent().tag != self.fixtag("structure", "DimensionList"):
            return
        
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

        self.dimensions[_id] = {
            "id": _id,
            "name": name,                                
            "enum": values,
            "attrs": dict(element.attrib)
        }
        if not _id in self.dimension_keys:
            self.dimension_keys.append(_id)
        
        element.clear()    
        
    def process_attribute(self, element):

        if element.getparent().tag != self.fixtag("structure", "AttributeList"):
            return

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
            
        self.attributes[_id] = {
            "id": _id,
            "name": name,                                
            "enum": values,
            "attrs": dict(element.attrib)
        }
        self.attribute_keys.append(_id)
        element.clear()    
        
    def process(self, filepath):
        """
        TODO: process sur plusieurs files
        """
        
        tree_iterator = etree.iterparse(filepath, events=['end', 'start-ns'])
        
        self.nsmap = get_nsmap(tree_iterator)
        
        for event, element in tree_iterator:
            if event == 'end':
                #OrganisationSchemes

                if element.tag == self.fixtag("str", "Agency"):
                    self.process_agency(element)
                
                elif element.tag == self.fixtag("str", "Category"):                    
                    self.process_category(element)

                elif element.tag == self.fixtag("str", "Categorisation"):
                    self.process_categorisation(element)

                elif element.tag == self.fixtag("str", "Dataflow"):
                    self.process_dataflow(element)

                elif element.tag == self.fixtag("str", "Codelist"):
                    self.process_codelist(element)

                elif element.tag == self.fixtag("str", "Concept"):
                    self.process_concept(element)

                elif element.tag == self.fixtag("str", "Dimension"):
                    self.process_dimension(element)

                elif element.tag == self.fixtag("str", "Attribute"):
                    self.process_attribute(element)
                                
            #element.clear()

def series_converter_v1(bson, xml):
    #print("---------------------------------------------------------------")
    #pprint(bson)
    #print("---------------------------------------------------------------")
    #bson['version'] = 1
    observations = bson.pop("observations")
    bson.pop("series_keys")
    
    bson["values"] = [obs["value"] for obs in observations]
    
    attributes = {}
    for obs in observations:
        #TODO: attr vide
        for key, value in obs["attributes"].items():
            if not key in attributes:
                attributes[key] = []
            attributes[key].append(value)
    
    bson["attributes"] = attributes
    
    return bson

def series_converter_v2(bson, xml):
    
    #bson['version'] = 2
    bson.pop("series_keys", None)
    
    bson["attributes"] = dict(bson.pop("series_attributes", {}))
    bson["values"] = bson.pop("observations")
    
    date_value = bson.pop("last_update")
    if not date_value:
        date_value = datetime.now()
    last_update = datetime(date_value.year, date_value.month, date_value.day, date_value.hour, date_value.minute)
    
    bson["last_update"] = last_update
        
    return bson

SERIES_CONVERTERS = {
    "dlstats_v1": series_converter_v1,
    "dlstats_v2": series_converter_v2,
}

class XMLDataBase:
    
    NS_TAG_DATA = None
    PROVIDER_NAME = None
    XMLStructureKlass = None

    def __init__(self, 
                 provider_name=None,
                 dataset_code=None,
                 ns_tag_data=None,
                 field_frequency="FREQ", 
                 field_obs_time_period="TIME_PERIOD",
                 field_obs_value="OBS_VALUE",
                 dimension_keys=None,                   #required: for separate dimensions/attributes
                 dsd_filepath=None, 
                 dimensions=None,                       #optional: for get_name()
                 frequencies_supported=None, 
                 frequencies_rejected=None,
                 series_converter="dlstats_v2"):
        
        self.provider_name = provider_name or self.PROVIDER_NAME
        self.dataset_code = dataset_code
        self.dimension_keys = dimension_keys or []
        self.dsd_filepath = dsd_filepath
        self.dimensions = dimensions
        
        if not self.dimensions and self.XMLStructureKlass and self.dsd_filepath:
            self.xml_dsd = self.XMLStructureKlass(self.provider_name)
            self.xml_dsd.process(dsd_filepath)
            self.dimensions = self.xml_dsd.dimensions
        
        self.frequencies_supported = frequencies_supported
        self.frequencies_rejected = frequencies_rejected

        self.field_frequency = field_frequency
        self.field_obs_time_period = field_obs_time_period
        self.field_obs_value = field_obs_value
        
        self.series_converter = SERIES_CONVERTERS[series_converter]

        self.nsmap = {}
        self.tree_iterator = None
        
        self._ns_tag_data = ns_tag_data
        
        self._period_cache = {}
        
    def get_ordinal_from_period(self, date_str, freq=None):
        if not freq in ['Q', 'M', 'A', 'W']:
            return pandas.Period(date_str, freq=freq).ordinal
                
        key = "%s.%s" % (date_str, freq)
        if key in self._period_cache:
            return self._period_cache[key]
        
        self._period_cache[key] = pandas.Period(date_str, freq=freq).ordinal
        return self._period_cache[key]

    @property    
    def ns_tag_data(self):
        if self._ns_tag_data:
            return self._ns_tag_data
        else:
            return self.NS_TAG_DATA
        
    def _get_nsmap(self, tree_iterator):
        return get_nsmap(tree_iterator)
        
    def _load_data(self, filepath):
        self.tree_iterator = etree.iterparse(filepath, events=['end', 'start-ns'])
        self.nsmap = self._get_nsmap(self.tree_iterator)

    def fixtag(self, ns, tag):
        if not ns in self.nsmap:
            msg = "Namespace not found[%s] - tag[%s] - provider[%s] - nsmap[%s]"
            raise Exception(msg %(ns, tag, self.provider_name, self.nsmap))
        return '{' + self.nsmap[ns] + '}' + tag

    def is_series_tag(self, element):
        return element.tag == self.fixtag(self.ns_tag_data, 'Series')

    def process(self, filepath):
        
        self._load_data(filepath)
        
        for event, element in self.tree_iterator:
            if event == 'end':
                
                if self.is_series_tag(element):
                    try:
                        yield self.one_series(element), None
                    except errors.RejectFrequency as err:
                        yield (None, err)
                    except errors.RejectEmptySeries as err:
                        yield (None, err)
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

    """
    def get_name(self, series, dimensions, attributes):
        raise NotImplementedError()

    def get_key(self, series, dimensions, attributes):
        raise NotImplementedError()
    """
    
    def get_name(self, series, dimensions, attributes):
        if self.dimensions:
            values = []
            for key in self.dimension_keys:
                dim_enum = self.dimensions[key]["enum"]
                value = dim_enum[dimensions[key]]
                values.append(value)
            
            return " - ".join(values)
        else:
            return " - ".join([dimensions[key] for key in self.dimension_keys])

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
    
    def get_frequency(self, series, dimensions, attributes):
        frequency = self.search_frequency(series, dimensions, attributes)
        frequency = self.fixe_frequency(frequency, series, dimensions, attributes)
        self.valid_frequency(frequency, series, dimensions)
        return frequency

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
        return self.get_ordinal_from_period(_date, freq=frequency)

    def end_date(self, series, frequency, observations=[], bson=None):
        _date = observations[-1]["period"]
        return self.get_ordinal_from_period(_date, freq=frequency)

    def finalize_bson(self, bson):
        return self.series_converter(bson, self)

    def build_series(self, series):
        raise NotImplementedError()

    def one_series(self, series):
        bson = self.build_series(series)
        return self.finalize_bson(bson)

class XMLCompactData_2_0(XMLDataBase):

    NS_TAG_DATA = "data"
    XMLStructureKlass = XMLStructure_2_0
    
    def get_observations(self, series, frequency):
        """
        element: <data:Series>
        
        <data:Series FREQ="Q" s_adj="NSA" unit="I10" na_item="NULC_HW" geo="AT" TIME_FORMAT="P3M">
            <data:Obs TIME_PERIOD="1996-Q1" OBS_VALUE="89.4" />
            <data:Obs TIME_PERIOD="1996-Q2" OBS_VALUE="89.9" />
        </data:Series>
        
        """
        observations = []
        for obs in series.iterchildren():

            item = {"period": None, "value": None, "attributes": {}}
            
            if obs.tag == self.fixtag(self.ns_tag_data, 'Obs'):
                item["period"] = obs.attrib["TIME_PERIOD"]
                item["period_o"] = item["period"]
                item["ordinal"] = self.get_ordinal_from_period(item["period"], freq=frequency)
                #TODO: value manquante
                item["value"] = obs.attrib.get("OBS_VALUE", "")
                
                for key, value in obs.attrib.items():
                    #TODO: if not key in [self.field_obs_time_period, self.field_obs_value]:
                    if not key in ['TIME_PERIOD', 'OBS_VALUE']:
                        item["attributes"][key] = value
                
                observations.append(item)

        return observations    
    
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
        return self.get_ordinal_from_period(date_string, freq=freq)
        
    def end_date(self, series, frequency, observations=[], bson=None):
        time_format = series.attrib.get('TIME_FORMAT')
        if not time_format or not time_format in SPECIAL_DATE_FORMATS:
            return super().start_date(series, frequency, observations=observations, bson=bson)

        period = observations[-1]["period"]        
        (date_string, freq) = parse_special_date(period, time_format, self.dataset_code)
        return self.get_ordinal_from_period(date_string, freq=freq)
        
class XMLData_1_0(XMLDataBase):
    """SDMX 1.0
    http://www.SDMX.org/resources/SDMXML/schemas/v1_0/message
    """

    NS_TAG_DATA = "frb"
    XMLStructureKlass = XMLStructure_1_0

    def is_series_tag(self, element):
        localname = etree.QName(element.tag).localname
        return localname == 'Series'

    def get_observations(self, series, frequency):
        observations = []

        for obs in series.iterchildren():
            item = {"period": None, "value": None, "attributes": {}}
            
            if obs.tag == self.fixtag(self.ns_tag_data, 'Obs'):
                item["period"] = obs.attrib["TIME_PERIOD"]
                item["period_o"] = item["period"]
                item["ordinal"] = self.get_ordinal_from_period(item["period"], freq=frequency)
                item["value"] = obs.attrib["OBS_VALUE"]
                
                for key, value in obs.attrib.items():
                    if not key in ['TIME_PERIOD', 'OBS_VALUE']:
                        item["attributes"][key] = value
                
                observations.append(item)

        return observations    
    
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

    
class XMLData_1_0_FED(XMLData_1_0):
    """
    TODO: si je stocke FREQ: 129 dans series, il faut retrouver 129 dans les dimension_list["FREQ"]
    """

    PROVIDER_NAME = "FED"
    NS_TAG_DATA = "frb"
     
    _frequency_map = {
        "129": "M",   #FIXME: <frb:Obs OBS_STATUS="A" OBS_VALUE="10.20" TIME_PERIOD="1972-02-29" />
        "203": "A", 
        "162": "Q",
        "8": "D",
    }
    #frequencies_supported": ["M", "A", "D", "Q"],
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.frequencies_supported = list(self._frequency_map.keys()) + list(self._frequency_map.values())     

    def _get_nsmap(self, iterator):
        #FIXME: attention http://www.federalreserve.gov/structure/compact/G19_TERMS
        return {'common': 'http://www.SDMX.org/resources/SDMXML/schemas/v1_0/common',
                'frb': 'http://www.federalreserve.gov/structure/compact/common',
                'kf': 'http://www.federalreserve.gov/structure/compact/G19_TERMS',
                'message': 'http://www.SDMX.org/resources/SDMXML/schemas/v1_0/message',
                'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}        

    def get_name(self, series, dimensions, attributes):
        return self.get_key(series, dimensions, attributes)
        
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

class XMLGenericData_2_1(XMLDataBase):
    """SDMX 2.1 application/vnd.sdmx.genericdata+xml;version=2.1
    """

    NS_TAG_DATA = "generic"
    XMLStructureKlass = XMLStructure_2_1
    
    def _get_values(self, element):
        """Parcours les children qui ont un id/value pour chaque entrée

        ex:
        <generic:SeriesKey>
            <generic:Value id="FREQ" value="M"/>
            <generic:Value id="CURRENCY" value="NOK"/>
            <generic:Value id="CURRENCY_DENOM" value="EUR"/>
        </generic:SeriesKey>                
        """
        d = OrderedDict()
        for value in element.iterchildren():
            key = value.attrib["id"]
            value =value.attrib["value"]
            d[key] = value
        return d
    
    #def search_frequency(self, series, dimensions, attributes):
    #    return dimensions[self.field_frequency]

    def get_observations(self, series, frequency):
        
        observations = []
        
        for element in series.xpath("child::%s:Obs" % self.ns_tag_data, namespaces=self.nsmap):

            item = {"period": None, "value": None, "attributes": {}}
            for child in element.getchildren():
                
                if child.tag == self.fixtag(self.ns_tag_data, 'ObsDimension'):
                    item["period"] = child.attrib["value"]
                    item["period_o"] = item["period"]
                    item["ordinal"] = self.get_ordinal_from_period(item["period"], freq=frequency)
                
                elif child.tag == self.fixtag(self.ns_tag_data, 'ObsValue'):
                    item["value"] = child.attrib["value"]
                
                elif child.tag == self.fixtag(self.ns_tag_data, 'Attributes'):
                    for key, value in self._get_values(child).items():
                        item["attributes"][key] = value
                
                child.clear()
            
            observations.append(item)
            element.clear()
            
        return observations

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
        """
        One series:
        
        <generic:Series>
            <generic:SeriesKey>
                <generic:Value id="FREQ" value="M"/>
                <generic:Value id="PRODUIT" value="B"/>
            </generic:SeriesKey>
            <generic:Attributes>
                <generic:Value id="LAST_UPDATE" value="2016-01-08"/>
                <generic:Value id="UNIT_MEASURE" value="SO"/>            
            </generic:Attributes>
            <generic:Obs>
                <generic:ObsDimension value="2015-11"/>
                <generic:ObsValue value="96.98"/>
                <generic:Attributes>
                    <generic:Value id="OBS_STATUS" value="A"/>
                </generic:Attributes>
            </generic:Obs>
        </generic:Series>                        
        
        """
        dimensions = self.get_dimensions(series)
        attributes = self.get_attributes(series)
        frequency = self.get_frequency(series, dimensions, attributes)
        observations = self.get_observations(series, frequency)
        
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
    Pb Frequency ECB:
    https://sdw-wsrest.ecb.europa.eu/service/codelist/ECB/CL_FREQ
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
    
    frequencies_supported = [
        "A", #Annual
        "D", #Daily
        "M", #Monthly
        "Q", #Quarterly
        "W"  #Weekly
    ]
    frequencies_rejected = [
        "E", #Event
        "B", #Business
        "H", #Half-yearly
        "N", #Minutely
        "S", #Half Yearly, semester 
    ]
            
    TODO: http://sdw-wsrest.ecb.int/service/contentconstraint/ECB/EXR2_CONSTRAINTS
    """
    
    PROVIDER_NAME = "ECB"
    
    _frequencies_supported = ["A", "M", "Q", "W", "D"]
    
    def get_name(self, series, dimensions, attributes):
        if "TITLE_COMPL" in attributes:
            return attributes["TITLE_COMPL"]
        else:
            logger.warning("Not TITLE_COMPL field for provider[%s] - dataset[%s]" % (self.provider_name,
                                                                                     self.dataset_code))
            return attributes.get("TITLE")

    def get_key(self, series, dimensions, attributes):
        return ".".join([dimensions[key] for key in self.dimension_keys])
    
    def TODO_is_updated(self, series):
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
    

class DataMixIn_INSEE:
    """Common class for INSEE datas
    """
    
    PROVIDER_NAME = "INSEE"

    #_frequencies_rejected = ["S", "B", "I"]
    _frequencies_supported = ["A", "M", "Q", "W", "D"]
    
    def _dim_attrs(self,  dimensions, attributes):
        attributes_keys = "|".join(list(attributes.keys())) if attributes else ""
        dimensions_keys = "|".join(list(dimensions.keys())) if dimensions else ""
        return "DIMENSIONS[%s] - ATTRIBUTES[%s]" % (attributes_keys, dimensions_keys)
    
    def get_name(self, series, dimensions, attributes):
        if "TITLE" in attributes:            
            return attributes["TITLE"]
        elif "TITLE" in dimensions:
            return dimensions["TITLE"]            
        raise Exception("TITLE field not found for INSEE - %s" % self._dim_attrs(dimensions, attributes))

    def get_key(self, series, dimensions, attributes):
        return attributes["IDBANK"]

    def fixe_frequency(self, frequency, series, dimensions, attributes):
        if frequency == "T":
            #TODO: T equal Trimestrial for INSEE
            frequency = "Q"
            idbank = self.get_key(series, dimensions, attributes)
            logger.warning("Replace T frequency by Q - dataset[%s] - idbank[%s]" % (self.dataset_code, idbank))

        return frequency

    def get_last_update(self, series, dimensions, attributes, bson=None):
        return datetime.strptime(attributes["LAST_UPDATE"], "%Y-%m-%d")

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
        self.frequencies_supported = self._frequencies_supported

class XMLGenericData_2_1_INSEE(DataMixIn_INSEE, XMLGenericData_2_1):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.frequencies_supported = self._frequencies_supported
    
class XMLSpecificData_2_1(XMLDataBase):
    """SDMX 2.1 application/vnd.sdmx.structurespecificdata+xml;version=2.1    
    """

    def is_series_tag(self, element):
        localname = etree.QName(element.tag).localname
        return localname == 'Series'

    def get_observations(self, series, frequency):
        
        observations = []

        for observation in series.iterchildren():

            item = {"period": None, "value": None, "attributes": {}}
            
            item["period"] = observation.attrib[self.field_obs_time_period]
            item["period_o"] = item["period"] 
            item["ordinal"] = self.get_ordinal_from_period(item["period"], freq=frequency)
            item["value"] = observation.attrib[self.field_obs_value]
            
            for key, value in observation.attrib.items():
                if not key in [self.field_obs_time_period, self.field_obs_value]:
                    item["attributes"][key] = value
            
            observation.clear()
            
            observations.append(item)
            
        return observations
    
    def build_series(self, series):
        """
        :series ElementTree: Element from lxml        
        """
        dimensions = self.get_dimensions(series)
        attributes = self.get_attributes(series)
        frequency = self.get_frequency(series, dimensions, attributes)
        observations = self.get_observations(series, frequency)
        
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
        self.frequencies_supported = self._frequencies_supported

class XMLSpecificData_2_1_INSEE(DataMixIn_INSEE, XMLSpecificData_2_1):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.frequencies_supported = self._frequencies_supported

XLM_KLASS = {
    "XMLStructure_1_0": XMLStructure_1_0,
    "XMLStructure_2_0": XMLStructure_2_0,
    "XMLStructure_2_1": XMLStructure_2_1,
    "XMLData_1_0": XMLData_1_0,
    "XMLData_1_0_FED": XMLData_1_0_FED,
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

    
"""
TODO: pour le parsing des observations:

class XMLGenericDataAsyncIO(XMLGenericData):

    def run_asyncio(self):
        import asyncio
        
        if os.name == 'nt':        
            self.io_loop = asyncio.ProactorEventLoop()
            asyncio.set_event_loop(self.io_loop)
        else:
            self.io_loop = asyncio.get_event_loop()
        
        @asyncio.coroutine
        def process():
            start = time.time()
            count = 0
            count_reject_frequencies = 0
            count_values = 0
            for series in self.process_series():                
                count += 1
                try:
                    bson = yield from self.one_series_async(series)
                    count_values += len(bson["values"])
                    #print(count, bson["key"], len(bson["values"]))
                except RejectFrequency as err:
                    count_reject_frequencies += 1
                    #print(str(err))
                
            end = time.time() - start
            print("SERIES[%s] - REJECT[%s] - DUREE[%.3f] - VALUES[%s]" % (count, count_reject_frequencies, end, count_values))
        
        f = asyncio.wait([process()])
        try:            
            self.io_loop.run_until_complete(f)
        except KeyboardInterrupt:
            pass    

    def process_series(self):
        for event, element in self.tree_iterator:
            if event == 'end':
                if element.tag == self.fixtag('generic', 'Series'):
                    try:
                        yield element
                    except RejectFrequency:
                        raise
                    finally:
                        element.clear()

    @asyncio.coroutine
    def one_series_async(self, series):        
        return self.one_series(series)
        #print(bson["key"], len(bson["values"]))
        #yield bson
"""        
                
