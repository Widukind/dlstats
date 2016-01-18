# -*- coding: utf-8 -*-

"""
destatis:
    xmlns:ns1="urn:sdmx:org.sdmx.infomodel.datastructure.DataStructure=IMF:ECOFIN_DSD(1.0):ObsLevelDim:TIME_PERIOD"
    
    Financial Sector (Depository corporations survey | Central Bank survey | Other Financial Corporations Survey | Financial Soundness Indicators | Debt securities | Interest rates | Stock market)
    Depository corporations survey: 
        https://www.destatis.de/sddsplus/DCS.xml
    Central bank survey
        https://www.destatis.de/sddsplus/CBS.xml
        
    <message:Header>
      <message:ID>IREF000331</message:ID>
      <message:Test>False</message:Test>
      <message:Prepared>2015-12-30T11:26:00</message:Prepared>
      <message:Sender id="DE2"></message:Sender>
      <message:Receiver id="1C0"></message:Receiver>
      <message:KeyFamilyRef>ECOFIN_DSD</message:KeyFamilyRef>
      <message:KeyFamilyAgency>IMF</message:KeyFamilyAgency>
      <message:DataSetID>DCS</message:DataSetID>
      <message:DataSetAction>Replace</message:DataSetAction>
    </message:Header>

    <ns1:DataSet>
      <ns1:Series FREQ="M" DATA_DOMAIN="DCS" REF_AREA="DE" INDICATOR="FM1_EUR" COUNTERPART_AREA="U2">
        <ns1:Obs TIME_PERIOD="2001-09" OBS_VALUE="593935" OBS_STATUS="A"></ns1:Obs>
        
Eurostat:
    xmlns:data="urn:sdmx:org.sdmx.infomodel.keyfamily.KeyFamily=EUROSTAT:namq_10_lp_ulc_DSD:compact"

    <Header>
        <ID>namq_10_lp_ulc</ID>                          !!!
        <Test>false</Test>
        <Name xml:lang="en">namq_10_lp_ulc</Name>        !!!
        <Prepared>2016-01-14T05:32:50</Prepared>
        <Sender id="EUROSTAT">
            <Name xml:lang="en">EUROSTAT</Name>
        </Sender>
        <Receiver id="XML">
            <Name xml:lang="en">SDMX-ML File</Name>
        </Receiver>
        <DataSetID>namq_10_lp_ulc</DataSetID>            !!!!
        <Extracted>2016-01-14T05:32:50</Extracted>
    </Header>

    <data:DataSet>
        <data:Series FREQ="Q" s_adj="NSA" unit="I10" na_item="NULC_HW" geo="AT" TIME_FORMAT="P3M">
            <data:Obs TIME_PERIOD="1996-Q1" OBS_VALUE="89.4" />
     
"""

from pprint import pprint
import logging
from collections import deque, OrderedDict
from datetime import datetime
import re

import lxml.etree
import pandas
import arrow

from dlstats import remote
from dlstats import errors

logger = logging.getLogger(__name__)

path_name_lang = lxml.etree.XPath("./*[local-name()='Name'][attribute::xml:lang=$lang]")

path_ref = lxml.etree.XPath("./*[local-name()='Ref']")

REGEX_DATE_P3M = re.compile(r"(.*)-Q(.*)")
#REGEX_DATE_P3M = re.compile(r"(\d+)-Q(\d)")
REGEX_DATE_P1D = re.compile(r"(\d\d\d\d)(\d\d)(\d\d)")

def utcnow():
    return arrow.utcnow().datetime

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

class XMLStructure_2_0:
    """Parsing SDMX 2.0
    """

class XMLStructure_2_1:
    """Parsing SDMX 2.1 datastructure only. with resource ID and references=all
    
    ex: https://sdw-wsrest.ecb.europa.eu/service/datastructure/ECB/ECB_EXR1?references=all
    """
    
    def __init__(self, 
                 provider_name=None,
                 dataset_code=None, 
                 dsd_id=None,
                 field_time_dimension="TIME_PERIOD",
                 sdmx_client=None):
        
        self.provider_name = provider_name
        self.dataset_code = dataset_code
        self.dsd_id = dsd_id
        self.field_time_dimension = field_time_dimension

        self.sdmx_client = sdmx_client
        if not self.sdmx_client:
            self.sdmx_client = XMLSDMX_2_1(sdmx_url=SDMX_PROVIDERS[self.provider_name]["url"], 
                                           agencyID=self.provider_name)

        self.nsmap = {}

        #TODO: share dataflows structure
        self.dataflows = {}
        self.dimension_keys = []
        self.dataset_name = None
        
        self.dimensions = OrderedDict()
        self.attributes = OrderedDict()
        self.codelists = OrderedDict()
        self.conceptschemes = OrderedDict()
        
    def get_codelist(self, cl_code):
        if not cl_code in self.codelists:
            source = self.sdmx_client.codelist(cl_code=cl_code)
            tree = lxml.etree.parse(source)            
            namespaces = tree.getroot().nsmap
            #TODO: attention str://
            codelists = tree.xpath('.//str:Codelist', namespaces=namespaces)
            for code_list in codelists:
                self.process_code_list(code_list)
        
        return self.codelists[cl_code]    
    
    def process_code_list(self, code_list):
        """
        <str:Codelist id="CL_COLLECTION" urn="urn:sdmx:org.sdmx.infomodel.codelist.Codelist=ECB:CL_COLLECTION(1.0)" agencyID="ECB" version="1.0">
            <com:Name xml:lang="en">Collection indicator code list</com:Name>
            <str:Code id="A" urn="urn:sdmx:org.sdmx.infomodel.codelist.Code=ECB:CL_COLLECTION(1.0).A">
                <com:Name xml:lang="en">Average of observations through period</com:Name>
            </str:Code>
            ...
        </str:Codelist>
        """        
        _id = code_list.attrib["id"]
        
        if not _id in self.codelists:
            self.codelists[_id] = {"id": _id,
                                   "name": xml_get_name(code_list), 
                                   "codes": OrderedDict()}
            
        for element in code_list.iterchildren():
            localname = lxml.etree.QName(element.tag).localname
            if localname == "Code":
                name = xml_get_name(element)
                code_id = element.attrib.get('id')
                self.codelists[_id]["codes"][code_id] = name
            #element.clear()

    def process_concept_scheme(self, concept):
        """
        <str:Concept id="COUNT_AREA" urn="urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=ECB:ECB_CONCEPTS(1.0).COUNT_AREA">
            <com:Name xml:lang="en">Counterpart area</com:Name>
        </str:Concept>
        """
        _id = concept.attrib["id"]
        if not _id in self.conceptschemes:
            self.conceptschemes[_id] = xml_get_name(concept) 
        #concept.clear()
    
    def process_dimension(self, dimension):
        
        _id = dimension.attrib.get('id')

        if _id == self.field_time_dimension:            
            return
        
        if not _id in self.dimensions:
            self.dimensions[_id] = { "id": _id,
                                    "name": None,
                                    "dimensions": OrderedDict()}
        
        for element in dimension.iterchildren():
            localname = lxml.etree.QName(element.tag).localname
            
            if localname == "ConceptIdentity":                
                concept_id = path_ref(element)[0].attrib.get('id')
                self.dimensions[_id]["name"] = self.conceptschemes[concept_id]
            
            elif localname == "LocalRepresentation":
                child = element.getchildren()[0]
                if lxml.etree.QName(child.tag).localname != "Enumeration":
                    continue
                
                codelist_id = path_ref(child)[0].attrib.get('id')
                self.dimensions[_id]["dimensions"] = self.codelists[codelist_id]["codes"]

    def process_dimension_list(self, dimension_list):
        for dimension in dimension_list.iterchildren():
            self.process_dimension(dimension)
        self.dimension_keys = list(self.dimensions.keys())

    def process_attribute(self, attribute):
        
        _id = attribute.attrib.get('id')

        if not _id in self.attributes:
            self.attributes[_id] = { "id": _id,
                                    "name": None,
                                    "values": OrderedDict()}
        
        for element in attribute.iterchildren():
            localname = lxml.etree.QName(element.tag).localname
            
            if localname == "ConceptIdentity":                
                concept_id = path_ref(element)[0].attrib.get('id')
                self.attributes[_id]["name"] = self.conceptschemes[concept_id]
            
            elif localname == "LocalRepresentation":
                child = element.getchildren()[0]
                if lxml.etree.QName(child.tag).localname != "Enumeration":
                    continue
                
                codelist_id = path_ref(child)[0].attrib.get('id')
                #print("!!! : ", _id, codelist_id, child)
                self.attributes[_id]["values"] = self.get_codelist(codelist_id)["codes"]

    def process_attribute_list(self, attribute_list):
        for attribute in attribute_list.iterchildren():
            self.process_attribute(attribute)

    def fixtag(self,ns,tag):
        return '{' + self.nsmap[ns] + '}' + tag

    def process(self, filepath):
        
        tree_iterator = lxml.etree.iterparse(filepath, events=['end', 'start-ns'])
        
        self.nsmap = get_nsmap(tree_iterator)
        
        for event, element in tree_iterator:
            if event == 'end':
                localname = lxml.etree.QName(element.tag).localname
                
                #OrganisationSchemes
                if localname == "Dataflow":
                    key = element.attrib.get('id') 
                    name = xml_get_name(element)
                    for child in element.iterchildren():
                        if lxml.etree.QName(child.tag).localname == "Structure":
                            self.dsd_id = path_ref(child)[0].attrib.get('id')
                            self.dataflows[key] = self.dsd_id
                            break 
                        
                #Codelists
                elif localname == "Codelist":
                    self.process_code_list(element)
                
                #Concepts -> ConceptScheme -> Concept
                elif localname == "Concept":
                    self.process_concept_scheme(element)
                
                #DataStructures -> DataStructure
                elif localname == "DataStructure":
                    self.dataset_name = xml_get_name(element)
                    self.dsd_id = element.attrib.get("id")  
                
                elif localname == "DataStructureComponents":
                    for child in element.iterchildren():
                        child_name = lxml.etree.QName(child.tag).localname
                        if child_name == "DimensionList":                        
                            self.process_dimension_list(child)
                        elif child_name == "Group":
                            pass                        
                        elif child_name == "AttributeList":
                            self.process_attribute_list(child)                        
                        elif child_name == "MeasureList":
                            pass
                  
            #element.clear()


class XMLDataBase:
    
    NS_TAG_DATA = None
    PROVIDER_NAME = None

    def __init__(self, 
                 provider_name=None,
                 dataset_code=None,
                 ns_tag_data=None,
                 field_frequency="FREQ", 
                 field_obs_time_period="TIME_PERIOD",
                 field_obs_value="OBS_VALUE",
                 dimension_keys=None,
                 frequencies_supported=None, 
                 frequencies_rejected=None):
        
        self.provider_name = provider_name or self.PROVIDER_NAME
        self.dataset_code = dataset_code
        self.dimension_keys = dimension_keys or []
        self.frequencies_supported = frequencies_supported
        self.frequencies_rejected = frequencies_rejected

        self.field_frequency = field_frequency
        self.field_obs_time_period = field_obs_time_period
        self.field_obs_value = field_obs_value

        self.nsmap = {}
        self.tree_iterator = None
        
        self.ns_tag_data = ns_tag_data or self.NS_TAG_DATA
        
    def _get_nsmap(self, tree_iterator):
        return get_nsmap(tree_iterator)
        
    def _load_data(self, filepath):
        self.tree_iterator = lxml.etree.iterparse(filepath, events=['end', 'start-ns'])
        self.nsmap = self._get_nsmap(self.tree_iterator)

    def fixtag(self, ns, tag):
        if not ns in self.nsmap:
            raise Exception("Namespace not found[%s] - tag[%s] - provider[%s] - nsmap[%s]" %(ns,
                                                                                             tag, 
                                                                                             self.provider_name,
                                                                                             self.nsmap))
        return '{' + self.nsmap[ns] + '}' + tag


    def get_series_tag(self):
        return self.fixtag(self.ns_tag_data, 'Series')

    def process(self, filepath):
        self._load_data(filepath)
        
        for event, element in self.tree_iterator:
            if event == 'end':
                if element.tag == self.get_series_tag():
                    try:
                        yield self.one_series(element)
                    except errors.RejectFrequency as err:
                        raise
                    except errors.RejectEmptySeries:
                        raise
                    finally:
                        element.clear()

    def get_dimensions(self, series):
        raise NotImplementedError()

    def get_name(self, series, dimensions):
        raise NotImplementedError()

    def get_key(self, series, dimensions):
        raise NotImplementedError()

    def fixe_frequency(self, frequency, series, dimensions):
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
    
    def get_frequency(self, series, dimensions):
        frequency = self.search_frequency(series, dimensions)
        frequency = self.fixe_frequency(frequency, series, dimensions)
        self.valid_frequency(frequency, series, dimensions)
        return frequency

    def search_frequency(self, series, dimensions):
        raise NotImplementedError()

    def get_observations(self, series, frequency):
        raise NotImplementedError()

    def get_last_update(self, series, dimensions, bson=None):
        return None

    def start_date(self, series, frequency, observations=[], bson=None):
        _date = observations[0]["period"]
        return pandas.Period(_date, freq=frequency).ordinal

    def end_date(self, series, frequency, observations=[], bson=None):
        _date = observations[-1]["period"]
        return pandas.Period(_date, freq=frequency).ordinal

    def _finalize_bson_v1(self, bson):
        #print("---------------------------------------------------------------")
        #pprint(bson)
        #print("---------------------------------------------------------------")

        observations = bson.pop("observations")
        bson.pop("series_keys")
        bson.pop("series_attributes")
        
        bson["values"] = [obs["value"] for obs in observations]
        
        attributes = {}
        for obs in observations:
            for key, value in obs["attributes"].items():
                if not key in attributes:
                    attributes[key] = []
                attributes[key].append(value)
        
        bson["attributes"] = attributes
        
        return bson

    def _finalize_bson_v2(self, bson):
        bson.pop("series_keys")
        bson.pop("series_attributes")
        bson["values"] = bson.pop("observations")
        
        last_update = bson.pop("last_update")
        if not last_update:
            last_update = datetime.now()
            
        for obs in bson["values"]:
            obs["last_update"] = last_update 
        
        return bson

    def finalize_bson(self, bson):
        return self._finalize_bson_v1(bson)

    def build_series(self, series):
        raise NotImplementedError()

    def one_series(self, series):
        bson = self.build_series(series)
        return self.finalize_bson(bson)

class XMLCompactData_2_0(XMLDataBase):
    """SDMX 2.1 application/vnd.sdmx.genericdata+xml;version=2.1
    http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message
    """

    NS_TAG_DATA = "data"    

    def get_name(self, series, dimensions):
        return "-".join(series.attrib.values())

    def get_key(self, series, dimensions):
        return ".".join(series.attrib.values())
    
    def search_frequency(self, series, dimensions):
        return series.attrib[self.field_frequency]

    def get_dimensions(self, series):
        return OrderedDict([(k, v) for k, v in series.attrib.items()])

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
                item["original_period"] = item["period"]
                item["ordinal"] = pandas.Period(item["period"], freq=frequency).ordinal
                item["value"] = obs.attrib["OBS_VALUE"]
                
                for key, value in obs.attrib.items():
                    #TODO: if not key in [self.field_obs_time_period, self.field_obs_value]:
                    if not key in ['TIME_PERIOD', 'OBS_VALUE']:
                        item["attributes"][key] = value
                
                observations.append(item)

        return observations    
    
    def build_series(self, series):
        dimensions = self.get_dimensions(series)
        frequency = self.get_frequency(series, dimensions)
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
            "name": self.get_name(series, dimensions),
            "key": self.get_key(series, dimensions), 
            "observations": observations,
            "series_keys": {},
            "series_attributes": {},
        }
        bson["start_date"] = self.start_date(series, frequency, observations, bson)
        bson["end_date"] = self.end_date(series, frequency, observations, bson)
        bson["last_update"] = self.get_last_update(series, dimensions, bson)
        
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
        return pandas.Period(date_string, freq=freq).ordinal
        
    def end_date(self, series, frequency, observations=[], bson=None):
        time_format = series.attrib.get('TIME_FORMAT')
        if not time_format or not time_format in SPECIAL_DATE_FORMATS:
            return super().start_date(series, frequency, observations=observations, bson=bson)

        period = observations[-1]["period"]        
        (date_string, freq) = parse_special_date(period, time_format, self.dataset_code)
        return pandas.Period(date_string, freq=freq).ordinal

        
class XMLData_1_0(XMLDataBase):
    """SDMX 1.0
    http://www.SDMX.org/resources/SDMXML/schemas/v1_0/message
    """

    NS_TAG_DATA = "frb"

    def process(self, filepath):
        self._load_data(filepath)
        
        count_reject = 0
        for event, element in self.tree_iterator:
            if event == 'end':
                localname = lxml.etree.QName(element.tag).localname
                if localname == "Series": 
                    try:
                        yield self.one_series(element)
                    except errors.RejectFrequency as err:
                        count_reject += 1
                        raise
                    except errors.RejectEmptySeries:
                        count_reject += 1
                        raise
                    finally:
                        element.clear()

    def get_dimensions(self, series):
        return OrderedDict([(k, v) for k, v in series.attrib.items()])

    def get_observations(self, series, frequency):
        observations = []

        for obs in series.iterchildren():
            item = {"period": None, "value": None, "attributes": {}}
            
            if obs.tag == self.fixtag(self.ns_tag_data, 'Obs'):
                item["period"] = obs.attrib["TIME_PERIOD"]
                item["original_period"] = item["period"]
                item["ordinal"] = pandas.Period(item["period"], freq=frequency).ordinal
                item["value"] = obs.attrib["OBS_VALUE"]
                
                for key, value in obs.attrib.items():
                    if not key in ['TIME_PERIOD', 'OBS_VALUE']:
                        item["attributes"][key] = value
                
                observations.append(item)

        return observations    
    
    def build_series(self, series):
        dimensions = self.get_dimensions(series)
        frequency = self.get_frequency(series, dimensions)
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
            "name": self.get_name(series, dimensions),
            "key": self.get_key(series, dimensions), 
            "observations": observations,
            "series_keys": {},
            "series_attributes": {},
        }
        bson["start_date"] = self.start_date(series, frequency, observations, bson)
        bson["end_date"] = self.end_date(series, frequency, observations, bson)
        bson["last_update"] = self.get_last_update(series, dimensions, bson)
        
        return bson

    
class XMLData_1_0_FED(XMLData_1_0):

    PROVIDER_NAME = "FED"
    NS_TAG_DATA = "frb"
     
    _frequency_map = {
        "129": "M",   #FIXME: <frb:Obs OBS_STATUS="A" OBS_VALUE="10.20" TIME_PERIOD="1972-02-29" />
        "203": "A", 
        "162": "Q",
        "8": "D",
    }
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.frequencies_supported = list(self._frequency_map.keys()) + ["M"]     

    def _get_nsmap(self, iterator):
        return {'common': 'http://www.SDMX.org/resources/SDMXML/schemas/v1_0/common',
                'frb': 'http://www.federalreserve.gov/structure/compact/common',
                'kf': 'http://www.federalreserve.gov/structure/compact/G19_TERMS',
                'message': 'http://www.SDMX.org/resources/SDMXML/schemas/v1_0/message',
                'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}        

    def get_dimensions(self, series):
        return OrderedDict([(k, v) for k, v in series.attrib.items() if not k in ["SERIES_NAME"]])

    def get_name(self, series, dimensions):
        return series.attrib['SERIES_NAME']

    def get_key(self, series, dimensions):
        return series.attrib['SERIES_NAME']

    def search_frequency(self, series, dimensions):
        return series.attrib[self.field_frequency]

    def fixe_frequency(self, frequency, series, dimensions):
        if frequency in self._frequency_map:
            return self._frequency_map[frequency]
        return frequency



class XMLGenericData_2_1(XMLDataBase):
    """SDMX 2.1 application/vnd.sdmx.genericdata+xml;version=2.1
    """

    NS_TAG_DATA = "generic"
    
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
    
    def get_name(self, series, dimensions):
        raise NotImplementedError()

    def get_key(self, series, dimensions):
        raise NotImplementedError()

    def search_frequency(self, series, dimensions):
        return dimensions[self.field_frequency]

    def get_observations(self, series, frequency):
        
        observations = []
        
        for element in series.xpath("child::%s:Obs" % self.ns_tag_data, namespaces=self.nsmap):

            item = {"period": None, "value": None, "attributes": {}}
            for child in element.getchildren():
                
                if child.tag == self.fixtag(self.ns_tag_data, 'ObsDimension'):
                    item["period"] = child.attrib["value"]
                    item["original_period"] = item["period"]
                    item["ordinal"] = pandas.Period(item["period"], freq=frequency).ordinal
                
                elif child.tag == self.fixtag(self.ns_tag_data, 'ObsValue'):
                    item["value"] = child.attrib["value"]
                
                elif child.tag == self.fixtag(self.ns_tag_data, 'Attributes'):
                    for key, value in self._get_values(child).items():
                        item["attributes"][key] = value
                
                child.clear()
            
            observations.append(item)
            element.clear()
            
        return observations

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
        dimensions = OrderedDict()
        
        el_series_keys = series.xpath("child::%s:SeriesKey" % self.ns_tag_data, 
                                      namespaces=self.nsmap)[0]

        el_series_attrs = series.xpath("child::%s:Attributes" % self.ns_tag_data, 
                                      namespaces=self.nsmap)[0]
        
        series_keys = self._get_values(el_series_keys)
        dimensions.update(series_keys)

        series_attributes = self._get_values(el_series_attrs)
        dimensions.update(series_attributes)            
        
        frequency = self.get_frequency(series, dimensions)
        observations = self.get_observations(series, frequency)
        
        bson = {
            "provider_name": self.provider_name,
            "dataset_code": self.dataset_code,
            "dimensions": dimensions,
            "frequency": frequency,
            "name": self.get_name(series, dimensions),
            "key": self.get_key(series, dimensions), 
            "observations": observations,
            "series_keys": series_keys,
            "series_attributes": series_attributes,
        }
        bson["start_date"] = self.start_date(series, frequency, observations, bson)
        bson["end_date"] = self.end_date(series, frequency, observations, bson)
        bson["last_update"] = self.get_last_update(series, dimensions, bson)
        
        return bson
    
class XMLGenericData_2_1_ECB(XMLGenericData_2_1):
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
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.frequencies_supported = ["A", "M", "Q", "W", "D"]
    
    def get_name(self, series, dimensions):
        #TODO: is not TITLE_COMPL
        return dimensions["TITLE_COMPL"]

    def get_key(self, series, dimensions):
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
    

class XMLGenericData_2_1_INSEE(XMLGenericData_2_1):

    PROVIDER_NAME = "INSEE"
    
    def get_name(self, series, dimensions):
        return dimensions["TITLE"]

    def get_key(self, series, dimensions):
        return dimensions["IDBANK"]

    def fixe_frequency(self, frequency, series, dimensions):
        if frequency == "T":
            #TODO: T equal Trimestrial for INSEE
            frequency = "Q"
            logger.warning("Replace T frequency by Q - dataset[%s] - idbank[%s]" % (self.dataset_code, dimensions["IDBANK"]))
        return frequency

    def get_last_update(self, series, dimensions, bson=None):
        return datetime.strptime(dimensions["LAST_UPDATE"], "%Y-%m-%d")

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

class XMLSpecificData_2_1(XMLDataBase):
    """SDMX 2.1 application/vnd.sdmx.structurespecificdata+xml;version=2.1    
    """

    def process(self, filepath):
        self._load_data(filepath)
        
        for event, element in self.tree_iterator:
            if event == 'end':
                if element.tag == 'Series':
                    try:
                        yield self.one_series(element)
                    except errors.RejectFrequency:
                        print("REJECT RejectFrequency !!!!!")
                        raise
                    except errors.RejectEmptySeries:
                        print("REJECT RejectEmptySeries !!!!!")
                        raise
                    finally:
                        element.clear()

    def get_name(self, series, dimensions):
        raise NotImplementedError()

    def get_key(self, series, dimensions):
        raise NotImplementedError()

    def search_frequency(self, series, dimensions):
        return dimensions[self.field_frequency]

    def get_dimensions(self, series):
        return OrderedDict([(k, v) for k, v in series.attrib.items()])

    def get_observations(self, series, frequency):
        
        observations = []

        for observation in series.iterchildren():

            item = {"period": None, "value": None, "attributes": {}}
            
            item["period"] = observation.attrib[self.field_obs_time_period]
            item["original_period"] = item["period"] 
            item["ordinal"] = pandas.Period(item["period"], freq=frequency).ordinal
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
        frequency = self.get_frequency(series, dimensions)
        observations = self.get_observations(series, frequency)
        
        bson = {
            "provider_name": self.provider_name,
            "dataset_code": self.dataset_code,
            "dimensions": dimensions,
            "frequency": frequency,
            "name": self.get_name(series, dimensions),
            "key": self.get_key(series, dimensions), 
            "observations": observations,
            "series_keys": {},
            "series_attributes": {},
        }
        bson["start_date"] = self.start_date(series, frequency, observations, bson)
        bson["end_date"] = self.end_date(series, frequency, observations, bson)
        bson["last_update"] = self.get_last_update(series, dimensions, bson)
        
        return bson

class XMLSpecificData_2_1_ECB(XMLSpecificData_2_1):

    PROVIDER_NAME = "ECB"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.frequencies_supported = ["A", "M", "Q", "W", "D"]
    
    def get_name(self, series, dimensions):
        return dimensions["TITLE_COMPL"]

    def get_key(self, series, dimensions):
        return ".".join([dimensions[key] for key in self.dimension_keys])

class XMLSpecificData_2_1_INSEE(XMLSpecificData_2_1):

    PROVIDER_NAME = "INSEE"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.frequencies_supported = ["A", "M", "Q", "W", "D"]
    
    def get_name(self, series, dimensions):
        return dimensions["TITLE"]

    def get_key(self, series, dimensions):
        return dimensions["IDBANK"]

    def fixe_frequency(self, frequency, series, dimensions):
        if frequency == "T":
            #TODO: T equal Trimestrial for INSEE
            frequency = "Q"
            logger.warning("Replace T frequency by Q - dataset[%s] - idbank[%s]" % (self.dataset_code, dimensions["IDBANK"]))
        return frequency

    def get_last_update(self, series, dimensions, bson=None):
        return datetime.strptime(dimensions["LAST_UPDATE"], "%Y-%m-%d")

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


    
"""
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
                
