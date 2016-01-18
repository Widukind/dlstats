# -*- coding: utf-8 -*-

from pprint import pprint
import time
import os

import unittest

from dlstats import errors
from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR, BaseTestCase

from dlstats.xml_utils import (XMLSDMX_2_1,
                               XMLData_1_0,
                               XMLData_1_0_FED,
                               XMLStructure_2_0,
                               XMLCompactData_2_0,
                               XMLCompactData_2_0_EUROSTAT,
                               XMLCompactData_2_0_DESTATIS, 
                               XMLStructure_2_1,
                               XMLGenericData_2_1, 
                               XMLGenericData_2_1_ECB,
                               XMLGenericData_2_1_INSEE,
                               XMLSpecificData_2_1, 
                               XMLSpecificData_2_1_ECB,
                               XMLSpecificData_2_1_INSEE)

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "xmlutils"))


SAMPLES_DSD_2_0 = {
    "EUROSTAT": {
        "provider": "EUROSTAT",
        "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "eurostat", "eurostat-datastructure-2.0.xml")),
        "dataset_code": "namq_10_lp_ulc",
        "dsd_id": "namq_10_lp_ulc",
        "dimension_keys": ["FREQ", "s_adj", "unit", "na_item", "geo", "TIME_FORMAT"],
        "dimension_count": None,
        "attribute_keys": [],
        "attribute_count": None,
        "codelist_keys": [],
        "codelist_count": None
    },
    "DESTATIS": {
        "provider": "DESTATIS",
        "filepath": None,
        "dataset_code": "DCS",
        "dsd_id": "DCS",
        "dimension_keys": [],#"FREQ", "DATA_DOMAIN", "REF_AREA", "INDICATOR", "COUNTERPART_AREA"],
        "dimension_count": None,
        "attribute_keys": [],
        "attribute_count": None,
        "codelist_keys": [],
        "codelist_count": None
    }                   
}

SAMPLES_DSD_2_1 = {
    "ECB": {
        "provider": "ECB",
        "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "ecb", "ecb-datastructure-2.1.xml")),
        "dataset_code": "EXR",
        "dsd_id": "ECB_EXR1",
        "dimension_keys": ['FREQ', 'CURRENCY', 'CURRENCY_DENOM', 'EXR_TYPE', 'EXR_SUFFIX'],
        "dimension_count": {
            'CURRENCY': 349,
            'CURRENCY_DENOM': 349,
            'EXR_SUFFIX': 6,
            'EXR_TYPE': 36,
            'FREQ': 10
        },
        "attribute_keys": ['TIME_FORMAT', 'OBS_STATUS', 'OBS_CONF', 'OBS_PRE_BREAK', 'OBS_COM', 'BREAKS', 'COLLECTION', 'DOM_SER_IDS', 'PUBL_ECB', 'PUBL_MU', 'PUBL_PUBLIC', 'UNIT_INDEX_BASE', 'COMPILATION', 'COVERAGE', 'DECIMALS', 'NAT_TITLE', 'SOURCE_AGENCY', 'SOURCE_PUB', 'TITLE', 'TITLE_COMPL', 'UNIT', 'UNIT_MULT'],
        "attribute_count": {
            "TIME_FORMAT": 0,
            "OBS_STATUS": 17,
            "OBS_CONF": 4,
            "OBS_PRE_BREAK": 0,
            "OBS_COM": 0,
            "BREAKS": 0,
            "COLLECTION": 10,
            "DOM_SER_IDS": 0,
            "PUBL_ECB": 0,
            "PUBL_MU": 0,
            "PUBL_PUBLIC": 0,
            "UNIT_INDEX_BASE": 0,
            "COMPILATION": 0,
            "COVERAGE": 0,
            "DECIMALS": 16,
            "NAT_TITLE": 0,
            "SOURCE_AGENCY": 893,
            "SOURCE_PUB": 0,
            "TITLE": 0,
            "TITLE_COMPL": 0,
            "UNIT": 330,
            "UNIT_MULT": 11
        }, 
        "codelist_keys": ['CL_COLLECTION', 'CL_CURRENCY', 'CL_DECIMALS', 'CL_EXR_SUFFIX', 'CL_EXR_TYPE', 'CL_FREQ', 'CL_OBS_CONF', 'CL_OBS_STATUS', 'CL_ORGANISATION', 'CL_UNIT', 'CL_UNIT_MULT'],
        "codelist_count": {
            'CL_COLLECTION': 10,
            'CL_CURRENCY': 349,
            'CL_DECIMALS': 16,
            'CL_EXR_SUFFIX': 6,
            'CL_EXR_TYPE': 36,
            'CL_FREQ': 10,
            'CL_OBS_CONF': 4,
            'CL_OBS_STATUS': 17,
            'CL_ORGANISATION': 893,
            'CL_UNIT': 330,
            'CL_UNIT_MULT': 11
        }      
    },
    "INSEE": {
        "provider": "INSEE",
        "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "insee", "insee-datastructure-2.1.xml")),
        "dataset_code": "IPI-2010-A21",
        "dsd_id": "IPI-2010-A21",
        "dimension_keys": ['FREQ', 'PRODUIT', 'NATURE'],
        "dimension_count": {
            'FREQ': 7, 
            'NATURE': 25,
            'PRODUIT': 30
        },
        "attribute_keys": ['IDBANK', 'TITLE', 'LAST_UPDATE', 'UNIT_MEASURE', 'UNIT_MULT', 'REF_AREA', 'DECIMALS', 'BASE_PER', 'TIME_PER_COLLECT', 'OBS_STATUS', 'EMBARGO_TIME'],      
        "attribute_count": {
            'IDBANK': 0, 
            'TITLE': 0, 
            'LAST_UPDATE': 0,
            'UNIT_MEASURE': 123, 
            'UNIT_MULT': 0, 
            'REF_AREA': 11, 
            'DECIMALS': 0,
            'BASE_PER': 0, 
            'TIME_PER_COLLECT': 7, 
            'OBS_STATUS': 10, 
            'EMBARGO_TIME': 0, 
        },
        "codelist_keys": ['CL_FREQ', 'CL_NAF2_A21', 'CL_NATURE', 'CL_UNIT', 'CL_AREA', 'CL_TIME_COLLECT', 'CL_OBS_STATUS'],
        "codelist_count": {
            'CL_FREQ': 7, 
            'CL_NAF2_A21': 30, 
            'CL_NATURE': 25,
            'CL_UNIT': 123,
            'CL_AREA': 11,
            'CL_TIME_COLLECT': 7,
            'CL_OBS_STATUS': 10,
        },
    },
}

SAMPLES_DATA_1_0 = {
    
    "FED": {
        "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "fed", "fed-data-1.0.xml")),
        "klass": XMLData_1_0_FED,
        "kwargs": {
            "provider_name": "FED",
            "dataset_code": "G19",
            "field_frequency": "FREQ",
            "dimension_keys": None, #SAMPLES_DSD_1_0["FED"]["dimension_keys"],
            "frequencies_supported": ["M", "A", "D", "Q"]
        },
        "series_sample": {
            'provider_name': 'FED',
            'dataset_code': 'G19',
            'key': 'RIFLPBCIANM48_N.M',
            'name': 'RIFLPBCIANM48_N.M',
            'frequency': '129',
            'start_date': 104,
            'end_date': 182,
            'last_update': None,
            'values_first': '89.4',
            'values_last': '106.1',
            'dimensions': {
                'FREQ': 'Q',
                's_adj': 'NSA',
                'unit': 'I10',
                'na_item': 'NULC_HW',
                'geo': 'AT',
                'TIME_FORMAT': 'P3M'
            },
            'attributes': None
        }
    }
}


SAMPLES_DATA_COMPACT_2_0 = {
    
    "EUROSTAT": {
        "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "eurostat", "eurostat-data-compact-2.0.xml")),
        "klass": XMLCompactData_2_0_EUROSTAT,
        "kwargs": {
            "provider_name": "EUROSTAT",
            "dataset_code": "namq_10_lp_ulc",
            "field_frequency": "FREQ",
            "dimension_keys": SAMPLES_DSD_2_0["EUROSTAT"]["dimension_keys"],
            "frequencies_supported": [] #TODO: specific: P1Y (A), P3M (Q), P1M (M), P1D (D)
        },
        "series_sample": {
            'provider_name': 'EUROSTAT',
            'dataset_code': 'namq_10_lp_ulc',
            'key': 'Q.NSA.I10.NULC_HW.AT.P3M',
            'name': 'Q-NSA-I10-NULC_HW-AT-P3M',
            'frequency': 'Q',
            'start_date': 104,
            'end_date': 182,
            'last_update': None,
            'values_first': '89.4',
            'values_last': '106.1',
            'dimensions': {
                'FREQ': 'Q',
                's_adj': 'NSA',
                'unit': 'I10',
                'na_item': 'NULC_HW',
                'geo': 'AT',
                'TIME_FORMAT': 'P3M'
            },
            'attributes': None
        }
    },
    "DESTATIS": {
        "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "destatis", "destatis-data-compact-2.0.xml")),
        "klass": XMLCompactData_2_0_DESTATIS,
        "kwargs": {
            "provider_name": "DESTATIS",
            "dataset_code": "DCS",
            "field_frequency": "FREQ",
            "dimension_keys": SAMPLES_DSD_2_0["DESTATIS"]["dimension_keys"],
            "frequencies_supported": ["A", "D", "M", "Q", "W"]
        },
        "series_sample": {
            'provider_name': 'DESTATIS',
            'dataset_code': 'DCS',
            'frequency': 'M',
            'key': 'M.DCS.DE.FM1_EUR.U2',
            'name': 'M-DCS-DE-FM1_EUR-U2',
            'start_date': 380,
            'end_date': 550,
            'values_first': '593935',
            'values_last': '1789542',
            'dimensions': {
                'FREQ': 'M',
                'DATA_DOMAIN': 'DCS',
                'REF_AREA': 'DE',
                'INDICATOR': 'FM1_EUR',
                'COUNTERPART_AREA': 'U2'
            },
            'sample_attributes': {
                'OBS_STATUS': 'A',
            },
        }
    }
}

SAMPLES_DATA_GENERIC_2_1 = {
    "ECB": {
        "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "ecb", "ecb-data-generic-2.1.xml")),
        "klass": XMLGenericData_2_1_ECB,
        "kwargs": {
            #"provider_name": "ECB",
            "dataset_code": "EXR",
            "field_frequency": "FREQ",
            "dimension_keys": SAMPLES_DSD_2_1["ECB"]["dimension_keys"],
            "frequencies_supported": ["A", "D", "M", "Q", "W"]
        },
        "series_sample": {
            'key': 'M.NOK.EUR.SP00.A',
            'name': 'ECB reference exchange rate, Norwegian krone/Euro, 2:15 pm (C.E.T.)',
            'frequency': 'M',
            'start_date': 348,
            'end_date': 551,
            'values_first': '8.651225',
            'values_last': '9.464159090909094',
            'dimensions': {
                'COLLECTION': 'A',
                'CURRENCY': 'NOK',
                'CURRENCY_DENOM': 'EUR',
                'DECIMALS': '4',
                'EXR_SUFFIX': 'A',
                'EXR_TYPE': 'SP00',
                'FREQ': 'M',
                'SOURCE_AGENCY': '4F0',
                'TITLE': 'Norwegian krone/Euro',
                'TITLE_COMPL': 'ECB reference exchange rate, Norwegian krone/Euro, 2:15 pm (C.E.T.)',
                'UNIT': 'NOK',
                'UNIT_MULT': '0'
            },
            'attributes': None
        }      
    },
    "INSEE" : {
        "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "insee", "insee-data-generic-2.1.xml")),
        "klass": XMLGenericData_2_1_INSEE,
        "kwargs": {
            "provider_name": "INSEE",
            "dataset_code": "IPI-2010-A21",
            "field_frequency": "FREQ",
            "dimension_keys": SAMPLES_DSD_2_1["INSEE"]["dimension_keys"],
            "frequencies_rejected": ["S", "B", "I"]
        },
        "series_sample": {
            "provider_name": "INSEE",
            'key': '001654489',
            'name': 'Indice brut de la production industrielle (base 100 en 2010) - Industries extractives (NAF rév. 2, niveau section, poste B)',
            'start_date': 240,
            'end_date': 550,
            'frequency': 'M',
            'values_first': '139.22',
            'values_last': '96.98',
            'dimensions': {
               'BASE_PER': '2010',
               'DECIMALS': '2',
               'FREQ': 'M',
               'IDBANK': '001654489',
               'LAST_UPDATE': '2016-01-08',
               'NATURE': 'BRUT',
               'PRODUIT': 'B',
               'REF_AREA': 'FM',
               'TIME_PER_COLLECT': 'PERIODE',
               'TITLE': 'Indice brut de la production industrielle (base 100 en 2010) - Industries extractives (NAF rév. 2, niveau section, poste B)',
               'UNIT_MEASURE': 'SO',
               'UNIT_MULT': '0'
            },
            'attributes': None
        }
    }
}

SAMPLES_DATA_SPECIFIC_2_1 = {
    "ECB": {
        "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "ecb", "ecb-data-specific-2.1.xml")),
        "klass": XMLSpecificData_2_1_ECB,
        "kwargs": SAMPLES_DATA_GENERIC_2_1["ECB"]["kwargs"],
        "series_sample": SAMPLES_DATA_GENERIC_2_1["ECB"]["series_sample"],
    },
    "INSEE" : {
        "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "insee", "insee-data-specific-2.1.xml")),
        "klass": XMLSpecificData_2_1_INSEE,
        "kwargs": SAMPLES_DATA_GENERIC_2_1["INSEE"]["kwargs"],
        "series_sample": SAMPLES_DATA_GENERIC_2_1["INSEE"]["series_sample"],
    }
}

class XMLUtilsTestCase(BaseTestCase):
    
    # nosetests -s -v dlstats.tests.test_xml_utils:XMLUtilsTestCase
    
    def setUp(self):
        super().setUp()
        
    def _run(self, xml, filepath):
        count = 0
        count_reject_frequencies = 0
        count_values = 0
        start = time.time()
        
        rows = xml.process(filepath)
        
        while True:        
            try:
                series = next(rows)
                count += 1
                count_values += len(series["values"])
                #TODO: retrouver pour chaque dimension, la valeur dans la structure
                print(count, series["key"], len(series["values"]), list(series["dimensions"].keys()))
            except errors.RejectFrequency as err:
                print("reject freq : ", err.frequency)
                count_reject_frequencies += 1
                print(str(err))
            except StopIteration:
                print("StopIteration!!!")
                break
        
        end = time.time() - start
        print("SERIES[%s] - REJECT[%s] - DUREE[%.3f] - VALUES[%s]" % (count, count_reject_frequencies, end, count_values))

    def _debug_datastructure_2_1(self, provider, xml):
        
        print()
        print("provider : ", provider["provider"])
        
        print(xml.dimension_keys)
        dimensions = {}
        for key, dim in xml.dimensions.items():
            dimensions[key] = len(dim["dimensions"].keys())
        pprint(dimensions)
        codes = {}
        print(list(xml.codelists.keys()))
        for key, code in xml.codelists.items():
            codes[key] = len(code["codes"].keys())
        pprint(codes)
        
        print("--------------------------")

    def _assertDatastructure_2_1(self, xml, provider):
        
        self.assertEqual(xml.dsd_id, provider["dsd_id"])
        
        #pprint(xml.dimensions)
        #print("!!!!! : ", list(xml.dimensions.items())[0])
        """
        !!!!! :  ('FREQ', {'dimensions': OrderedDict([('A', 'Annual'), ('T', 'Quarterly'), ('M', 'Monthly'), ('B', 'Two-monthly'), ('S', 'Semi-annual'), ('Q', 'Quarterly'), ('I', 'Irregular')]), 'id': 'FREQ', 'name': 'Frequency'})
        
        dimension_list: {
            'freq': [
                 ['A', 'Annual'],
                 ['S', 'Half-yearly, semester'],
                 ['Q', 'Quarterly'],
                 ['M', 'Monthly'],
                 ['W', 'Weekly'],
                 ['B', 'Business week'],
                 ['D', 'Daily'],
                 ['H', 'Hourly'],
                 ['N', 'Minutely']
            ],
        }
        """
        
        #print("attributes keys : ", list(xml.attributes.keys()))
        self.assertEqual(list(xml.attributes.keys()), provider["attribute_keys"])
        for key in provider["attribute_keys"]:
            #print(key, len(xml.attributes[key]["values"].keys()))
            self.assertEqual(len(xml.attributes[key]["values"].keys()), provider["attribute_count"][key])
        
        self.assertEqual(xml.dimension_keys, provider["dimension_keys"])
        for key in xml.dimension_keys:
            self.assertEqual(len(xml.dimensions[key]["dimensions"].keys()), provider["dimension_count"][key])

        #print(list(xml.codelists.keys()))
        self.assertEqual(list(xml.codelists.keys()), provider["codelist_keys"])
        for key in provider["codelist_keys"]:
            #print(key, len(xml.codelists[key]["codes"].keys()))
            self.assertEqual(len(xml.codelists[key]["codes"].keys()), provider["codelist_count"][key])
        

    def test_datastructure_2_1(self):

        # nosetests -s -v dlstats.tests.test_xml_utils:XMLUtilsTestCase.test_datastructure_2_1
        
        for provider_name, provider in SAMPLES_DSD_2_1.items():
            #if provider_name != "ECB": continue
            print("provider: ", provider_name)
            xml = XMLStructure_2_1(provider_name=provider_name)            
            xml.process(provider["filepath"])
            self._assertDatastructure_2_1(xml, provider)
            #self._debug_datastructure_2_1(provider, xml)
            
    def test_datastructure_2_1_codelist(self):

        # nosetests -s -v dlstats.tests.test_xml_utils:XMLUtilsTestCase.test_datastructure_2_1_codelist

        for provider_name, provider in SAMPLES_DSD_2_1.items():
            #if provider_name != "ECB": continue
            print("provider: ", provider_name)
            xml = XMLStructure_2_1(provider_name=provider_name)
            xml.process(provider["filepath"])
            #obs_status = xml.get_codelist("CL_OBS_STATUS")
            #pprint(obs_status)
            
    def test_dataflow_2_1(self):

        # nosetests -s -v dlstats.tests.test_xml_utils:XMLUtilsTestCase.test_dataflow_2_1
                    
        filepath = os.path.abspath(os.path.join(RESOURCES_DIR, "ecb", "ecb-dataflow-2.1.xml"))
        xml = XMLStructure_2_1(provider_name="ECB", dataset_code="EXR")#, dsd_id="ECB_EXR1")
        xml.process(filepath)
        self.assertEqual(xml.dataflows, {'EXR': 'ECB_EXR1'})
        self.assertEqual(xml.dsd_id, "ECB_EXR1")

    def _assertGenericSeries(self, series, series_samples, provider_name):
        
        self.assertEqual(series["key"], series_samples["key"])
        self.assertEqual(series["name"], series_samples["name"])
        
        self.assertEqual(series["frequency"], series_samples["frequency"])
        self.assertEqual(series["start_date"], series_samples["start_date"])
        self.assertEqual(series["end_date"], series_samples["end_date"])
        
        self.assertEqual(series["values"][0], series_samples["values_first"])
        self.assertEqual(series["values"][-1], series_samples["values_last"])
        
        self.assertEqual(series["dimensions"], series_samples["dimensions"])

        self.assertTrue(series["end_date"] > series["start_date"])
        
        if series["attributes"]:
            for attribute, values in series["attributes"].items():
                #TODO: test first et last value: series_samples["attributes"]
                msg = "bad length for attribute[%s] for provider[%s] - values[%s]" % (attribute, provider_name, values)
                self.assertEqual(len(series["values"]), len(values), msg)
        
    def test_generic_data_2_1(self):
        
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLUtilsTestCase.test_generic_data_2_1
        
        for provider_name, provider in SAMPLES_DATA_GENERIC_2_1.items():
            #if provider_name != "ECB": continue
            print("provider: ", provider_name)
            klass = provider["klass"]
            kwargs = provider["kwargs"]
            xml = klass(**kwargs)
            self.assertIsNotNone(xml.ns_tag_data)
            series_samples = provider.get("series_sample")
            series = next(xml.process(provider["filepath"]))
            #pprint(series)
            self._assertGenericSeries(series, series_samples, provider_name)

    def test_data_1_0(self):
        
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLUtilsTestCase.test_data_1_0
        
        for provider_name, provider in SAMPLES_DATA_1_0.items():
            #if provider_name != "EUROSTAT": continue
            print("provider: ", provider_name)
            klass = provider["klass"]
            kwargs = provider["kwargs"]
            xml = klass(**kwargs)
            self.assertIsNotNone(xml.ns_tag_data)
            #pprint(xml.nsmap)
            print("self.frequencies_supported : ", xml.frequencies_supported)
            
            #self._run(xml, provider["filepath"])
            #pprint(series)
            series_samples = provider.get("series_sample")
            #FIXME: self._assertGenericSeries(series, series_samples, provider_name)
    
    def test_compact_data_2_0(self):
        
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLUtilsTestCase.test_compact_data_2_0
        
        for provider_name, provider in SAMPLES_DATA_COMPACT_2_0.items():
            #if provider_name != "EUROSTAT": continue
            print("provider: ", provider_name)
            klass = provider["klass"]
            kwargs = provider["kwargs"]
            xml = klass(**kwargs)
            self.assertIsNotNone(xml.ns_tag_data)
            series_samples = provider.get("series_sample")
            series = next(xml.process(provider["filepath"]))
            #pprint(series)
            self._assertGenericSeries(series, series_samples, provider_name)
    
    def test_specific_data_2_1(self):
        
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLUtilsTestCase.test_specific_data_2_1
        """
        http https://sdw-wsrest.ecb.europa.eu/service/data/EXR Accept:application/vnd.sdmx.structurespecificdata+xml;version=2.1 > ecb-data-exr.structurespecificdata.xml
        SERIES[10635] - REJECT[44] - DUREE[220.546] - VALUES[2994740]
        """
        for provider_name, provider in SAMPLES_DATA_SPECIFIC_2_1.items():
            #if provider_name != "ECB": continue
            print("provider: ", provider_name)
            klass = provider["klass"]
            kwargs = provider["kwargs"]
            xml = klass(**kwargs)
            series_samples = provider.get("series_sample")
            series = next(xml.process(provider["filepath"]))
            #pprint(series)
            self._assertGenericSeries(series, series_samples, provider_name)
        
        #self._run(xml)


