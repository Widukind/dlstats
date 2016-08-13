# -*- coding: utf-8 -*-

import logging
import traceback
from pprint import pprint
import time
import os

import unittest

from widukind_common import errors

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR, BaseTestCase
from dlstats.tests.resources import xml_samples

from dlstats import xml_utils

logger = logging.getLogger(__name__)

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "xmlutils"))

SAMPLES_DSD_1_0 = {
    'FED': xml_samples.DSD_FED_TERMS,
}

SAMPLES_DSD_2_0 = {
    "EUROSTAT": xml_samples.DSD_EUROSTAT, 
    #"DESTATIS": xml_samples.DSD_DESTATIS,
    "OECD-MEI": xml_samples.DSD_OECD_MEI,
    "OECD-EO": xml_samples.DSD_OECD_EO,
}

SAMPLES_DSD_2_1 = {
    "ECB": xml_samples.DSD_ECB, 
    "INSEE": xml_samples.DSD_INSEE, 
}

SAMPLES_DATA_1_0 = {
    "FED": xml_samples.DATA_FED_TERMS, 
}

SAMPLES_DATA_COMPACT_2_0 = {
    "EUROSTAT": xml_samples.DATA_EUROSTAT, 
    #"DESTATIS": xml_samples.DATA_DESTATIS, 
}

SAMPLES_DATA_GENERIC_2_0 = {
    "OECD-MEI": xml_samples.DATA_OECD_MEI, 
    "OECD-EO": xml_samples.DATA_OECD_EO, 
}

SAMPLES_DATA_GENERIC_2_1 = {
    "ECB": xml_samples.DATA_ECB_GENERIC, 
    "INSEE": xml_samples.DATA_INSEE_GENERIC, 
}

SAMPLES_DATA_SPECIFIC_2_1 = {
    "ECB": xml_samples.DATA_ECB_SPECIFIC,
    "INSEE" : xml_samples.DATA_INSEE_GENERIC
}

class UtilsTestCase(BaseTestCase):
    
    # nosetests -s -v dlstats.tests.test_xml_utils:UtilsTestCase
    
    def test_parse_special_date(self):
        
        # annual
        (string_date,freq) = xml_utils.parse_special_date("2015", "P1Y")
        self.assertEqual(string_date, '2015')
        self.assertEqual(freq, 'A')

        # quarterly
        (string_date,freq) = xml_utils.parse_special_date("1988-Q3", "P3M")
        self.assertEqual(string_date, '1988Q3')
        self.assertEqual(freq, 'Q')

        # monthly
        (string_date,freq) = xml_utils.parse_special_date("2004-10", "P1M")
        self.assertEqual(string_date, '2004-10')
        self.assertEqual(freq, 'M')

        # daily
        (string_date,freq) = xml_utils.parse_special_date("20040906", "P1D")
        self.assertEqual(string_date, '2004-09-06')
        self.assertEqual(freq, 'D')
        
    def test_select_dimension(self):

        # nosetests -s -v dlstats.tests.test_xml_utils:UtilsTestCase.test_select_dimension
        
        print()

        dimensions = {}
        dimension_keys = ["A", "B", "C"]
        dimensions["A"] = {"a1": "a1 lib"}
        dimensions["B"] = {"b1": "b1 lib", "b2": "b2 lib"}
        dimensions["C"] = {"c1": "c1 lib", "c2": "c2 lib", "c3": "c3 lib"}
        
        position, key, dimension_values = xml_utils.select_dimension(dimension_keys, 
                                                                      dimensions, 
                                                                      choice="max")
        self.assertEqual(key, "C")
        self.assertEqual(position, 2)
        self.assertEqual(sorted(dimension_values), ["c1", "c2", "c3"])
        
        position, key, dimension_values = xml_utils.select_dimension(dimension_keys, 
                                                                    dimensions, 
                                                                    choice="min")
        self.assertEqual(key, "A")
        self.assertEqual(position, 0)
        self.assertEqual(sorted(dimension_values), ["a1"])

        
        
        position, key, dimension_values = xml_utils.select_dimension(dimension_keys, 
                                                                    dimensions, 
                                                                    choice="avg")
        self.assertEqual(key, "B")
        self.assertEqual(position, 1)
        self.assertEqual(sorted(dimension_values), ["b1", "b2"])

        
        
        position, key, dimension_values = xml_utils.select_dimension(dimension_keys, 
                                                                    dimensions, 
                                                                    choice=None)
        self.assertEqual(key, "B")
        self.assertEqual(position, 1)
        self.assertEqual(sorted(dimension_values), ["b1", "b2"])



        position, key, dimension_values = xml_utils.select_dimension([], 
                                                                     {}, 
                                                                     choice="min")
        self.assertEqual(key, None)
        self.assertEqual(position, 0)
        self.assertEqual(sorted(dimension_values), [])

class BaseXMLStructureTestCase(BaseTestCase):
    
    XMLStructureKlass = None
    SAMPLES = None
    DEBUG_MODE = False

    def setUp(self):
        super().setUp()
        self.samples = self.SAMPLES
        self.xml_klass = self.XMLStructureKlass
        self.is_debug = self.DEBUG_MODE
        #self.maxDiff = None

    def _debug_agency(self, xml, provider, provider_name):
        self.fail("NotImplemented")
        """
        self.agencies[_id] = {
            'id': _id,
            'name': xml_get_name(element),
            'attrs': dict(element.attrib)
        }        
        """

    def _debug_categoryscheme(self, xml, provider, provider_name):
        print()
        print("-------------- CATEGORIES -----------------------")
        print("PROVIDER: ", provider_name)
        print(list(xml.categories.keys()))
        for cat in xml.categories.values():
            print(cat["id"], cat["name"], xml.iter_parent_category_id(cat))

        if provider["categories_key"]:
            print("-------------------------------------------------")
            print("CATEGORIES FOR THIS DATASET[%s]" % provider["dataset_code"])
            cat = xml.categories.get(provider["categories_key"])
            print(cat["id"], cat["name"], xml.iter_parent_category_id(cat))
            
        print("-------------------------------------------------")
        
    def _debug_categorisation(self, xml, provider, provider_name):
        print()
        print("-------------- CATEGORISATION -------------------")
        print("PROVIDER: ", provider_name)
        print(list(xml.categorisations.keys()))
        for categorisation in xml.categorisations.values():
            dataflow_id = categorisation['dataflow']['id']
            category_id = categorisation['category']['id']
            print(categorisation["name"], dataflow_id, category_id)
        print("-------------------------------------------------")

    def _debug_dataflow(self, xml, provider, provider_name):
        print()
        print("-------------- DATAFLOW -------------------------")
        print("PROVIDER: ", provider_name)
        print(sorted(list(xml.dataflows.keys())))
        for dataflow in xml.dataflows.values():
            pprint(dataflow)
        print("-------------------------------------------------")

    def _debug_datastructure(self, xml, provider, provider_name):
        print()
        print("-------------- DATASTRUCTURE --------------------")
        print("PROVIDER: ", provider_name)
        print(sorted(list(xml.datastructures.keys())))
        for dsd in xml.datastructures.values():
            pprint(dsd)
        print("-------------------------------------------------")
        
        self._debug_dimension(xml, provider, provider_name)
        
        self._debug_attribute(xml, provider, provider_name)
        
        self._debug_dataset(xml, provider, provider_name)
        
    def _debug_conceptscheme(self, xml, provider, provider_name):
        print()
        print("-------------- CONCEPT --------------------------")
        print("PROVIDER: ", provider_name)
        print(sorted(list(xml.concepts.keys())))
        print("-------------------------------------------------")

    def _debug_codelist(self, xml, provider, provider_name):
        print()
        print("-------------- CODELIST -------------------------")
        print("PROVIDER: ", provider_name)
        print(sorted(list(xml.codelists.keys())))
        for key in sorted(xml.codelists.keys()):
            print('"%s": %s,' % (key, len(xml.codelists[key]["enum"])))
        print("-------------------------------------------------")

    def _debug_dimension(self, xml, provider, provider_name):
        print()
        print("-------------- DIMENSION ------------------------")
        dsd_id = provider["dsd_id"]
        print("PROVIDER/DATASET: ", provider_name, dsd_id)
        print(list(xml.dimension_keys_by_dsd[dsd_id]))
        for key in xml.dimension_keys_by_dsd[dsd_id]:
            print('"%s": %s,' % (key, len(xml.dimensions_by_dsd[dsd_id][key]["enum"])))
        print("-------------------------------------------------")

    def _debug_attribute(self, xml, provider, provider_name):
        print()
        print("-------------- ATTRIBUTE ------------------------")
        dsd_id = provider["dsd_id"]
        print("PROVIDER/DATASET: ", provider_name, dsd_id)
        print(list(xml.attribute_keys_by_dsd[dsd_id]))
        for key in xml.attribute_keys_by_dsd[dsd_id]:
            print('"%s": %s,' % (key, len(xml.attributes_by_dsd[dsd_id][key]["enum"])))
        print("-------------------------------------------------")
        
    def _debug_dataset(self, xml, provider, provider_name):
        print()
        print("------------------------ DATASET --------------------------------")
        bson = xml_utils.dataset_converter(xml, provider["dataset_code"])
        pprint(bson, width=120)
        print("-----------------------------------------------------------------")

    def assert_agency(self, xml, provider, provider_name):
        self.fail("NotImplemented")

    def assert_categoryscheme(self, xml, provider, provider_name):
        if not provider.get("categories_key"):
            return
        
        key = provider.get("categories_key")
        if key:
            cat = xml.categories.get(key)
            self.assertIsNotNone(cat)
            '''Parent de cette category'''
            if provider["categories_parents"]:
                self.assertEqual(xml.iter_parent_category_id(cat),
                                 provider["categories_parents"])
                 

    def assert_categorisation(self, xml, provider, provider_name):

        if not provider.get("categorisations_key"):
            return
            
        key = provider["categorisations_key"]
        if key:
            categorisation = xml.categorisations.get(key)
            self.assertIsNotNone(categorisation) 
            
            dataflow_id = categorisation['dataflow']['id']
            category_id = categorisation['category']['id']
            
            #self.assertTrue(category_id in provider["categories_keys"])
            
            if xml.categories:
                self.assertTrue(category_id in xml.categories)
            
            if xml.dataflows:
                self.assertTrue(dataflow_id in xml.dataflows)

    def assert_dataflow(self, xml, provider, provider_name):

        self.assertEqual(provider["dataflow_keys"], sorted(list(xml.dataflows.keys())))
        self.assertEqual(xml.get_dsd_id(provider["dataset_code"]), provider["dsd_id"])
        self.assertEqual(xml.get_dataset_name(provider["dataset_code"]), provider["dataset_name"])        

    def assert_datastructure(self, xml, provider, provider_name):

        self.assertEqual(provider["dsd_ids"], sorted(list(xml.datastructures.keys())))
        self.assert_dimension(xml, provider, provider_name)
        self.assert_attribute(xml, provider, provider_name)

    def assert_conceptscheme(self, xml, provider, provider_name):

        # uniq verify in samples datas
        self.assertEqual(len(provider["concept_keys"]), len(set(provider["concept_keys"])))

        self.assertEqual(sorted(list(xml.concepts.keys())), provider["concept_keys"])

    def assert_codelist(self, xml, provider, provider_name):
        
        # uniq verify in samples datas
        self.assertEqual(len(provider["codelist_keys"]), len(set(provider["codelist_keys"])))        
        self.assertEqual(sorted(list(xml.codelists.keys())), provider["codelist_keys"])

        self.assertEqual(len(provider["codelist_keys"]), len(provider["codelist_count"]))

        for key in provider["codelist_keys"]:
            self.assertEqual(len(xml.codelists[key]["enum"].keys()), 
                             provider["codelist_count"][key],
                             key)
            
    def assert_dimension(self, xml, provider, provider_name):

        dsd_id = provider["dsd_id"]
        dimension_keys = xml.dimension_keys_by_dsd[dsd_id]
        dimensions = xml.dimensions_by_dsd[dsd_id]

        '''unicity'''
        self.assertEqual(len(dimension_keys), len(set(dimension_keys)))        
        self.assertEqual(len(provider["dimension_keys"]), len(set(provider["dimension_keys"])))

        '''equality'''
        self.assertEqual(dimension_keys, provider["dimension_keys"])
        self.assertEqual(len(provider["dimension_count"].keys()), len(provider["dimension_keys"]))

        for key in provider["dimension_keys"]:
            self.assertEqual(len(dimensions[key]["enum"].keys()), 
                             provider["dimension_count"][key],
                             key)
        
        #TODO: codelists and concepts ?
        
    def assert_attribute(self, xml, provider, provider_name):

        dsd_id = provider["dsd_id"]
        attribute_keys = xml.attribute_keys_by_dsd[dsd_id]
        attributes = xml.attributes_by_dsd[dsd_id]

        self.assertEqual(len(attribute_keys), len(set(attribute_keys)))
        self.assertEqual(len(provider["attribute_keys"]), len(set(provider["attribute_keys"])))

        self.assertEqual(attribute_keys, provider["attribute_keys"])
        self.assertEqual(len(provider["attribute_count"].keys()), len(provider["attribute_keys"]))

        for key in provider["attribute_keys"]:
            self.assertEqual(len(attributes[key]["enum"].keys()), 
                             provider["attribute_count"][key],
                             key)
        
    def _commons_tests(self, test_name=None):
        
        assert_method = "assert_%s" % test_name
        debug_method = "_debug_%s" % test_name
        
        for provider_name, provider in self.samples.items():
            
            logger.debug("PROVIDER : %s" % provider_name)
            
            xml = self.xml_klass(provider_name=provider_name)

            if test_name in provider["filepaths"]:
                filepath = provider["filepaths"][test_name]
            else:
                filepath = provider["filepaths"]["datastructure"]
            
            logger.debug("filepath : %s" % filepath)
            
            xml.process(filepath)
            
            if self.is_debug:
                getattr(self, debug_method)(xml, provider, provider_name)
            
            getattr(self, assert_method)(xml, provider, provider_name)
            
    def _test_agency(self):
        self._commons_tests("agency")

    def _test_categoryscheme(self):
        self._commons_tests("categoryscheme")

    def _test_categorisation(self):
        self._commons_tests("categorisation")

    def _test_dataflow(self):
        self._commons_tests("dataflow")
        
    def _test_datastructure(self):
        self._commons_tests("datastructure")

    def _test_conceptscheme(self):
        self._commons_tests("conceptscheme")
            
    def _test_codelist(self):
        self._commons_tests("codelist")

    def _test_dimension(self):
        self._commons_tests("dimension")

    def _test_attribute(self):
        self._commons_tests("attribute")

class XMLStructure_1_0_TestCase(BaseXMLStructureTestCase):
    
    # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_1_0_TestCase

    XMLStructureKlass = xml_utils.XMLStructure_1_0
    SAMPLES = SAMPLES_DSD_1_0
    DEBUG_MODE = False

    def test_conceptscheme(self):
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_1_0_TestCase.test_conceptscheme
        self._test_conceptscheme()
            
    def test_codelist(self):
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_1_0_TestCase.test_codelist
        self._test_codelist()

    def test_dataflow(self):
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_1_0_TestCase.test_dataflow
        self._test_dataflow()
        
    def test_datastructure(self):
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_1_0_TestCase.test_datastructure
        self._test_datastructure()
    
class XMLStructure_2_0_TestCase(BaseXMLStructureTestCase):
    
    # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_2_0_TestCase

    XMLStructureKlass = xml_utils.XMLStructure_2_0
    SAMPLES = SAMPLES_DSD_2_0
    DEBUG_MODE = False

    def test_conceptscheme(self):
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_2_0_TestCase.test_conceptscheme
        self._test_conceptscheme()
            
    def test_codelist(self):
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_2_0_TestCase.test_codelist
        self._test_codelist()
        
    def test_dataflow(self):
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_2_0_TestCase.test_dataflow
        self._test_dataflow()

    def test_datastructure(self):
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_2_0_TestCase.test_datastructure
        self._test_datastructure()    

class XMLStructure_2_1_TestCase(BaseXMLStructureTestCase):
    
    # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_2_1_TestCase

    XMLStructureKlass = xml_utils.XMLStructure_2_1
    SAMPLES = SAMPLES_DSD_2_1
    DEBUG_MODE = False

    """
    TODO:    
    def test_agency(self):
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_2_1_TestCase.test_agency
        self._test_agency()
    """

    def test_categoryscheme(self):
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_2_1_TestCase.test_categoryscheme
        self._test_categoryscheme()

    def test_categorisation(self):
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_2_1_TestCase.test_categorisation
        self._test_categorisation()

    @unittest.skipIf(True, "FIXME")
    def test_dataflow(self):
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_2_1_TestCase.test_dataflow
        self._test_dataflow()
    
    def test_conceptscheme(self):
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_2_1_TestCase.test_conceptscheme
        self._test_conceptscheme()
            
    def test_codelist(self):
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_2_1_TestCase.test_codelist
        self._test_codelist()

    def test_datastructure(self):
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_2_1_TestCase.test_datastructure
        self._test_datastructure()    
    
"""
TODO:
class XMLStructure_2_1_Dataflow_TestCase(BaseXMLStructureTestCase):
    
    # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_2_1_Dataflow_TestCase

    XMLStructureKlass = xml_utils.XMLStructure_2_1
    SAMPLES = SAMPLES_DSD_2_1
    DEBUG_MODE = False

    def test_dataflow(self):
        # nosetests -s -v dlstats.tests.test_xml_utils:XMLStructure_2_1_Dataflow_TestCase.test_dataflow
        self._test_dataflow()
"""

class BaseXMLDataTestCase(BaseTestCase):
    
    SAMPLES = None
    DEBUG_MODE = False

    def setUp(self):
        super().setUp()
        self.samples = self.SAMPLES
        self.is_debug = self.DEBUG_MODE
        self.series_list = []
        self.count_values = 0
        self.errors = []
        self.reject_frequency = 0
        self.reject_empty = 0
        self.rows = None

    def _run(self, xml, filepath):
        start = time.time()
        self.series_list = []
        self.count_values = 0
        self.errors = []
        self.reject_frequency = 0
        self.reject_empty = 0
        self.rows = xml.process(filepath)
        
        while True:        
            try:
                series, err = next(self.rows)
                if not err:
                    self.count_values += len(series["values"])
                    self.series_list.append(series)
                else:
                    if isinstance(err, errors.RejectFrequency):
                        self.reject_frequency += 1
                    elif isinstance(err, errors.RejectEmptySeries):
                        self.reject_empty += 1
                    
                    #TODO: RejectUpdatedSeries
                        
                    self.errors.append(str(err))
            except StopIteration:
                break
            except Exception as err:
                traceback.print_exc()
                self.fail("Not captured exception : %s" % str(err))
        
        end = time.time() - start
        if logger.isEnabledFor(logging.DEBUG):
            tmpl = "PROVIDER[%s] - SERIES[%s] - REJECT-FREQ[%s] - REJECT-EMPTY[%s] - DUREE[%.3f] - VALUES[%s]"
            logger.debug(tmpl % (xml.provider_name,
                          len(self.series_list), 
                          self.reject_frequency, 
                          self.reject_empty, 
                          end, 
                          self.count_values))

    def _debug_series(self, xml, provider, provider_name):
        print()
        print("PROVIDER: ", provider_name)
        print("------------------------------------------------")
        if self.series_list:        
            pprint(self.series_list[0])
        else:
            print("NOT SERIES !!!")
        pprint(xml.dimension_keys)
        pprint(xml.dimensions)
        print("------------------------------------------------")        

    def assert_series(self, xml, provider, provider_name):
                
        self.assertTrue(len(xml.dimension_keys) > 0)
        self.assertTrue(len(xml.dimensions) > 0)
        
        self.assertEqual(provider["series_accept"], len(self.series_list))
        self.assertEqual(provider["series_reject_frequency"], self.reject_frequency)
        self.assertEqual(provider["series_reject_empty"], self.reject_empty)
        
        self.assertEqual(provider["series_all_values"], self.count_values)
        self.assertEqual(self.series_list[0]["key"], provider["series_key_first"])
        self.assertEqual(self.series_list[-1]["key"], provider["series_key_last"])

        series = self.series_list[0]
        series_sample = provider["series_sample"]

        dsd = provider["DSD"]

        if dsd["is_completed"]:        
            for key in series["dimensions"].keys():
                msg = "%s not in %s" % (key, dsd["dimension_keys"])
                self.assertTrue(key in dsd["dimension_keys"], msg)

        self.assertEqual(series["key"], series_sample["key"])
        self.assertEqual(series["name"], series_sample["name"])
        self.assertEqual(series["frequency"], series_sample["frequency"])
        
        self.assertTrue(series["end_date"] >= series["start_date"])
        
        self.assertEqual(series["dimensions"], series_sample["dimensions"])
        self.assertEqual(series["attributes"], series_sample["attributes"])

        #self._assert_series_v2(xml, provider, provider_name, series)
        series_sample = provider["series_sample"]
        first_sample = series_sample["first_value"]
        last_sample = series_sample["last_value"]
        first_value = series["values"][0]
        last_value = series["values"][-1]
        
        for source, target in [(first_value, first_sample), (last_value, last_sample)]:
            self.assertEqual(source["value"], target["value"])
            self.assertEqual(source["period"], target["period"])
            self.assertEqual(source["attributes"], target["attributes"])
            
    def _commons_tests(self, test_name=None):
        
        assert_method = "assert_%s" % test_name
        debug_method = "_debug_%s" % test_name
        
        for provider_name, provider in self.samples.items():
            if not provider.get("filepath"):
                self.fail("not filepath for provider[%s]" % provider_name)
                
            klass = xml_utils.XML_STRUCTURE_KLASS[provider["klass"]]
            xml = klass(**provider["kwargs"])
            
            self._run(xml, provider["filepath"])
            if self.is_debug:
                print()
                getattr(self, debug_method)(xml, provider, provider_name)
            
            getattr(self, assert_method)(xml, provider, provider_name)

    def _test_series(self):
        self._commons_tests("series")


class XMLData_1_0_TestCase(BaseXMLDataTestCase):

    # nosetests -s -v dlstats.tests.test_xml_utils:XMLData_1_0_TestCase
    
    SAMPLES = SAMPLES_DATA_1_0
    DEBUG_MODE = False
    
    def test_series(self):
        self._test_series()

class XMLData_2_0_COMPACT_TestCase(BaseXMLDataTestCase):

    # nosetests -s -v dlstats.tests.test_xml_utils:XMLData_2_0_COMPACT_TestCase
    
    SAMPLES = SAMPLES_DATA_COMPACT_2_0
    DEBUG_MODE = False
    
    def test_series(self):
        self._test_series()

class XMLData_2_0_GENERIC_TestCase(BaseXMLDataTestCase):

    # nosetests -s -v dlstats.tests.test_xml_utils:XMLData_2_0_GENERIC_TestCase
    
    SAMPLES = SAMPLES_DATA_GENERIC_2_0
    DEBUG_MODE = False
    
    def test_series(self):
        self._test_series()

class XMLData_2_1_GENERIC_TestCase(BaseXMLDataTestCase):

    # nosetests -s -v dlstats.tests.test_xml_utils:XMLData_2_1_GENERIC_TestCase
    
    SAMPLES = SAMPLES_DATA_GENERIC_2_1
    DEBUG_MODE = False
    
    def test_series(self):
        self._test_series()

class XMLData_2_1_SPECIFIC_TestCase(BaseXMLDataTestCase):

    # nosetests -s -v dlstats.tests.test_xml_utils:XMLData_2_1_SPECIFIC_TestCase
    
    SAMPLES = SAMPLES_DATA_SPECIFIC_2_1
    DEBUG_MODE = False
    
    def test_series(self):
        self._test_series()

