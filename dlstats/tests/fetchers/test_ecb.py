import unittest
from unittest.mock import MagicMock, patch, Mock
from dlstats.fetchers import ecb
from dlstats import constants
from dlstats.tests.base import BaseDBTestCase
import pickle
import pkgutil
import sdmx
import datetime

CATEGORIES = {
    'name': 'Concepts',
    'subcategories':[
        {'name': 'Example subcategory 1',
         'subcategories': [
             {'name': 'Example subcategory 1_1',
              'flowrefs': ['1_1_1']},
             {'name': 'Example subcategory 1_2',
              'flowrefs': ['1_2_1','1_2_2']}
         ]},
        {'name': 'Example subcategory 2',
         'subcategories': [
             {'name': 'Example subcategory 2_1'},
             {'name': 'Example subcategory 2_2',
              'flowrefs': ['2_2_1']}
         ]}
    ]}

DATAFLOWS = dict()
DATAFLOWS['1_1_1'] = {'WDK_TEST': ('WDK', '1.0', {'en': 'Name of 1_1_1'})}
DATAFLOWS['1_2_1'] = {'WDK_TEST': ('WDK', '1.0', {'en': 'Name of 1_2_1'})}
DATAFLOWS['1_2_2'] = {'WDK_TEST': ('WDK', '1.0', {'en': 'Name of 1_2_2'})}
DATAFLOWS['2_2_1'] = {'WDK_TEST': ('WDK', '1.0', {'en': 'Name of 2_2_1'})}

def dataflows(flowref):
    return DATAFLOWS[flowref]

def get_categories(self):
    return CATEGORIES


class ECBCategoriesDBTestCase(BaseDBTestCase):
    def setUp(self):
        BaseDBTestCase.setUp(self)
        self.fetcher = ecb.ECB(db=self.db)
    @patch('sdmx.ecb.dataflows',dataflows)
    @patch('dlstats.fetchers.ecb.ECB.get_categories',get_categories)
    def test_categories(self):
        self.maxDiff = None
        reference = [{'docHref': None,
                      'lastUpdate': datetime.datetime(2014, 12, 2, 0, 0),
                      'name': 'Concepts',
                      'exposed': True,
                      'provider': 'ECB', 
                      'categoryCode': 'Concepts'},
                     {'docHref': None,
                      'lastUpdate': datetime.datetime(2014, 12, 2, 0, 0),
                      'name': 'Example subcategory 1',
                      'exposed': True,
                      'provider': 'ECB',
                      'categoryCode': 'Example subcategory 1'},
                     {'docHref': None,
                      'lastUpdate': datetime.datetime(2014, 12, 2, 0, 0),
                      'name': 'Example subcategory 1_1',
                      'exposed': True,
                      'provider': 'ECB',
                      'categoryCode': 'Example subcategory 1_1'},
                     {'docHref': None,
                      'lastUpdate': datetime.datetime(2014, 12, 2, 0, 0),
                      'name': 'Example subcategory 1_2',
                      'exposed': True,
                      'provider': 'ECB',
                      'categoryCode': 'Example subcategory 1_2'},
                     {'docHref': None,
                      'lastUpdate': datetime.datetime(2014, 12, 2, 0, 0),
                      'name': 'Example subcategory 2',
                      'exposed': True,
                      'provider': 'ECB',
                      'categoryCode': 'Example subcategory 2'},
                     {'docHref': None,
                      'lastUpdate': datetime.datetime(2014, 12, 2, 0, 0),
                      'name': 'Example subcategory 2_2',
                      'exposed': True,
                      'provider': 'ECB',
                      'categoryCode': 'Example subcategory 2_2'},
                     {'docHref': None,
                      'lastUpdate': datetime.datetime(2014, 12, 2, 0, 0),
                      'name': 'Name of 2_2_1',
                      'exposed': True,
                      'provider': 'ECB',
                      'categoryCode': 'WDK_TEST'}]
        self.fetcher.upsert_categories()
        #We exclude ObjectIDs because their values are not determinitstic. We
        #can't test these elements
        results = self.db[constants.COL_CATEGORIES].find(
            {"provider": self.fetcher.provider_name},
            {'_id': False, 'children': False})
        results = [result for result in results]
        self.assertEqual(results, reference)

if __name__ == '__main__':
    unittest.main()
    #print(generate_datasets_sdmx())
