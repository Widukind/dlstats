import unittest
from unittest.mock import MagicMock, patch, Mock
from dlstats.fetchers import ecb
from dlstats import constants
from dlstats.tests.fetchers.base import BaseDBFetcherTestCase
import pickle
import pkgutil
import sdmx

def generate_categories_sdmx():
    with open('ecb_categories_sdmx_dict.pkl', 'wb') as f:
        pickle.dump(sdmx.ecb.categories, f, pickle.HIGHEST_PROTOCOL)

def generate_datasets_sdmx():
    def walk_dictionnary(dictionnary):
        accumulator = []
        for key in dictionnary:
            if key == 'flowrefs':
                accumulator.extend(dictionnary[key])
            if key == 'subcategories':
                for subdictionnary in dictionnary[key]:
                    accumulator.extend(walk_dictionnary(subdictionnary))
        return accumulator
    flowrefs =  walk_dictionnary(sdmx.ecb.categories)
    for flowref in flowrefs:
        dataflow_definition = sdmx.ecb.dataflows(flowref)
        with open('ecb_dataflows_'+str(flowref)+'.pkl', 'wb') as f:
            pickle.dump(dataflow_definition, f, pickle.HIGHEST_PROTOCOL)

def dataflows(flowref):
    output = pickle.loads(pkgutil.get_data('dlstats', 'tests/resources/ecb/ecb_dataflows_'+flowref+'.pkl'))
    return output

def get_categories(self):
    output = pickle.loads(pkgutil.get_data('dlstats', 'tests/resources/ecb/ecb_categories_sdmx_dict.pkl'))
    return output


class ECBCategoriesDBTestCase(BaseDBFetcherTestCase):
    def setUp(self):
        BaseDBFetcherTestCase.setUp(self)
        self.fetcher = ecb.ECB(db=self.db)
    @patch('sdmx.ecb.dataflows',dataflows)
    @patch('dlstats.fetchers.ecb.ECB.get_categories',get_categories)
    def test_categories(self):
        #We exclude ObjectIDs because their values are not determinitstic. We
        #can't test these elements
        results = self.db[constants.COL_CATEGORIES].find(
            {"provider": self.fetcher.provider_name},
            {'_id': False, 'children': False})
        reference = pickle.loads(pkgutil.get_data(
            'dlstats', 'tests/resources/ecb/ecb_categories_mongo_dict.pkl'))
        self.assertEqual([result for result in results],reference)

if __name__ == '__main__':
    unittest.main()
    #print(generate_datasets_sdmx())
