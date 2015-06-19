import os
os.environ['DLSTATS_TEST_ENVIRONMENT'] = 'True'
import unittest
import ming
from unittest.mock import MagicMock
import dlstats.fetchers.eurostat as eurostat
import tests.mongo_temporary_instance

os.environ['MONGODB_TEST_PORT'] = '27002'

class EurostatTestCase(tests.mongo_temporary_instance.TestCase):
    def setUp(self):
        super(tests.mongo_temporary_instance.TestCase,self).setUp()
        eurostat.BulkSeries.bulk_update_elastic = MagicMock(return_value=True)
        self.eurostat = eurostat.Eurostat(self.db)
        #Don't test elasticsearch at all
    def tearDown(self):
        del(self.eurostat)
    def test_update_eurostat(self):
        self.eurostat.update_eurostat()
    def test_update_selected_dataset_annually(self):
        self.eurostat.update_selected_dataset('nama_gdp_c')
    def test_update_selected_dataset_quarterly(self):
        self.eurostat.update_selected_dataset('namq_gdp_c')

if __name__ == '__main__':
    unittest.main()
