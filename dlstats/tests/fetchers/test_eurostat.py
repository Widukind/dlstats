import unittest
from unittest.mock import MagicMock
from dlstats.fetchers import eurostat

from ..base import RESOURCES_DIR

class EurostatTestCase(unittest.TestCase):
    
    def setUp(self):
        eurostat.BulkSeries.bulk_update_elastic = MagicMock(return_value=True)
        self.eurostat = eurostat.Eurostat()
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
