import unittest
from dlstats.fetchers import eurostat

class EurostatTestCase(unittest.TestCase):
    def setUp(self):
        self.eurostat = eurostat.Eurostat()
    def test_update_eurostat(self):
        self.eurostat.update_eurostat()
    def test_update_selected_dataset_annually(self):
        self.eurostat.update_selected_dataset('nama_gdp_c')
    def test_update_selected_dataset_quarterly(self):
        self.eurostat.update_selected_dataset('namq_gdp_c')

if __name__ == '__main__':
    unittest.main()
