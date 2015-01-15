import unittest
import eurostat

class EurostatTestCase(unittest.TestCase):
    def test_update_eurostat(self):
        eurostat_test = eurostat.Eurostat()
        eurostat_test.update_eurostat()
    def test_update_selected_dataset_annually(self):
        eurostat_test = eurostat.Eurostat()
        eurostat_test.update_selected_dataset('nama_gdp_c')
    def test_update_selected_dataset_quarterly(self):
        eurostat_test = eurostat.Eurostat()
        eurostat_test.update_selected_dataset('nama_gdp_c')

if __name__ == '__main__':
    unittest.main()
