import unittest
from datetime import datetime
from _skeleton import Dataset

class DatasetTestCase(unittest.TestCase):
    def test_full_example(self):
        self.assertIsInstance(Dataset(provider='Test provider',name='GDP',dataset_code='nama_gdp_fr',dimension_list=[{'name':'COUNTRY','values':[('FR','France'),('DE','Germany')]}],doc_href='rasessr',last_update=datetime(2014,12,2)),Dataset)
    def test_empty_doc_href(self):
        self.assertIsInstance(Dataset(provider='Test provider',name='GDP',dataset_code='nama_gdp_fr',dimension_list=[{'name':'COUNTRY','values':[('FR','France'),('DE','Germany')]}],last_update=datetime(2014,12,2)),Dataset)

if __name__ == '__main__':
    unittest.main()
