# -*- coding: utf-8 -*-

import unittest

from dlstats.fetchers._commons import Fetcher

class FetcherTestCase(unittest.TestCase):

    def test_not_implemented_methods(self):
        
        f = Fetcher()

        with self.assertRaises(NotImplementedError):
            f.upsert_categories()

        with self.assertRaises(NotImplementedError):
            f.upsert_series()

        with self.assertRaises(NotImplementedError):
            f.upsert_a_series()

        with self.assertRaises(NotImplementedError):
            f.upsert_dataset()
        
        #TODO: f.insert_provider()
    
if __name__ == '__main__':
    unittest.main()