# -*- coding: utf-8 -*-

from datetime import datetime

import pandas

from dlstats.fetchers._commons import Fetcher, Dataset, Series

import unittest

from ..base import RESOURCES_DIR

class FetcherTestCase(unittest.TestCase):

    def test_not_implemented_methods(self):
        
        f = Fetcher()

        with self.assertRaises(NotImplementedError):
            f.upsert_categories()

        with self.assertRaises(NotImplementedError):
            f.upsert_series()

        with self.assertRaises(NotImplementedError):
            f.upsert_a_series(None)

        with self.assertRaises(NotImplementedError):
            f.upsert_dataset(None)
        
        #TODO: f.insert_provider()
        
class SeriesInstantiation(unittest.TestCase):
    def test_full_example(self):
        series = Series(provider='Test provider',name='GDP in France',
                        key='GDP_FR',datasetCode='nama_gdp_fr',
                        values = [2700, 2720, 2740, 2760],
                        releaseDates = [datetime(2013,11,28),datetime(2014,12,28),datetime(2015,1,28),datetime(2015,2,28)],
                        period_index = pandas.period_range('1/1999', periods=72, freq='Q'),
                        attributes = {'name':'OBS_VALUE','value':'p'},
                        revisions = [{'value':2710, 'position':2,
                        'releaseDates' : [datetime(2014,11,28)]}],
                        dimensions = [{'name':'Seasonal adjustment', 'value':'wda'}])
        self.assertIsInstance(series,Series)
    def test_empty_revisions(self):
        series = Series(provider='Test provider',name='GDP in Germany',
                        key='GDP_DE',datasetCode='nama_gdp_de',
                        values = [2700, 2720, 2740, 2760],
                        releaseDates = [datetime(2013,11,28),datetime(2014,12,28),datetime(2015,1,28),datetime(2015,2,28)],
                        period_index = pandas.period_range('1/1999', periods=72, freq='Q'),
                        attributes = {'name':'OBS_VALUE','value':'p'},
                        dimensions = [{'name':'Seasonal adjustment', 'value':'wda'}])
        self.assertIsInstance(series,Series)
        
    
if __name__ == '__main__':
    unittest.main()