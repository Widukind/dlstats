# -*- coding: utf-8 -*-

from dlstats.tests.base import BaseTestCase

from dlstats import utils
from dlstats import cache

class UtilsTestCase(BaseTestCase):
    
    # nosetests -s -v dlstats.tests.test_utils:UtilsTestCase
    
    def test_get_ordinal_from_period(self):
        
        cache.configure_cache()
        
        """
        >>> pd.Period("1970-Q1", freq="Q").ordinal
        0
        >>> pd.Period("1970-Q2", freq="Q").ordinal
        1
        >>> pd.Period("1970-Q3", freq="Q").ordinal
        2
        >>> pd.Period("1970-Q4", freq="Q").ordinal
        3
        >>> pd.Period("1971-Q1", freq="Q").ordinal
        4        
        >>> pd.Period("1969-Q1", freq="Q").ordinal
        -4
        >>> pd.Period("1969-Q4", freq="Q").ordinal
        -1
        >>> pd.Period("1968-Q1", freq="Q").ordinal
        -8
        """
        
        TEST_VALUES = [
             ("1970", "A", 0),
             ("1969", "A", -1),
             ("1971", "A", 1),
             ("1970-01-01", "A", 0),
             ("19700101", "A", 0),
             
             ("1970-Q1", "Q", 0),
             ("1970Q1", "Q", 0),
             ("1968-Q1", "Q", -8)
             
        ]
        
        for date_str, freq, result in TEST_VALUES:
            self.assertEquals(utils.get_ordinal_from_period(date_str, freq), result) 
    
