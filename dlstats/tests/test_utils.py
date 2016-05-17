# -*- coding: utf-8 -*-

from dlstats.tests.base import BaseTestCase

from dlstats import utils
from dlstats import cache

class UtilsTestCase(BaseTestCase):
    
    # nosetests -s -v dlstats.tests.test_utils:UtilsTestCase
    
    def test_get_ordinal_from_period(self):
        
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
        
        >>> pd.Period('1970', freq='A')
        Period('1970', 'A-DEC')
        >>> pd.Period('1970', freq='A').ordinal
        0
        >>> pd.Period('1970', freq='M').ordinal
        0
        >>> pd.Period('1970-01', freq='M').ordinal
        0
        >>> pd.Period('1970-02', freq='M').ordinal
        1
        >>> pd.Period('1969-12', freq='M').ordinal
        -1
        >>> pd.Period('1968-01', freq='M').ordinal
        -24
        >>> pd.Period('1971-01', freq='M').ordinal
        12
        >>> pd.Period('1969-01', freq='M').ordinal
        -12
        >>> pd.Period('1970-07', freq='M').ordinal
        6
        >>> pd.Period('1971-07', freq='M').ordinal
        18
        >>> pd.Period('1969-07', freq='M').ordinal
        -6    
        """
        
        TEST_VALUES = [
             ("1970", "A", 0),
             ("1969", "A", -1),
             ("1971", "A", 1),
             ("1970-01-01", "A", 0),
             ("19700101", "A", 0),

             ("1970-01", "M", 0),
             ("197001", "M", 0),
             ("1970-02", "M", 1),
             ("1969-12", "M", -1),
             ("1969-01", "M", -12),
             ("1971-01", "M", 12),
             ("1970-07", "M", 6),
             ("1971-07", "M", 18),
             ("1969-07", "M", -6),
             
             ("1970-Q1", "Q", 0),
             ("1970Q1", "Q", 0),
             ("1968-Q1", "Q", -8)
             
        ]
        
        for date_str, freq, result in TEST_VALUES:
            _value = utils.get_ordinal_from_period(date_str, freq)
            msg = "DATE[%s] - FREQ[%s] - ATEMPT[%s] - RETURN[%s]" % (date_str, freq, result, _value)
            self.assertEquals(_value, result, msg) 
    
        cache.configure_cache()
        
        for date_str, freq, result in TEST_VALUES:
            _value = utils.get_ordinal_from_period(date_str, freq)
            msg = "DATE[%s] - FREQ[%s] - ATEMPT[%s] - RETURN[%s]" % (date_str, freq, result, _value)
            self.assertEquals(_value, result, msg) 
