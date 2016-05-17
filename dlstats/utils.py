# -*- coding: utf-8 -*-

import hashlib
from datetime import datetime
import time
import os
import logging
import tempfile
from io import StringIO
import traceback

import requests
import arrow
from bson import ObjectId
from slugify import slugify as original_slugify

from widukind_common.debug import timeit

logger = logging.getLogger(__name__)

MONGO_DENIED_KEY_CHARS = [".", "$"]

def last_error():
    f = StringIO() 
    traceback.print_exc(file=f)
    return f.getvalue()

def make_store_path(base_path=None, provider_name=None, dataset_code=None):
    store_filepath = None
    
    if not base_path:
        store_filepath = tempfile.mkdtemp()
    else:
        store_filepath = base_path
    
    if provider_name:
        store_filepath = os.path.abspath(os.path.join(store_filepath, provider_name))
        if not os.path.exists(store_filepath):
            os.makedirs(store_filepath)
    
    if dataset_code:
        store_filepath = os.path.abspath(os.path.join(store_filepath, dataset_code))
        if not os.path.exists(store_filepath):
            os.makedirs(store_filepath)
    
    return store_filepath

def get_url_hash(url):
    return hashlib.sha224(url.encode("utf-8")).hexdigest()    

class Downloader:
    
    DEFAULT_HEADERS = {
        'user-agent': 'dlstats - https://github.com/Widukind/dlstats'
    }
    
    def __init__(self, url=None, filename=None, store_filepath=None, 
                 timeout=None, max_retries=0, 
                 replace=True, force_replace=True, use_existing_file=False,
                 headers={}, client=None):
        
        self.url = url
        self.filename = filename
        self.store_filepath = store_filepath
        self.timeout = timeout
        self.max_retries = max_retries
        self.force_replace = force_replace
        self.headers = headers
        self.client = client or requests
        self.use_existing_file = use_existing_file

        if not self.url:
            raise ValueError("url is required")
        
        if not self.filename:
            raise ValueError("filename is required")
        
        for item in self.DEFAULT_HEADERS.items():
            self.headers.setdefault(*item)
        
        if not self.store_filepath:
            self.store_filepath = tempfile.mkdtemp()
        else:
            if not os.path.exists(self.store_filepath):
                os.makedirs(self.store_filepath, exist_ok=True)
        
        self.filepath = os.path.abspath(os.path.join(self.store_filepath, self.filename))
        
        if os.path.exists(self.filepath) and not self.use_existing_file and not replace:
            raise Exception("filepath is already exist : %s" % self.filepath)

    def _download(self, raise_errors=True):
        
        #TODO: max_retries (self.max_retries)
        #TODO: analyse rate limit dans headers
        
        start = time.time()
        try:
            response = self.client.get(self.url, 
                                    timeout=self.timeout, 
                                    stream=True,
                                    allow_redirects=True,
                                    verify=False,
                                    headers=self.headers)

            code = int(response.status_code)
            
            if code == 304 or code >= 400:
                msg = "download url[%s] - status_code[%s] - reason[%s]" % (self.url, 
                                                                           code, 
                                                                           response.reason)
                if raise_errors:
                    logger.error(msg)
                    raise response.raise_for_status()
                else:
                    logger.warning(msg)
                    return response

            with open(self.filepath, mode='wb') as f:
                for chunk in response.iter_content():
                    f.write(chunk)

            return response
        
        except Exception as err:
            logger.critical("Not captured exception : %s" % str(err))
            raise

        end = time.time() - start
        logger.info("download file[%s] - END - time[%.3f seconds]" % (self.url, end))
    
    def get_filepath(self):
        
        if os.path.exists(self.filepath) and not self.use_existing_file and self.force_replace:
            os.remove(self.filepath)
        
        if not os.path.exists(self.filepath):
            logger.warning("not found file[%s] - download dataset url[%s]" % (self.filepath, self.url))
            self._download()
        else:
            logger.warning("use local dataset file [%s]" % self.filepath)
        
        return self.filepath

    def get_filepath_and_response(self):
        
        response = None
        
        if os.path.exists(self.filepath) and not self.use_existing_file and self.force_replace:
            os.remove(self.filepath)
        
        if not os.path.exists(self.filepath):
            logger.warning("not found file[%s] - download dataset url[%s]" % (self.filepath, self.url))
            response = self._download(raise_errors=False)
        else:
            logger.warning("use local dataset file [%s]" % self.filepath)
        
        return self.filepath, response


def clean_datetime(dt=None,
                   rm_hour=False, 
                   rm_minute=False, 
                   rm_second=False, 
                   rm_microsecond=True, 
                   rm_tzinfo=True):
    
    now = dt or datetime.now()
    year = now.year 
    month = now.month 
    day = now.day
    hour = now.hour
    minute = now.minute
    second = now.second    
    microsecond = now.microsecond
    tzinfo = now.tzinfo
    
    if rm_hour:
        hour = 0
    if rm_minute:
        minute = 0
    if rm_second:
        second = 0    
    if rm_microsecond:
        microsecond = 0
    if rm_tzinfo:
        tzinfo = None
    return datetime(year, month, day, hour, minute, second, microsecond, tzinfo=tzinfo)
    
def remove_file_and_dir(filepath, let_root=False):
    if not os.path.exists(filepath):
        #logger.warning("file not found [%s]" % filepath)
        return
    
    if not os.path.isfile(filepath):
        logger.warning("file is not file [%s]" % filepath)
        return
    
    dirname = os.path.dirname(filepath)

    if not os.path.isdir(dirname) or not os.path.exists(dirname):
        #logger.warning("dir is not dir or not found [%s]" % dirname)
        return
    if os.path.ismount(dirname):
        logger.critical("dir is protected [%s]" % dirname)
        return

    #if logger.isEnabledFor(logging.DEBUG):
    #    logger.debug("remove file[%s]" % filepath)    
    #os.remove(filepath)
    
    #if logger.isEnabledFor(logging.DEBUG):
    #    logger.debug("remove dir[%s]" % dirname)

    for root, dirs, files in os.walk(dirname, topdown=False):
        for name in files:
            _filepath = os.path.join(root, name)
            try:
                os.remove(_filepath)
                logger.debug("remove file[%s]" % _filepath)
            except Exception as err:
                logger.error("not remove file[%s] - error[%s]" % (_filepath, str(err)))
        for name in dirs:
            _filepath = os.path.join(root, name)
            try:
                os.rmdir(_filepath)            
                logger.debug("remove dir[%s]" % _filepath)
            except Exception as err:
                logger.error("not remove dir[%s] - error[%s]" % (_filepath, str(err)))

    if os.path.exists(dirname):
        if let_root:
            logger.debug("root dir not remove %s" % dirname)
        else:
            try:
                os.rmdir(dirname)
            except Exception as err:
                logger.error("root dir not remove %s" % dirname)

def get_year(date_str):
    if "-" in date_str:
        return date_str.split("-")[0]
    else:
        return date_str[:4]

def get_month(date_str):
    if "-" in date_str:
        return date_str.split("-")[1]
    else:
        return date_str[4:][:2]

def get_day(date_str):
    if "-" in date_str:
        return date_str.split("-")[2]
    else:
        return date_str[-2:]

@timeit("utils.get_datetime_from_period", stats_only=True)
def get_datetime_from_period(date_str, freq=None):
    
    #TODO: cache
    
    year = None
    month = None
    day = None
    
    if freq == "A":
        year = int(get_year(date_str))
        month = 1
        day = 1
    elif freq == "M":
        year = int(get_year(date_str))
        month = int(get_month(date_str))
    elif freq == "D":
        year = int(get_year(date_str))
        month = int(get_month(date_str))
        day = int(get_day(date_str))
    elif freq == "Q":
        year = int(get_year(date_str))
        month_str = get_month(date_str)
        if month_str.startswith("Q"):
            if month_str == "Q1":
                month = 1
            elif month_str == "Q2":
                month = 4
            elif month_str == "Q3":
                month = 7
            elif month_str == "Q4":
                month = 10
            else:
                raise NotImplementedError("freq not implemented freq[%s] date[%s]" % (freq, date_str))
        #else:
        #    month = int(month_str)
            
    elif freq == "W":
        year = int(get_year(date_str))
        """
        W-WED
        W-MON
        W-FRI
        W-THU
        1998-W53
        2010-06-16
        >>> datetime(2016,12,31).strftime("%Y-%m-%d %W")
        '2016-12-31 52'
        
        >>> pd.Period("2016-01-27", freq="W-WED").to_timestamp()
        Timestamp('2016-01-21 00:00:00')
        >>> pd.Period("2016-01-27", freq="W-WED").ordinal
        2404
        
        >>> pd.Period("2016-01-27", freq="W-MON").to_timestamp()
        Timestamp('2016-01-26 00:00:00')
        >>> pd.Period("2016-01-27", freq="W-MON").ordinal
        2405                        
        """
        raise NotImplementedError("freq not implemented freq[%s] date[%s]" % (freq, date_str))
    elif freq == "S":
        """
        ECB:
            H: Half-yearly (2000-S2)
            S: Half Yearly
        """
        year = int(get_year(date_str))
        if freq.endswith("1"):
            month = 1
        elif freq.endswith("2"):
            month = 7
        else:
            raise NotImplementedError("freq not implemented freq[%s] date[%s]" % (freq, date_str))
    elif freq == "B":
        """
        Bimestre: période de 2 mois
        """
        raise NotImplementedError("freq not implemented freq[%s] date[%s]" % (freq, date_str))
    else:
        raise NotImplementedError("freq not implemented freq[%s] date[%s]" % (freq, date_str))
        
    
    dt = datetime(year, month or 1, day or 1)
    return clean_datetime(dt, rm_hour=True, rm_minute=True, rm_second=True, rm_microsecond=True, rm_tzinfo=True)

@timeit("utils.get_ordinal_from_period", stats_only=True)
def get_ordinal_from_period(date_str, freq=None):
    """
    Frequency stats - 2016-03-30
    { "_id" : "A", "count" : 36858826 }
    { "_id" : "Q", "count" : 3318158 }
    { "_id" : "M", "count" : 507243 }
    { "_id" : "W-WED", "count" : 1713 }
    { "_id" : "D", "count" : 845 }
    { "_id" : "W-MON", "count" : 77 }
    { "_id" : "W-FRI", "count" : 60 }
    { "_id" : "W-THU", "count" : 2 }    
    """
    
    from dlstats.cache import cache
    from dlstats import constants
    from pandas import Period
        
    key = "ordinal.%s.%s" % (date_str, freq)

    if cache and freq in constants.CACHE_FREQUENCY:
        period_from_cache = cache.get(key)
        if not period_from_cache is None:
            return period_from_cache
    
    period_ordinal = None
    if freq == "A":
        year = int(get_year(date_str))
        period_ordinal = year - 1970
    """
    elif freq == "M":
        year = int(get_year(date_str))
        month = int(get_month(date_str))
        period_ordinal = ((year - 1970) * 12) * month
    """
    """
     ("1970", "A", 0),
     ("1969", "A", -1),
     ("1971", "A", 1),

     ("1970-01", "M", 0),
     ("197001", "M", 0),
     ("1970-02", "M", 1),
     ("1969-12", "M", -1),
     ("1969-01", "M", -12),
     ("1971-01", "M", 12),
     ("1970-07", "M", 6),
     ("1971-07", "M", 18),
     ("1969-07", "M", -6),
     
    """

    if not period_ordinal:
        period_ordinal = Period(date_str, freq=freq).ordinal
    
    if cache and freq in constants.CACHE_FREQUENCY:
        cache.set(key, period_ordinal)
    
    return period_ordinal

@timeit("commons.slugify", stats_only=True)
def slugify(text, **kwargs):
    
    from dlstats.cache import cache

    key = "slugify.%s" % text

    if cache:
        slug_from_cache = cache.get(key)
        if slug_from_cache:
            return slug_from_cache
    
    slug = original_slugify(text, **kwargs)

    if cache:
        cache.set(key, slug)
        
    return slug

def clean_key(key):
    if not key:
        return key    
    for k in MONGO_DENIED_KEY_CHARS:
        key = key.replace(k, "_")
    return key

def clean_dict(dct):
    if not dct:
        return dct
    new_dct = dct.copy()
    for k, v in dct.items():
        new_dct.pop(k)
        key = clean_key(k)
        new_dct[key] = v
    return new_dct

def json_dump_convert(obj):
    
    if isinstance(obj, ObjectId):
        return str(obj)
    
    elif isinstance(obj, datetime):
        return arrow.get(obj).for_json()
    
    return obj

