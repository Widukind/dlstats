# -*- coding: utf-8 -*-

from datetime import datetime
import time
import os
import logging
import tempfile
from io import StringIO
import traceback

import requests

logger = logging.getLogger(__name__)

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

class Downloader:
    
    DEFAULT_HEADERS = {
        'user-agent': 'dlstats - https://github.com/Widukind/dlstats'
    }
    
    def __init__(self, url=None, filename=None, store_filepath=None, 
                 timeout=None, max_retries=0, 
                 replace=True, force_replace=True,
                 headers={}, client=None):
        
        self.url = url
        self.filename = filename
        self.store_filepath = store_filepath
        self.timeout = timeout
        self.max_retries = max_retries
        self.force_replace = force_replace
        self.headers = headers
        self.client = client or requests

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
        
        if os.path.exists(self.filepath) and not replace:
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
        
        if os.path.exists(self.filepath) and self.force_replace:
            os.remove(self.filepath)
        
        if not os.path.exists(self.filepath):
            logger.warning("not found file[%s] - download dataset url[%s]" % (self.filepath, self.url))
            self._download()
        else:
            logger.warning("use local dataset file [%s]" % self.filepath)
        
        return self.filepath

    def get_filepath_and_response(self):
        
        response = None
        
        if os.path.exists(self.filepath) and self.force_replace:
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
        logger.warning("file not found [%s]" % filepath)
        return
    
    if not os.path.isfile(filepath):
        logger.warning("file is not file [%s]" % filepath)
        return
    
    dirname = os.path.dirname(filepath)

    if not os.path.isdir(dirname) or not os.path.exists(dirname):
        logger.warning("dir is not dir or not found [%s]" % dirname)
        return
    if os.path.ismount(dirname):
        logger.critical("dir is protected [%s]" % dirname)
        return

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("remove file[%s]" % filepath)    
    os.remove(filepath)
    
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("remove dir[%s]" % dirname)

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


def get_ordinal_from_period(date_str, freq=None):
    from dlstats.cache import cache
    from dlstats import constants
    import pandas
    
    if not cache or not freq in constants.CACHE_FREQUENCY:
        return pandas.Period(date_str, freq=freq).ordinal
    
    key = "%s.%s" % (date_str, freq)
    period_from_cache = cache.get(key)
    if period_from_cache:
        return period_from_cache
    
    period_ordinal = pandas.Period(date_str, freq=freq).ordinal
    cache.set(key, period_ordinal)
    
    return period_ordinal
