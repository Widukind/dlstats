# -*- coding: utf-8 -*-

import time
import os
import logging
import tempfile
from io import StringIO
import traceback

import requests

try:
    import requests_cache
    HAVE_REQUESTS_CACHE = True
except ImportError:
    HAVE_REQUESTS_CACHE = False

logger = logging.getLogger(__name__)

def last_error():
    f = StringIO() 
    traceback.print_exc(file=f)
    return f.getvalue()

class Downloader:
    
    DEFAULT_HEADERS = {
        'user-agent': 'dlstats - https://github.com/Widukind/dlstats'
    }
    
    def __init__(self, url=None, filename=None, store_filepath=None, 
                 timeout=None, max_retries=0, 
                 replace=True, force_replace=True,
                 headers={},
                 cache=None):
        
        self.url = url
        self.filename = filename
        self.store_filepath = store_filepath
        self.timeout = timeout
        self.max_retries = max_retries
        self.force_replace = force_replace
        self.headers = headers
        
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

        if HAVE_REQUESTS_CACHE and cache:
            requests_cache.install_cache(**cache)
        
    def _download(self, raise_errors=True):
        
        #TODO: max_retries (self.max_retries)
        #TODO: analyse rate limit dans headers
        
        start = time.time()
        try:
            response = requests.get(self.url, 
                                    timeout=self.timeout, 
                                    stream=True,
                                    allow_redirects=True,
                                    verify=False,
                                    headers=self.headers)

            code = int(response.status_code)
            
            if code >= 400:
                msg = "download url[%s] - status_code[%s] - reason[%s]" % (self.url, 
                                                                           code, 
                                                                           response.reason)
                if raise_errors:
                    logger.error(msg)
                    raise response.raise_for_status()
                else:
                    logger.warning(msg)
                    return response

            with open(self.filepath, 'wb') as f:
                for chunk in response.iter_content():
                    f.write(chunk)

            return response
        
            """
            except requests.exceptions.ConnectionError as err:
                raise Exception("Connection Error")
            except requests.exceptions.ConnectTimeout as err:
                raise Exception("Connect Timeout")
            except requests.exceptions.ReadTimeout as err:
                raise Exception("Read Timeout")
            """
        except Exception as err:
            logger.critical("Not captured exception : %s" % str(err))
            raise
            #raise Exception("Not captured exception : %s" % str(err))            

        end = time.time() - start
        logger.info("download file[%s] - END - time[%.3f seconds]" % (self.url, end))
    
    def get_filepath(self):
        
        if os.path.exists(self.filepath) and self.force_replace:
            os.remove(self.filepath)
        
        if not os.path.exists(self.filepath):
            logger.info("not found file[%s] - download dataset url[%s]" % (self.filepath, self.url))
            self._download()
        else:
            logger.info("use local dataset file [%s]" % self.filepath)
        
        return self.filepath

    def get_filepath_and_response(self):
        
        response = None
        
        if os.path.exists(self.filepath) and self.force_replace:
            os.remove(self.filepath)
        
        if not os.path.exists(self.filepath):
            logger.info("not found file[%s] - download dataset url[%s]" % (self.filepath, self.url))
            response = self._download(raise_errors=False)
        else:
            logger.info("use local dataset file [%s]" % self.filepath)
        
        return self.filepath, response
