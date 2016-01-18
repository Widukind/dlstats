# -*- coding: utf-8 -*-

import time
import logging
import uuid
import os
import tempfile
from contextlib import closing

import requests

try:
    import requests_cache
    HAVE_REQUESTS_CACHE = True
except ImportError:
    HAVE_REQUESTS_CACHE = False

logger = logging.getLogger(__name__)

def get_new_tmp_file(url=None):
    #TODO: use url
    tmpdir = tempfile.mkdtemp()
    while True:
        filepath = os.path.abspath(os.path.join(tmpdir, str(uuid.uuid4())))
        if not os.path.exists(filepath):
            return filepath
        else:
            logger.warning("new tmp file exists: %s" % filepath)

class REST(object):

    """
    Query SDMX resources via REST or from a file

    The constructor accepts arbitrary keyword arguments that will be passed
    to the requests.get function on each call. This makes the REST class somewhat similar to a requests.Session. E.g., proxies or
    authorisation data needs only be provided once. The keyword arguments are
    stored in self.config. Modify this dict to issue the next 'get' request with
    changed arguments.
    """

    max_size = 2 ** 24
    '''upper bound for in-memory temp file. Larger files will be spooled from disc'''

    def __init__(self, local_filepath=None, 
                 cache=None, 
                 http_client=None, http_cfg={}):
        
        self.local_filepath = local_filepath
        
        self.http_client = http_client or requests
        
        default_cfg = dict(stream=True, timeout= 30.1, allow_redirects=True, verify=False)
        
        for it in default_cfg.items():
            http_cfg.setdefault(*it)
            
        self.config = http_cfg
        
        if HAVE_REQUESTS_CACHE and cache:
            requests_cache.install_cache(**cache)

    def get(self, url, fromfile=None, params={}, headers=None):
        """Get SDMX message from REST service or local file
        """
        
        if fromfile:
            source = fromfile
            final_url = headers = status_code = None
        else:
            source, final_url, headers, status_code = self.request(url, params=params, headers=headers)
            
        return source, final_url, headers, status_code

    def request(self, url, params={}, headers=None):
        """
        Retrieve SDMX messages.
        If needed, override in subclasses to support other data providers.

        :param url: The URL of the message.
        :type url: str
        :return: the xml data as file-like object
        """
        
        start = time.time()

        try:        

            # closing for stream=True
            with closing(self.http_client.get(url, params=params, headers=headers, **self.config)) as response:
                
                if response.ok:
                    
                    if not self.local_filepath:
                        self.local_filepath = get_new_tmp_file(response.url)
            
                    #TODO: encoding
                    with open(self.local_filepath, 'wb') as source:
                        #TODO: chunk size
                        for c in response.iter_content():
                            source.write(c)
                            
                    source = self.local_filepath
                    
                else:
                    source = None
                
                end = time.time() - start
                logger.info("download url[%s] - time[%.3f seconds]" % (url, end))

                code = int(response.status_code)
                
                #TODO: codes 500
                #TODO: raise choice
                if code >= 400:
                    raise response.raise_for_status()
                
                return source, response.url, response.headers, code
    
        except requests.exceptions.ConnectionError as err:
            raise Exception("Connection Error")
        except requests.exceptions.ConnectTimeout as err:
            raise Exception("Connect Timeout")
        except requests.exceptions.ReadTimeout as err:
            raise Exception("Read Timeout")
        except Exception as err:
            raise Exception("Not captured exception : %s" % str(err))

