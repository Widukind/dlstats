# -*- coding: utf-8 -*-

from dlstats.tests.base import BaseTest, BaseDBTest

class BaseFetcherTestCase(BaseTest):
    """Fetchers tests without DB"""

class BaseDBFetcherTestCase(BaseDBTest):
    """Fetchers tests with DB"""
    
    """
    # TODO: patchs
    def _patch_urls(self, dataset, urls):

        def urls(self):
            _urls = []
            for u in urls:
                _urls.append("file:" + urllib.request.pathname2url(u))
            return _urls
        setattr(dataset, 'urls', urls)
        
    def _patch_download(self, dataset):
        
        def download(self):
            urls = self.urls()
        setattr(dataset, 'download', download)
    """
