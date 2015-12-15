# -*- coding: utf-8 -*-

import tempfile
import logging
import os
import zipfile
import datetime
import time

import xlrd
import pandas
import requests

from dlstats.fetchers._commons import Fetcher, Categories, Datasets, Providers

logger = logging.getLogger(__name__)


class WorldBank(Fetcher):
    def __init__(self, db=None, es_client=None):
        super().__init__(provider_name='WorldBank',  db=db, es_client=es_client)         
        
        self.provider = Providers(name=self.provider_name,
                                 long_name='World Bank',
                                 region='world',
                                 website='http://www.worldbank.org/',
                                 fetcher=self)
        self.api_url = 'http://api.worldbank.org/'
        self.requests_client = requests.Session()

    def download_json(self, url, parameters={}):
        per_page = 50
        payload = {'format': 'json', 'per_page': per_page}
        payload.update(parameters)
        request = self.requests_client.get(self.api_url + url, params=payload)
        first_page = request.json()
        number_of_pages = int(first_page[0]['pages'])
        for page in range(1,number_of_pages+1):
            payload = {'format': 'json', 'per_page': per_page, 'page': page}
            request = self.requests_client.get(self.api_url + url, params=payload)
            yield request.json()[1]

    def download_indicator(self, country_code, indicator_code):
        for page in self.download_json('/'.join(['countries', country_code, 'indicators', indicator_code])):
            yield page

    def datasets_list(self):
        output = []
        for page in self.download_json('sources'):
            for source in page:
                output.append(source['id'])
        return output


    def datasets_long_list(self):
        output = []
        for page in self.download_json('sources'):
            for source in page:
                output.append((source['id'], source['name']))
        return output




if __name__ == "__main__":
    wb = WorldBank()
    print(wb.datasets_list())
    #for i in wb.download_indicator('FR','SP.POP.TOTL'):
        #print(i)
