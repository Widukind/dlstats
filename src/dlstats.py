#!/usr/bin/python
# -*- coding: utf-8 -*-

import numpy
from pandas.tseries.offsets import *
import pandas
import datetime
import os
from io import BytesIO, StringIO, TextIOWrapper
import zipfile
import urllib.request
import functions
import logging
import re
import itertools
from multiprocessing import Pool
import pymongo

lgr = logging.getLogger('dlstats')
lgr.setLevel(logging.DEBUG)
fh = logging.FileHandler('dlstats.log')
fh.setLevel(logging.DEBUG)
frmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(frmt)
lgr.addHandler(fh)

class INSEE(object):
    def __init__(self):
        self.last_update = None
        self._categories = []
        self.client = pymongo.MongoClient()
        self.db = self.client.INSEE

    def retrieve_identifier(self,post_request):
        webpage = functions.urlopen(
                'http://www.bdm.insee.fr/bdm2/listeSeries.action', post_request)
        try:
            webpage = lxml.html.tostring(webpage)
            re_search = re.search("<td>[0-9]{9}</td>",str(webpage))
            if re_search:
                return(re_search.group(0)[4:13])
        except:
            return("lxml failed to parse the request"+str(post_request))

    def set_categories(self):
        lgr.debug('Retrieving categories')
        lgr.debug('set_categories() got called')
        self.db.categories = {}
        lgr.debug('Retrieving http://www.bdm.insee.fr/bdm2/index.action')
        webpage = functions.urlopen('http://www.bdm.insee.fr/bdm2/index.action')
        div = webpage.get_element_by_id("col-centre")
        for liste in div.iterfind(".//li"):
            for anchor in liste.iterfind(".//a"):
                _url = "http://www.bdm.insee.fr"+anchor.get("href")
                _name = anchor.text
                if self.db.categories.find_one({'name': _name}):
                    lgr.warning(
                        'Category %s already in the database. Not updating.',
                        _name)
                else:
                    __id_categories = self.db.categories.insert({
                    name:_name,
                    url: _url})
                    lgr.debug('Inserted main category {name: %s, url: %s}',
                              (_name, _url))
                webpage1 = functions.urlopen(_url)
                ul = webpage1.get_element_by_id("racine")
                for anchor in ul.iterfind(".//a"):
                    _url = "http://www.bdm.insee.fr"+anchor.get("href")
                    _name = anchor.text
                    if self.db.subcategories.find_one({'name': _name}):
                        lgr.warning(
                            'Subategory %s already in the database. Not updating.',
                            _name)
                    else:
                        __id_subcategories = self.db.subcategories.insert({
                            name:anchor.text,
                            url: _url})
                        self.db.subcategories_blgs_to_category.insert({
                            _id_categories: __id_categories,
                            _id_subcategories: __id_subcategories})
                        lgr.debug('Inserted subcategory {name: %s, url: %s}',
                                  (_name, _url))

        lgr.debug('Extracting POST requests from subcategories')
        #TODO revoir à partir de la ligne 108 où ma compréhension de liste me met les valeurs avec les textes. J'ai besoin de retester le produit cartésien de la ligne 114 pour voir s'il fonctionne avec ce genre de liste de liste.
        POST_requests = []
        subcategories = self.db.subcategories.find()
        for subcategory in subcategories:
            lgr.debug('Opening : %s', subcategory['url'])
            webpage2 = functions.urlopen(subcategory['url'])
            code_groupe_input = webpage2.get_element_by_id("listeSeries_codeGroupe")
            code_groupe = code_groupe_input.get("value")
            lgr.debug('Got code_groupe : %s', code_groupe)

            lgr.debug('Looking for series.')
            all_idcriteria = []
            all_options = []
            for myinput in webpage2.iterfind(".//input"):
                lgr.debug('Looking for series.')
                if re.search("_multiselect_liste", str(myinput.get("id"))):
                    idcriteria_number = webpage2.get_element_by_id(
                        re.search('liste.+', str(myinput.get("id"))).group(0)
                        ).name
                    select = webpage2.get_element_by_id(
                        re.search('liste.+', str(myinput.get("id"))).group(0)
                        )
                    options = [(option.value, option.text) for option in select.iterfind(".//option")]
                    all_idcriteria.append(idcriteria_number)
                    all_options.append(options)
                    lgr.debug('Updated all_idcriteria : %s', all_idcriteria)
            i = 0
            i = 0
            combinations = list(itertools.product(*all_options))
            while i < len(combinations):
                j=0
                values = combinations[i]
                POST_request = {}
                while j < len(values):
                    POST_request[str(all_idcriteria[j])] = values[j]
                    POST_request["__multiselect_"+str(all_idcriteria[j])] = ""
                    POST_request["nombreCriteres"]=str(len(values))
                    POST_request["codeGroupe"]=str(code_groupe)
                    POST_requests.append(POST_request) 
                    j+=1
                i+=1

        pool = Pool(process_count)
        job_count = 0
        jobs = []
        for job in pool.imap_unordered(retrieve_identifier,POST_requests,chunksize=1500):
            job.append(job)
            job_count += 1
            incomplete = len(POST_requests) - job_count
            if job_count % 10:
                sys.stdout.write(str(job_count)+"/"+str(len(post_requests)))
        pool.close()
        pool.join()

    def get_categories(self):
        if self._categories == []:
            self.set_categories()
        return self._categories

    categories = property(get_categories,set_categories)


    def download_series(self,INSEE_id,name):
        """INSEE identifier on the BDM (www.insee.fr)"""
    #TODO : validate INSEE id by checking on the website
    #TODO : Network failure exception
        response = urllib.request.urlopen(
                "http://www.bdm.insee.fr/bdm2/exporterSeries.action?"
                "periodes=toutes&nbPeriodes=0&liste_formats=txt&request_locale=en"
                "&chrono=true&idbank="+INSEE_id)
        memzip = BytesIO(response.read())
        archive = zipfile.ZipFile(memzip, mode='r')
        """ZipFile provided by the INSEE"""

        charact_csv = TextIOWrapper(archive.open('Charact.csv','rU'),encoding='latin-1',newline='')
        carac_list = [line.strip() for line in charact_csv]
        series_info={}
        """Various unreliable info on the series"""
        for line in carac_list:
            vars = line.split(';')
            if vars != ['']:
                series_info[vars[0]] = vars[1]

        values_csv = TextIOWrapper(archive.open('Values.csv','rU'),encoding='latin-1',newline='')
        if series_info['Periodicity'] == 'Annual':
            data = pandas.read_table(values_csv,encoding='latin-1',sep=';',header=0,parse_dates=True,index_col=0,decimal='.',thousands=',',names=['Year',Name],skiprows=3, dtype='float64')
            data = data.resample('A', fill_method='ffill')
        if series_info['Periodicity'] == 'Quarterly':
            def _quarterly_data_parser(year_and_quarter):
                year,quarter = year_and_quarter.split()
                return datetime.datetime.strptime(year + ' ' + str(int(quarter)*3),'%Y %m')
            data = pandas.read_table(values_csv,encoding='latin-1',sep=';',header=0,parse_dates=[['Year','Quarter']],index_col=0,date_parser=_quarterly_data_parser,decimal='.',thousands=',',names=['Year','Quarter',name],skiprows=2)
            data = data.resample('Q', fill_method='ffill')
        if series_info['Periodicity'] == 'Monthly':
            def _monthly_data_parser(year_and_month):
                return datetime.datetime.strptime(year_and_month,'%Y %m')
            data = pandas.read_table(values_csv,encoding='latin-1',sep=';',header=0,parse_dates=[['Year','Month']],index_col=0,date_parser=_monthly_data_parser,decimal='.',thousands=',',names=['Year','Month',name],skiprows=3)
            data = data.resample('M', fill_method='ffill')
        #data = data.tz_localize('Europe/Paris')
        data = data.astype('float64')
        data.index.name = 'Date'
        #data.columns = [name]
        #output = data[name]
        #output.name = name
        return data



