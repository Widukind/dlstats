#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import lxml.html
import numpy
from pandas.tseries.offsets import *
import pandas
import datetime
import os
from io import BytesIO, StringIO, TextIOWrapper
import urllib.request, http.cookiejar
import re
import logging

lgr = logging.getLogger('monitoring')
lgr.setLevel(logging.DEBUG)

fh = logging.FileHandler('INE.log')
fh.setLevel(logging.DEBUG)

frmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(frmt)

lgr.addHandler(fh)

lgr.debug('DEBUG')

def open_url(url):
    response = urllib.request.urlopen(url)
    webpage = response.read()
    webpage = webpage.decode("ISO-8859-15")
    return lxml.html.fromstring(webpage)

def make_url_absolute(url):
    regex = re.compile('/')
    is_absolute_url = regex.match(url)
    if is_absolute_url != None:
        absolute_url = "http://www.ine.es" + url
    else:
        absolute_url = url
    return absolute_url

class INE(object):
    def __init__(self):
        webpage = open_url("http://www.ine.es/en/inebmenu/indice_en.htm")
        regex1 = re.compile('/jaxi/menu.do\?type=pcaxis&path=(.*)&file=inebase&L=1')
        regex2 = re.compile('%2F')
        regex3 = re.compile('(/jaxi/menu.do\?)|(/jaxiBD/menu.do\?)|(http://www.ine.es/jaxi/menu.do\?)|(http://www.ine.es/jaxiBD/menu.do\?)')
        regex4 = re.compile('((.htm)|(.pdf))$')
        categories = {}
        for table in webpage.iterfind(".//table"):
            if table.get("summary") == "Table with the complete list of INEbase statistical operations":
                for line in table.iterfind(".//td"):
                    lineclass = line.get("class")
                    if lineclass == "diez":
                        for anchor in line.iterfind(".//a"):
                            current_category = anchor.text
                            if current_category != None:
                                categories[current_category] = {}
                    for anchor in line.iterfind(".//a"):
                        href = anchor.get("href")
                        if href != None:
                            match = regex1.findall(href)
                            if match != []:
                                if current_category != None:
                                    categories[current_category][anchor.text] = re.sub("%2F","/",match[0])
        self.categories = {}
        for l1category, l1value in categories.items():
            for l2category, l2value in l1value.items():
                webpage = open_url("http://www.ine.es/jaxi/menu.do?type=pcaxis&path=" + l2value + "&file=inebase&L=1")
                for table in webpage.iterfind(".//table"):
                    if table.get("class") == "MENUJAXI":
                        l3values = []
                        for anchor in table.iterfind(".//a"):
                            href = anchor.get("href")
                            match = regex3.match(href)
                            if match != None:
                                l3values.append(make_url_absolute(match.string)) 
                        l4values = {}
                        for l3value in l3values:
                            lgr.debug('l3value : %s', l3value)
                            webpage_ = open_url(l3value)
                            for table_ in webpage_.iterfind(".//table"):
                                if table_.get("class") == "MENUJAXI":
                                    for anchor_ in table_.iterfind(".//a"):
                                        if anchor_.get("class") == "sinsubrayar":
                                            if not regex4.search(anchor_.get("href")):
                                                l4values[anchor_.text.strip()] = make_url_absolute(anchor_.get("href"))
                        lgr.debug('l4value : %s', l4value)
               # self.categories[l1category][l2category][l3category] = l3value
                
        

lgr.debug('1')
cookie = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie))
response2 = opener.open("http://www.ine.es/jaxi/tabla.do?path=/t38/bme2/t30/p149/l1/&file=0907001.px&type=pcaxis&L=1")
webpage = response2.read()
webpage = webpage.decode("ISO-8859-15")
lgr.debug('webpage for choosing rows and cols : %s', webpage)
webpage = lxml.html.fromstring(webpage)
POST_request = []
for form in webpage.iterfind(".//form"):
    for input in form.iterfind(".//input"):
        POST_request.append((input.get("name"), input.get("value")))
    for select in form.iterfind(".//select"):
        for option in select.iterfind(".//option"):
            POST_request.append((select.get("name"), option.get("value")))
# By default the INE presents data in rows. We (and pandas) don't like that and
# permutate the POST variables.
lgr.debug('2')
cookie = http.cookiejar.CookieJar()
rows = []
columns = []
i = 0
while i < len(POST_request):
    if POST_request[i][0] == 'rows':
        rows.append(POST_request[i][1])
        del POST_request[i]
    if POST_request[i][0] == 'columns':
        columns.append(POST_request[i][1])
        del POST_request[i]
    i += 1
for item in columns:
    POST_request.append(('rows',item))
for item in rows:
    POST_request.append(('columns',item))
lgr.debug('POST request : %s', POST_request)
POST_request = urllib.parse.urlencode(POST_request)
POST_request = POST_request.encode("ISO-8859-15")
response3 = opener.open("http://www.ine.es/jaxiBD/tabla.do", POST_request)
webpage = response3.read()
webpage = webpage.decode("ISO-8859-15")
lgr.debug('webpage with the time series : %s', webpage)
webpage = lxml.html.fromstring(webpage)
POST_request = []
for form in webpage.iterfind(".//form"):
    if form.get("action") == "download.do" and form.get("name") == "formDownloadCabecera":
        for input in form.iterfind(".//input"):
            if input.get("name") != "descargarformato":
                POST_request.append((input.get("name"), input.get("value")))
    else:
        pass
POST_request.append(("typeDownload", "4"))
POST_request.append(("descargarformato", "Go"))
POST_request = urllib.parse.urlencode(POST_request)
response4 = opener.open("http://www.ine.es/jaxiBD/download.do?"+POST_request)
webpage = response4.read()
webpage = webpage.decode("ISO-8859-15")
csv = StringIO(webpage)
#Retrieves the capital loaned nicely (one stupid trailing col)
#output = pandas.read_csv(csv,header=7, sep=',', skip_footer=11, skiprows=[0,1,2,3,4,5,6,8], parse_dates=True, index_col=0)
lgr.debug('csv : %s', csv.getvalue())
output = pandas.read_csv(csv,header=7, sep=',', skip_footer=11, skiprows=[0], parse_dates=True, index_col=0)
