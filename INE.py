#!/usr/bin/python
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

response1 = urllib.request.urlopen("http://www.ine.es/en/inebmenu/indice_en.htm")
webpage = response1.read()
webpage = webpage.decode("ISO-8859-15")
webpage = lxml.html.fromstring(webpage)
regex = re.compile('/jaxi/menu.do\?type=pcaxis&path=(.*)&file=inebase&L=1')
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
                    match = regex.findall(href)
                    if match != []:
                        if current_category != None:
                            categories[current_category][anchor.text] = match[0]

cookie = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie))
response2 = opener.open("http://www.ine.es/jaxi/tabla.do?path=/t38/bme2/t30/p149/l1/&file=0907001.px&type=pcaxis&L=1")
webpage = response2.read()
webpage = webpage.decode("ISO-8859-15")
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
POST_request = urllib.parse.urlencode(POST_request)
POST_request = POST_request.encode("ISO-8859-15")
response3 = opener.open("http://www.ine.es/jaxiBD/tabla.do", POST_request)
webpage = response3.read()
webpage = webpage.decode("ISO-8859-15")
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
output = pandas.read_csv(csv,header=7, sep=',', skip_footer=11, skiprows=[0,1,2,3,4,5,6,8], parse_dates=True, index_col=0)
