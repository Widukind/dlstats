# -*- coding: utf-8 -*-
"""
Created on Fri Nov 27 17:54:00 2015

@author: salimeh
"""
from datetime import datetime
import pandas
import requests
from lxml import etree


url = 'http://www.esri.cao.go.jp/en/sna/data/sokuhou/files/toukei_top.html'
webpage = requests.get(url)
html = etree.HTML(webpage.text)     
archive = html.xpath("//ul [@class = 'bulletList ml20']")
hrefs = archive[0].xpath (".//a")
links = [href.values() for href in hrefs]
urls = ['http://www.esri.cao.go.jp/en/sna/data/sokuhou/files/' + links[i][0]  for i in range(len(links))]
hrefs_ = tables[1].xpath(".//a")
links_ = [href_.values() for href_ in hrefs_]
deflator_urls = ['http://www.esri.cao.go.jp' + links_[2*i][0][20:]  for i in range(4)]
url_all = gdp_urls + deflator_urls


http://www.esri.cao.go.jp/en/sna/data/sokuhou/files/2015/toukei_2015.html