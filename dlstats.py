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

def annu_pct_change(self):
	"""Compute the annualized growth rate from a DataFrame"""
	if self.index.freqstr == 'Q-DEC':
		return ((self.pct_change(1)+1)**4-1)
	else:
		raise NotImplementedError("Please, send me a pull request.")
pandas.core.frame.DataFrame.annu_pct_change = annu_pct_change
pandas.core.series.TimeSeries.annu_pct_change = annu_pct_change

def INSEE(INSEE_id,name):
	INSEE_id = INSEE_id
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
			return datetime.datetime.strptime(year + ' ' + str(int(quarter)*3),'%Y.0 %m.0')
		data = pandas.read_table(values_csv,encoding='latin-1',sep=';',header=0,parse_dates=[['Year','Quarter']],index_col=0,date_parser=_quarterly_data_parser,decimal='.',thousands=',',names=['Year','Quarter',name],skiprows=2, dtype='float64')
		data = data.resample('Q', fill_method='ffill')
	if series_info['Periodicity'] == 'Monthly':
		def _monthly_data_parser(year_and_month):
			return datetime.datetime.strptime(year_and_month,'%Y.0 %m.0')
		data = pandas.read_table(values_csv,encoding='latin-1',sep=';',header=0,parse_dates=[['Year','Month']],index_col=0,date_parser=_monthly_data_parser,decimal='.',thousands=',',names=['Year','Month',name],skiprows=3,dtype='float64')
		data = data.resample('M', fill_method='ffill')
	#data = data.tz_localize('Europe/Paris')
	data.name = series_info['Heading']
	data.index.name = 'Date'
	#data.columns = [name]
	#output = data[name]
	#output.name = name
	return data



