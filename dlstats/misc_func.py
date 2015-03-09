#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import lxml.html
import urllib
import logging
import os
import configparser

lgr = logging.getLogger('misc_func')
lgr.setLevel(logging.DEBUG)
fh = logging.FileHandler('misc_func.log')
fh.setLevel(logging.DEBUG)
frmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(frmt)
lgr.addHandler(fh)


def print_in_place(i):
	s = str(i)
	sys.stdout.write(s)
	sys.stdout.write('\b' * len(s))
	sys.stdout.flush()

def retry(tries):
	"""Retry calling the decorated function

	:param tries: number of times to try
	:type tries int
	"""
	def tryIt(func):
		def f(*args,**kwargs):
			attempts = 0
			while attempts < tries:
				try:
					return func(*args,**kwargs)
				except:
					attempts += 1
			raise Exception("Retry decorator exceeded the number of authorized attempts.")
		return f
	return tryIt

@retry(2)
def urlopen(address, data=None):
    """Retrieves a web page and returns the source code
    
    :param address: the URL of the page
    :type address str
    :param data: POST variables you want to pass.
    :type data dict
    """
    if data is not None:
        encodeddata = urllib.parse.urlencode(data)
        binary_data = encodeddata.encode("ISO-8859-1")
        webpage = urllib.request.urlopen(address, binary_data, timeout=7)
    else:
        webpage = urllib.request.urlopen(address, timeout=7)
    webpage = webpage.read()
    webpage = webpage.decode("ISO-8859-1")
    webpage = lxml.html.fromstring(webpage)
    lgr.debug('webpage = %s', webpage)
    if webpage is None:
        raise Exception("Failed to retrieve" + address)
    return webpage

def dictionary_union(*dictionaries):
    keys = [list(dictionary.keys()) for dictionary in dictionaries]
    keys = [item for items in keys for item in items]
    merged_dictionary = {}
    for key in keys:
        values=[]
        for dictionary in dictionaries:
            if key in dictionary.keys():
                values.extend(dictionary[key])
        merged_dictionary[key] = list(set(values))
    return merged_dictionary                              

