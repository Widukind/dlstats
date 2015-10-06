========
WIDUKIND
========

This is a database of international macroeconomic data collected on
public web servers of statistical offices worldwide.

|Build Status|

REQUIREMENTS
============

Debian packages
---------------
- python3.4
- libpython3.4-dev
- mongodb
- elasticsearch
- libxml2-dev
- libsxlt-dev
- libz-dev
python modules
--------------
- configobj
- ming
- elasticsearch
- pandas
- voluptuous
- lxml
- sdmx
- beautifulsoup4
- xlrd
    
INSTALLATION
============

1. create a python virtual environment
`python -m venv widukind-venv

1. clone this git project

   `git clone git@github.com:Widukind/dlstats

.. |Build Status| image:: https://travis-ci.org/Widukind/dlstats.svg?branch=master
   :target: https://travis-ci.org/Widukind/dlstats
   :alt: Travis Build Status
   
   