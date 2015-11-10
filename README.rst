========
WIDUKIND
========

This is a database of international macroeconomic data collected on
public web servers of statistical offices worldwide.

|Build Status| |Build Doc| |Coveralls|

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
- elasticsearch
- pandas
- voluptuous
- lxml
- pysdmx
- beautifulsoup4
- xlrd
- requests
    
INSTALLATION
============

1. create a python virtual environment

    python -m venv widukind-venv

1. clone this git project

   `git clone git@github.com:Widukind/dlstats

.. |Build Status| image:: https://travis-ci.org/Widukind/dlstats.svg?branch=master
   :target: https://travis-ci.org/Widukind/dlstats
   :alt: Travis Build Status
   
.. |Build Doc| image:: https://readthedocs.org/projects/widukind-dlstats/badge/?version=latest
   :target: http://widukind-dlstats.readthedocs.org/en/latest/?badge=latest
   :alt: Documentation Status   
   
.. |Coveralls| image:: https://coveralls.io/repos/Widukind/dlstats/badge.svg?branch=master&service=github
   :target: https://coveralls.io/github/Widukind/dlstats?branch=master
   :alt: Coverage   
