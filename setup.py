#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(name='dlstats',
	version="0.0.1",
    description='A python module that provides an interface between statistics providers and pandas.',
    author='Widukind team',
    author_email='dev@michaelmalter.fr',
    url='https://github.com/Widukind/dlstats', 
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'requests>=2.4.3',
        'pymongo>=3.0.0',
        'pandas>=0.12',
        'docopt>=0.6.0',
        'voluptuous>=0.8',
        'xlrd>=0.8',
        'configobj>=5.0',
        'beautifulsoup4>=4.4.0',
        'lxml>=3.4.0',
        'elasticsearch>=1.0.0,<2.0.0'
    ],
	tests_require=[
		'nose>=1.0'
		'coverage',
		'flake8'
	],
	test_suite='nose.collector',	
)

        
