#! /usr/bin/env python3
# -*- coding: utf-8 -*-
from setuptools import setup, find_packages


def rq(s):
    return s.strip("\"'")


def version(filepath):
    import re
    import os
    re_vers = re.compile(r'VERSION\s*=.*?\((.*?)\)')
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, filepath)) as fp:
        for line in fp:
            m = re_vers.match(line.strip())
            if m:
                v = list(map(rq, m.groups()[0].split(', ')))
                return "{0}.{1}.{2}".format(*v[0:3])

setup(name='dlstats',
      version=version('dlstats/version.py'),
      description='A python module that provides an interface between\
                   statistics providers and pandas.',
      author='Widukind team',
      author_email='dev@michaelmalter.fr',
      url='https://github.com/Widukind/dlstats',
      packages=find_packages(),
      include_package_data=True,
      install_requires=[
        'requests>=2.4.3',
        'pymongo>=3.0.0',
        'pandas>=0.12',
        'voluptuous>=0.8',
        'xlrd>=0.8',
        'configobj>=5.0',
        'beautifulsoup4>=4.4.0',
        'lxml>=3.4.0',
        'elasticsearch>=1.0.0,<2.0.0',
        'colorama>=0.3.3',
        'click>=5.1'
      ],
      entry_points={
        'console_scripts': [
          'dlstats = dlstats.client:main',
        ],
      },
      tests_require=[
        'nose>=1.0',
        'coverage',
        'flake8',
        'httpretty'
      ],
      test_suite='nose.collector',
      )
