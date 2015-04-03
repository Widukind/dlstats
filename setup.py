#! /usr/bin/env python3
# -*- coding: utf-8 -*-
from setuptools import setup
from dlstats import version
import os

setup(name='dlstats',
	version=version.version,
    description='A python module that provides an interface between statistics providers and pandas.',
    author='Widukind team',
    author_email='dev@michaelmalter.fr',
    url='https://github.com/Widukind', 
      package_dir={'dlstats': 'dlstats', 'dlstats.fetchers': 'dlstats/fetchers'},
    packages=['dlstats', 'dlstats.fetchers'],
    data_files=[('/usr/local/bin',['dlstats/dlstats_server.py']),
                ('/etc/systemd/system',['os_specific/dlstats.service']),
                ('/etc/',['config/dlstats'])],
    install_requires=[
        'requests>=2.4.3',
        'pymongo>=2.7.2',
        'pandas>=0.12',
        'docopt>=0.6.0',
        'voluptuous>=0.8',
        'xlrd>=0.8',
        'configobj>=5.0',
        'elasticsearch>=1.0.0,<2.0.0'
      ]
	)

with open('/etc/systemd/system/dlstats.service'):
        os.chmod('/etc/systemd/system/dlstats.service', 0o755)

with open('/usr/local/bin/dlstats_server.py'):
        os.chmod('/usr/local/bin/dlstats_server.py', 0o755)

with open('/etc/dlstats'):
        os.chmod('/etc/dlstats', 0o755)
