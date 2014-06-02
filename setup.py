#! /usr/bin/env python3
# -*- coding: utf-8 -*-
from distutils.core import setup
import os

setup(name='dlstats',
	version='0.1',
    description='A python module that provides an interface between statistics providers and pandas.',
    author='Widukind team',
    author_email='dev@michaelmalter.fr',
    url='https://github.com/Widukind', 
      package_dir={'dlstats': 'package', 'dlstats.fetchers': 'package/fetchers'},
    packages=['dlstats', 'dlstats.fetchers'],
    data_files=[('/usr/local/bin',['server/dlstats_server.py'])],
    install_requires=[
        'pandas>=0.12',
      ]
	)

with open('/etc/init.d/dlstats'):
        os.chmod('/etc/init.d/dlstats', 0o755)

with open('/usr/local/bin/dlstats_daemon.py'):
        os.chmod('/usr/local/bin/dlstats_daemon.py', 0o755)
