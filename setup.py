#! /usr/bin/env python3
# -*- coding: utf-8 -*-
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
import os

setup(name='dlstats',
	version='0.1',
    description='A python module that provides an interface between statistics providers and pandas.',
    author='Michaël Malter',
    author_email='dev@michaelmalter.fr',
    url='http://.com' 
    package_dir={'dlstats': 'src'},
    packages=['dlstats', 'dlstats.gunicorn', 'dlstats.fetchers'],
    data_files=[('/etc/init.d',['init/dlstats']),
                ('/usr/local/bin',['init/dlstats-daemon.py'])],
    install_requires=[
        'pandas>=0.12'
      ]
	)

try:
	with open('/etc/init.d/dlstats'):
		os.chmod('/etc/init.d/dlstats', 0o755)
except IOError:
	pass

try:
	with open('/usr/local/bin/dlstats-daemon.py'):
		os.chmod('/usr/local/bin/dlstats-daemon.py', 0o755)
except IOError:
	pass
