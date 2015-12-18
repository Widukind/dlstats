# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

from dlstats import version

setup(name='dlstats',
      version=version.version_str(),
      description='A python module that provides an interface between\
                   statistics providers and pandas.',
      author='Widukind team',
      url='https://github.com/Widukind/dlstats',
      license='AGPLv3',
      packages=find_packages(),
      include_package_data=True,
      entry_points={
        'console_scripts': [
          'dlstats = dlstats.client:main',
        ],
      },
      test_suite='nose.collector',
      )
