# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

from dlstats import version

setup(
    name='dlstats',
    version=version.version_str(),
    description='A python module that provides an interface between statistics'
                ' providers and Pandas',
    author='Widukind team',
    url='https://git.nomics.world/dbnomics/dlstats',
    license='AGPLv3',
    packages=find_packages(),
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'dlstats = dlstats.client:main',
            'dlstats-gevent = dlstats.client_gevent:main',
        ],
    },
    test_suite='nose.collector',
)
