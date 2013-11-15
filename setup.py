from distutils.core import setup
import os

setup(name='dlstats',
	version='0.1',
    description='A python module that provides an interface between statistics providers and pandas.',
    author='Michaël Malter',
    author_email='dev@michaelmalter.fr',
    package_dir={'': 'src'},
    packages=[''],
    data_files=[('/etc/init.d','init/dlstats'),
                ('/usr/local/bin','init/dlstats.py')],
    install_requires=[
        'pandas>=0.11'
      ]
	)

try:
	with open('/etc/init.d/dlstats'):
		pass
	except IOError:
		os.chmod('/etc/init.d/dlstats', 0755)

try:
	with open('/usr/local/bin/dlstats.py'):
		pass
	except IOError:
		os.chmod('/usr/local/bin/dlstats.py', 0755)
