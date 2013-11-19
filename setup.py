from distutils.core import setup
import os

setup(name='dlstats',
	version='0.1',
    description='A python module that provides an interface between statistics providers and pandas.',
    author='Michaël Malter',
    author_email='dev@michaelmalter.fr',
    package_dir={'': 'src'},
    packages=[''],
    data_files=[('/etc/init.d',['init/dlstats']),
                ('/usr/local/bin',['init/dlstats.py'])],
    install_requires=[
        'pandas>=0.11'
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
