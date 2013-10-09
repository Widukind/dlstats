from distutils.core import setup
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

os.chmod('/etc/init.d/dlstats', 0755)
os.chmod('/usr/local/bin/dlstats.py', 0755)
