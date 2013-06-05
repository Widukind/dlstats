from distutils.core import setup
setup(name='dlstats',
	version='0.1',
    description='A python module that provides an interface between statistics providers and pandas.',
    author='Michaël Malter',
    author_email='dev@michaelmalter.fr'
    package_dir={'': 'src'},
    packages=[''],
    install_requires=[
        'pandas>=0.11'
      ]
	)
