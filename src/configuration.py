#TODO Switch to configobj. ConfigParser is stuck in stdlib, doesn't support uppercase (?!?!?!?), mess up builtins and doesn't enforce dynamic typing. (Unrelated: Someone should really write a PEP about this. Guido seems to be really reluctant about rewriting this module)
"""Cross platform configuration file handler.

This module manages dlstats configuration files, providing
easy access to the options."""

import configparser
import os
import ast

def get_filename():
    """Return the configuration file path."""
    appname = 'dlstats'
    if os.name == 'posix':
        if os.path.isfile(os.environ["HOME"]+'/.'+appname):
            return os.environ["HOME"]+'/.'+appname
        elif os.path.isfile('/etc/'+appname):
            return '/etc/'+appname
        else:
            raise FileNotFoundError('No configuration file found.')
    elif os.name == 'mac':
        return ("%s/Library/Application Support/%s" % (os.environ["HOME"], appname))
    elif os.name == 'nt':
        return ("%s\Application Data\%s" % (os.environ["HOMEPATH"], appname))
    else:
        raise UnsupportedOSError(os.name)

filename = get_filename()

_configuration = configparser.ConfigParser()
_configuration.read(filename)
configuration_types = {'MongoDB': {'host': str,
                                   'port': int,
                                   'max_pool_size': int,
                                   'socketTimeoutMS': int,
                                   'connectTimeoutMS': int,
                                   'waitQueueTimeout': int,
                                   'waitQueueMultiple': int,
                                   'auto_start_request': bool,
                                   'use_greenlets': bool}}
configuration = _configuration.__dict__['_sections'].copy()
print(configuration)
for section in _configuration.sections():
    for key, value in _configuration.items(section):
        configuration[section][key] = configuration_types[section][key](value)
