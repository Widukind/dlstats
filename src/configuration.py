"""Cross platform configuration file handler.

This module manages dlstats configuration files, providing
easy access to the options."""

import configparser
import os

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

configuration = configparser.ConfigParser()
configuration.read(filename)
