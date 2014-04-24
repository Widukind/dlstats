#! /usr/bin/env python3
# -*- coding: utf-8 -*-
from . import fetchers, gunicorn, misc_func
import configparser
import os

def _get_filename():
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

_filename = _get_filename()

config = configparser.ConfigParser()
config.read(_filename)
