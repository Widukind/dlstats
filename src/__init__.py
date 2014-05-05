import configobj
import validate
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

configuration_filename = _get_filename()

_configspec = """
[MongoDB]
host = ip_addr()
port = integer()
max_pool_size = integer()
socketTimeoutMS = integer()
connectTimeoutMS = integer()
waitQueueTimeout = integer()
waitQueueMultiple = integer()
auto_start_request = boolean()
use_greenlets = boolean()"""
configuration = configobj.ConfigObj(configuration_filename,
                                    configspec=_configspec.split('\n'))
validator = validate.Validator()
configuration.validate(validator, copy=True)
configuration = configuration.dict()

from . import fetchers, misc_func
