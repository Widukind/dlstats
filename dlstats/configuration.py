import configobj
import validate
import os

def _get_filename():
    """Return the configuration file path."""
    appname = 'dlstats'
    if os.name == 'posix':
        if "HOME" in os.environ:
            if os.path.isfile(os.environ["HOME"]+'/.'+appname+'/main.conf'):
                return os.environ["HOME"]+'/.'+appname+'main.conf'
        if os.path.isfile('/etc/'+appname+'/main.conf'):
            return '/etc/'+appname+'/main.conf'
        else:
            raise FileNotFoundError('No configuration file found.')
    elif os.name == 'mac':
        return ("%s/Library/Application Support/%s" % (os.environ["HOME"], appname+'/main.conf'))
    elif os.name == 'nt':
        return ("%s\Application Data\%s" % (os.environ["HOMEPATH"], appname+'/main.conf'))
    else:
        raise UnsupportedOSError(os.name)

configuration_filename = _get_filename()

_configspec = """
[General]
logging_directory = string()
socket_directory = string()
[MongoDB]
host = ip_addr()
port = integer()
max_pool_size = integer()
socketTimeoutMS = integer()
connectTimeoutMS = integer()
waitQueueTimeout = integer()
waitQueueMultiple = integer()
auto_start_request = boolean()
use_greenlets = boolean()
[ElasticSearch]
host = integer()
port = integer()
[Fetchers]
[[Eurostat]]
url_table_of_contents = string()"""
configuration = configobj.ConfigObj(configuration_filename,
                                    configspec=_configspec.split('\n'))
validator = validate.Validator()
configuration.validate(validator)
configuration = configuration.dict()

