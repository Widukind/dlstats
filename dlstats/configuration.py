import configobj
import validate
import os

def _get_filename():
    """Return the configuration file path."""
    appname = 'dlstats'
    if os.name == 'posix':
        if "HOME" in os.environ:
            if os.path.isfile(os.environ["HOME"]+'/.'+appname+'/main.conf'):
                return os.environ["HOME"]+'/.'+appname+'/main.conf'
        if os.path.isfile('/etc/'+appname+'/main.conf'):
            return '/etc/'+appname+'/main.conf'
        else:
            raise FileNotFoundError('No configuration file found.')
    elif os.name == 'mac':
        return ("%s/Library/Application Support/%s" % (os.environ["HOME"], appname+'/main.conf'))
    elif os.name == 'nt':
        #TODO: Trouver une meilleure méthode
        return ("%s/%s" % (os.environ["APPDATA"], appname+'/main.conf'))
    else:
        raise UnsupportedOSError(os.name)

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

try:
    configuration_filename = _get_filename()
    configuration = configobj.ConfigObj(configuration_filename,
                                        configspec=_configspec.split('\n'))
    validator = validate.Validator()
    configuration.validate(validator)
except FileNotFoundError:
    configuration = configobj.ConfigObj()
    configuration['General'] = {'logging_directory': os.environ["HOME"], 'socket_directory': os.environ["HOME"]}
    configuration['Fetchers'] = {'Eurostat':{'url_table_of_contents':'http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=table_of_contents.xml'}}
    configuration['MongoDB'] = {'host':'127.0.0.1', 'port':27017}
    configuration['ElasticSearch'] = {'host':'127.0.0.1', 'port':9200}
configuration = configuration.dict()
