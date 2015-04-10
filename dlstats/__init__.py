import logging
from .configuration import configuration
from .mongo_client import mongo_client
from . import fetchers, misc_func

logging.config.fileConfig('/etc/dlstats/logging.conf')
logger = logging.getLogger('dlstats')
'%(asctime)s - %(name)s - %(levelname)s - %(message)s')
