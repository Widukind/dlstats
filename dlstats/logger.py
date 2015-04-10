import logging

logging.config.fileConfig('/etc/dlstats/logging.conf')
logger = logging.getLogger('dlstats')
