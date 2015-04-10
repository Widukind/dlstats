import logging
import logging.config

config_filename = '/etc/dlstats/logging.conf'
try:
    file = os.open(config_filename)
    logging.config.fileConfig(config_filename)
    logger = logging.getLogger('dlstats')
except:
    logger = logging.getLogger('dlstats')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('dlstats.log')
    fh.setLevel(logging.DEBUG)
    frmt = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(frmt)
    logger.addHandler(fh)

