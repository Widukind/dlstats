import ConfigParser
import os

class ConfigDlstats(object):
    """Cross platform configuration file handler.

    This class manages dlstats configuration files, providing
    easy access to the options."""

    def __init__(self):
        """Open the configuration files handler, choosing the right
        path depending on the platform."""
        appname = 'dlstats'
        if os.name == 'posix':
            if os.path.isfile(os.environ["HOME"]+'/.'+appname):
                self.filename = os.environ["HOME"]+'/.'+appname
            elif os.path.isfile('/etc/'+appname):
                self.filename = '/etc/'+appname
            else:
                raise FileNotFoundError('No configuration file found.')
        elif os.name == 'mac':
            self.filename = ("%s/Library/Application Support/%s" %
                        (os.environ["HOME"], appname))
        elif os.name == 'nt':
            self.filename = ("%s\Application Data\%s" %
                        (os.environ["HOMEPATH"], appname))
        else:
            raise UnsupportedOSError(os.name)
    self.config = ConfigParser.ConfigParser()
    self.config.read(self.filename)
