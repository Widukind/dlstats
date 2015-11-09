#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pprint
import os
import sys

import click

from dlstats import utils
from dlstats import version

DLSTATS_SETTINGS = dict(auto_envvar_prefix='DLSTATS')

opt_mongo_url = click.option('--mongo-url',
                             envvar='DLSTATS_MONGODB_URL',
                             default=utils.get_mongo_url(),
                             show_default=True,
                             help="URL for MongoDB connection.")

opt_es_url = click.option('--es-url',
                          envvar='DLSTATS_ES_URL',
                          default=utils.get_es_url(),
                          show_default=True,
                          help="URL for ElasticSearch connection.")

opt_silent = click.option('--silent', '-S', is_flag=True,
                          help="Suppress confirm")

opt_debug = click.option('--debug', '-D', is_flag=True)

opt_verbose = click.option('-v', '--verbose', is_flag=True,
                           help='Enables verbose mode.')

opt_pretty = click.option('--pretty', is_flag=True,
                          help='Pretty display.')

opt_logger = click.option('--log-level', '-l', 
                          required=False, 
                          type=click.Choice(['DEBUG', 'WARN', 'ERROR', 'INFO', 
                                             'CRITICAL']),
                          default='ERROR', 
                          help='Logging level')

opt_logger_conf = click.option('--log-config', 
                               type=click.Path(exists=True), 
                               help='Logging config filepath')

cmd_folder = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), 'commands'))

class Context(object):
    def __init__(self, mongo_url=None, es_url=None, verbose=False,
                 log_level='ERROR', log_config=None, 
                 debug=False, silent=False, pretty=False):
        self.verbose = verbose
        self.debug = debug
        self.silent = silent
        self.pretty = pretty
        self.mongo_url = mongo_url
        self.es_url = es_url
        
        self.log_level = log_level
        self.log_config = log_config
        
        if self.verbose:
            self.log_level = 'INFO'
        
        self.client_mongo = None
        self.db_mongo = None
        self.client_es = None
        
        self.logger = utils.configure_logging(debug=self.debug, 
                                #stdout_enable, 
                                config_file=self.log_config, 
                                level=self.log_level)

    def log(self, msg, *args):
        """Logs a message to stderr."""
        if args:
            msg %= args
        click.echo(msg, file=sys.stderr)

    def log_error(self, msg, *args):
        """Logs a message to stderr."""
        if args:
            msg %= args
        click.echo(click.style(msg, fg='red'), file=sys.stderr)

    def log_ok(self, msg, *args):
        """Logs a message to stdout."""
        if args:
            msg %= args
        click.echo(click.style(msg, fg='green'), file=sys.stdout)

    def log_warn(self, msg, *args):
        """Logs a message to stdout."""
        if args:
            msg %= args
        click.echo(click.style(msg, fg='yellow'), file=sys.stdout)

    def vlog(self, msg, *args):
        """Logs a message to stderr only if verbose is enabled."""
        if self.verbose:
            self.log(msg, *args)

    def pretty_print(self, obj):
        pprint.pprint(obj)

    def mongo_client(self):
        if not self.client_mongo:
            self.client_mongo = utils.get_mongo_client(self.mongo_url)
        return self.client_mongo

    def mongo_database(self):
        if not self.db_mongo:
            self.db_mongo = self.mongo_client().get_default_database()
        return self.db_mongo

    def es_client(self):
        if not self.client_es:
            self.client_es = utils.get_es_client(self.es_url)
        return self.client_es


class ComplexCLI(click.MultiCommand):
    def list_commands(self, ctx):
        rv = []
        for filename in os.listdir(cmd_folder):
            if filename.endswith('.py') and \
               filename.startswith('cmd_'):
                rv.append(filename[4:-3])
        rv.sort()
        return rv

    def get_command(self, ctx, name):
        try:
            mod = __import__('dlstats.commands.cmd_' + name, None, None,
                             ['cli'])
        except ImportError:
            return
        return mod.cli


@click.command(cls=ComplexCLI)
@click.version_option(version=version.version_str(),
                      prog_name="dlstats",
                      message="%(prog)s %(version)s")
def cli():
    pass


def main():
    cli()

if __name__ == "__main__":
    """
    DLSTATS_DEBUG=True dlstats fetchers run -v -S -f BIS
    same:
    dlstats fetchers run --debug -v -S -f BIS
    """

    main()
