# -*- coding: utf-8 -*-

from pprint import pprint

import click

from dlstats import constants
from dlstats import client
from dlstats import utils

opt_index = click.option('--index', '-i', 
              required=False,
              show_default=True,
              default=constants.ES_INDEX, 
              help='Index name')

@click.group()
def cli():
    """ElasticSearch commands."""
    pass

@cli.command('create-index', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_es_url
@opt_index
def cmd_create_index(index, **kwargs):
    """Create Index
    """

    ctx = client.Context(**kwargs)
    
    index_name = index or constants.ES_INDEX
    
    ctx.log_warn("Create index [%s]" % index_name)
    
    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
        try:
            es_client = ctx.es_client()
            es_client.indices.create(index_name)
            ctx.log_ok("Index [%s] created" % index_name)            
        except Exception as err:
            ctx.log_error(str(err))

@cli.command('clean', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_es_url
@opt_index
def cmd_clean_index(index, **kwargs):
    """Delete index
    """

    ctx = client.Context(**kwargs)
    
    index_name = index or constants.ES_INDEX
    
    ctx.log_warn("Delete index [%s]" % index_name)
    
    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
        try:
            es_client = ctx.es_client()
            es_client.indices.delete(index_name)
            ctx.log_ok("Index [%s] deleted" % index_name)            
        except Exception as err:
            ctx.log_error(str(err))

#TODO: @cli.command('reindex', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_es_url
def cmd_reindex(**kwargs):
    """ElasticSearch Reindex
    """

    #elasticsearch.helpers.reindex(client, source_index, target_index, query=None, target_client=None, chunk_size=500, scroll=u'5m', scan_kwargs={}, bulk_kwargs={})
    ctx.log_error("Not Implemented")
    return
    ctx = client.Context(**kwargs)
    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
        pass    

#TODO: timeout
#TODO: option retry ?
@cli.command('check', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_debug
@client.opt_pretty
@client.opt_logger
@client.opt_logger_conf
@client.opt_es_url
def cmd_check(**kwargs):
    """Verify ElasticSearch connection
    """
    ctx = client.Context(**kwargs)
    #TODO: liste indexes ?
    try:
        from elasticsearch import __versionstr__
        es_client = ctx.es_client()
        server_info = es_client.info()
        print("------------------------------------------------------")
        ctx.log_ok("Connection OK")
        print("------------------------------------------------------")
        print("elasticsearch-py version : %s" % __versionstr__)
        print("-------------------- Server Infos --------------------")
        pprint(server_info)
        print("------------------------------------------------------")
    except Exception as err:
        """
        TODO: retry et erreur
        'elasticsearch.exceptions.ConnectionError'>
             'error', 'info', 'status_code'        
        GET http://localhost:9200/ [status:N/A request:2.021s]
            ConnectionRefusedError        
        """
        ctx.log_error("Connection Error:")
        print("------------------------------------------------------")
        ctx.log_error(str(err))
        print("------------------------------------------------------")
        

#TODO: @cli.command('rebuild', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_es_url
def cmd_rebuild(**kwargs):
    """ElasticSearch Rebuild
    
    All indexes or for one fetcher
    """

