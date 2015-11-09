# -*- coding: utf-8 -*-

import sys
from pprint import pprint
import click

from dlstats import constants
from dlstats.fetchers import FETCHERS, FETCHERS_DATASETS
from dlstats import client

opt_fetcher = click.option('--fetcher', '-f', 
              required=True, type=click.Choice(FETCHERS.keys()), 
              help='Fetcher choice')

opt_dataset = click.option('--dataset', '-d', 
              required=False, 
              help='Run selected dataset only')

@click.group()
def cli():
    """Fetchers commands."""
    pass

@cli.command('list')
def cmd_list():
    """Show fetchers list"""
    print("----------------------------------------------------")    
    for key in FETCHERS.keys():                                
        print(key)
    print("----------------------------------------------------")

@cli.command('datasets')
@opt_fetcher
def cmd_dataset_list(fetcher):
    """Show datasets list"""

    datasets = FETCHERS_DATASETS.get(fetcher, None)
    ctx = client.Context()
    if not datasets:
        #TODO: translation
        ctx.log_error("Les datasets de ce fetcher ne sont pas définis.")
        return
        
    print("----------------------------------------------------")    
    for key in datasets.keys():                                
        print(key)
    print("----------------------------------------------------")

@cli.command('run', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_mongo_url
@client.opt_es_url
@opt_fetcher
@opt_dataset
def cmd_run(fetcher=None, dataset=None, **kwargs):
    """Run Fetcher - All datasets or selected dataset"""

    ctx = client.Context(**kwargs)
    
    ctx.log_ok("Run %s fetcher:" % fetcher)
    
    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
        
        f = FETCHERS[fetcher](db=ctx.mongo_database(), es_client=ctx.es_client())
        
        if not dataset and not hasattr(f, "upsert_all_dataset"):
            #TODO: translation EN
            ctx.log_error("Ce fetcher n'implémente pas la méthode upsert_all_dataset().")
            ctx.log_error("Vous devez choisir un dataset.")
            ctx.log_error("Operation cancelled !")
            #TODO: click fail ?
            return
        
        f.provider.update_database()
        
        f.upsert_categories()
        
        if dataset:
            f.upsert_dataset(dataset)
        else:
            f.upsert_all_dataset()
        
        #TODO: lock commun avec tasks ?

#TODO: multi include/exclude fetcher        
#TODO: options sort by: series, provider, dataset + asc/desc
#TODO: categories
#TODO: option pretty pour sortie json
@cli.command('report', context_settings=client.DLSTATS_SETTINGS)
@client.opt_mongo_url
@client.opt_es_url
def cmd_report(**kwargs):
    """Fetchers report"""
        
    """
    Report example:
    ----------------------------------------------------------------------------------------------------------
    MongoDB: mongodb://localhost/widukind :
    ----------------------------------------------------------------------------------------------------------
    Provider             | Dataset                        | Series     | last Update
    ----------------------------------------------------------------------------------------------------------
    WorldBank            | GEM                            |       9346 | 2015-09-15 21:38:18
    Eurostat             | demo_pjanbroad                 |        834 | 2015-04-23 00:00:00
    Eurostat             | gov_10q_ggdebt                 |       6170 | 2015-04-21 00:00:00
    Eurostat             | namq_gdp_p                     |      11956 | 2015-04-13 00:00:00
    INSEE                | 1430                           |         42 | 1900-01-01 00:00:00
    INSEE                | 158                            |        393 | 1900-01-01 00:00:00
    IMF                  | WEO                            |      10936 | 2015-04-01 00:00:00
    BIS                  | CNFS                           |        938 | 2015-09-16 09:34:20
    BIS                  | DSRP                           |         66 | 2015-09-16 08:47:38
    ----------------------------------------------------------------------------------------------------------    
    """
    ctx = client.Context(**kwargs)
    db = ctx.mongo_database()
    fmt = "{0:20} | {1:30} | {2:10} | {3:20}"
    print("----------------------------------------------------------------------------------------------------------")
    print("MongoDB: %s :" % ctx.mongo_url)
    print("----------------------------------------------------------------------------------------------------------")
    print(fmt.format("Provider", "Dataset", "Series", "last Update"))
    print("----------------------------------------------------------------------------------------------------------")
    for provider in db[constants.COL_PROVIDERS].find({}):
        for dataset in db[constants.COL_DATASETS].find({"provider": provider['name']}):
            lastUpdate = str(dataset['lastUpdate'])
            series_count = db[constants.COL_SERIES].count({"provider": provider['name'], "datasetCode": dataset['datasetCode']})
            print(fmt.format(provider['name'], dataset['datasetCode'], series_count, lastUpdate))
    print("----------------------------------------------------------------------------------------------------------")
        
