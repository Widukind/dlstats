# -*- coding: utf-8 -*-

import sys
from pprint import pprint
import click

from dlstats import constants
from dlstats.fetchers import FETCHERS, FETCHERS_DATASETS
from dlstats import client
from dlstats import utils

opt_fetcher = click.option('--fetcher', '-f', 
              required=True, type=click.Choice(FETCHERS.keys()), 
              help='Fetcher choice')

opt_provider = click.option('--provider', '-p', 
              required=True, 
              type=click.Choice(FETCHERS.keys()), 
              help='Provider Name')

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
@client.opt_verbose
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_mongo_url
@opt_fetcher
def cmd_dataset_list(fetcher, **kwargs):
    """Show datasets list"""

    ctx = client.Context(**kwargs)

    f = FETCHERS[fetcher](db=ctx.mongo_database(), es_client=ctx.es_client())

    if ctx.verbose:
        func_name = 'datasets_long_list'
    else:
        func_name = 'datasets_list'

    have_func = hasattr(f, func_name)

    if not have_func:
        ctx.log_error("not implemented %s() method in fetcher" % func_name)
        return

    datasets = getattr(f, func_name)()
    if not datasets:
        ctx.log_error("Not datasets for this fetcher")
        return
        
    if ctx.verbose:
        for key, name in datasets:
            print(key, name)
    else:
        for key in datasets:
            print(key)

@cli.command('update-categories', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_mongo_url
@client.opt_es_url
@opt_fetcher
def cmd_update_categories(fetcher=None, **kwargs):
    """Create or Update fetcher Categories"""

    ctx = client.Context(**kwargs)

    ctx.log_ok("Run Update Categories for %s fetcher:" % fetcher)

    if ctx.silent or click.confirm('Do you want to continue?', abort=True):

        f = FETCHERS[fetcher](db=ctx.mongo_database(), es_client=ctx.es_client())
        f.upsert_categories()
        #TODO: lock commun avec tasks ?

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
        
        if not dataset and not hasattr(f, "upsert_all_datasets"):
            #TODO: translation EN
            ctx.log_error("Ce fetcher n'implémente pas la méthode upsert_all_datasets().")
            ctx.log_error("Vous devez choisir un dataset.")
            ctx.log_error("Operation cancelled !")
            #TODO: click fail ?
            return
        
        f.provider.update_database()
        
        f.upsert_categories()
        
        if dataset:
            f.upsert_dataset(dataset)
        else:
            f.upsert_all_datasets()
        
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
        
@cli.command('update-metas', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_mongo_url
@client.opt_es_url
@opt_fetcher
@opt_dataset
def cmd_update_metadatas(fetcher=None, dataset=None, **kwargs):
    """Update Metadatas for one or more Datasets
    
    
    dlstats fetchers update-metas -f BIS -d CNFS -l INFO -S

    dlstats fetchers update-metas -f BIS -l INFO -S    
    """

    ctx = client.Context(**kwargs)

    ctx.log_ok("Run update Metadatas for %s fetcher:" % fetcher)

    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
        
        db = ctx.mongo_database()
        es_client = ctx.es_client()
        
        f = FETCHERS[fetcher](db=db, es_client=es_client)

        if dataset:
            ctx.log("Update Metas for dataset[%s]" % dataset)
            f.update_metas(dataset)            
        else:
            datasets = db[constants.COL_DATASETS].find({"provider": fetcher},
                                                       projection={"datasetCode": True})
            for dataset in datasets:
                ctx.log("Update Metas for dataset[%s]" % dataset['datasetCode'])
                f.update_metas(dataset['datasetCode'])

@cli.command('update-tags', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_mongo_url
@opt_provider
@opt_dataset
@click.option('--max-bulk', '-M', 
              type=click.INT,
              default=20, 
              show_default=True,
              help='Max Bulk')
@click.option('--collection', '-c', 
              required=True,
              default=constants.COL_DATASETS,
              show_default=True,
              type=click.Choice(constants.COL_ALL + ['ALL']),
              help='Collection')
@click.option('-g', '--aggregate', is_flag=True, 
              help='Run aggregate tags after update.')
def cmd_update_tags(provider=None, dataset=None, collection=None, max_bulk=20, 
                    aggregate=False, **kwargs):
    """Create or Update field tags"""
    
    """
    Examples:
    
    dlstats fetchers update-tags -p BIS -d CNFS -S -c ALL
    dlstats fetchers update-tags -p BEA -d "10101 Ann" -S -c datasets
    dlstats fetchers update-tags -p BEA -d "10101 Ann" -S -c series
    dlstats fetchers update-tags -p Eurostat -d nama_10_a10 -S -c datasets
    dlstats fetchers update-tags -p OECD -d MEI -S -c datasets
    
    dlstats fetchers update-tags -p BIS -d CNFS -S -c ALL --aggregate
    """

    ctx = client.Context(**kwargs)

    ctx.log_ok("Run update tags for %s:" % provider)

    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
        
        db = ctx.mongo_database()

        if collection == "ALL":
            cols = constants.COL_ALL
        else:
            cols = [collection]
        
        for col in cols:
            #TODO: serie_key
            #TODO: cumul result et rapport
            result = utils.update_tags(db, 
                                       provider_name=provider, 
                                       dataset_code=dataset, 
                                       serie_key=None,
                                       col_name=col,
                                       max_bulk=max_bulk)

        if aggregate:
            result_datasets = utils.aggregate_tags_datasets(db, max_bulk=max_bulk)
            result_series = utils.aggregate_tags_series(db, max_bulk=max_bulk)

@cli.command('search', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_mongo_url
@click.option('--provider', '-p', 
              required=False, 
              type=click.Choice(FETCHERS.keys()), 
              help='Provider Name')
@opt_dataset
@click.option('--frequency', '-f', 
              required=False, 
              type=click.Choice(list(constants.FREQUENCIES_DICT.keys())), 
              help='Frequency choice')
@click.option('--search', '-s', 
              required=True, 
              help='Search text')
@click.option('--limit', '-l', 
              default=20, 
              type=int, 
              show_default=True,
              help='Result limit')
def cmd_search(provider=None, dataset=None, 
               frequency=None, search=None, limit=None, **kwargs):
    """Search in Series"""
    
    #TODO: pretty
    #TODO: csv ?
    
    """
    dlstats fetchers search -f Q -f A
    frequency = ('Q', 'A')
    
    dlstats fetchers search -f Q -f A -s "Economic Indicator Belgium"
    
    """
    
    ctx = client.Context(**kwargs)
    db = ctx.mongo_database()

    result = utils.search_series_tags(db, 
                                      provider_name=provider, 
                                      dataset_code=dataset, 
                                      frequency=frequency, 
                                      search_tags=search.split(), 
                                      skip=0, limit=limit)
    
    ctx.log("Count result : %s" % result.count())
    for doc in result:
        print(doc['provider'], doc['datasetCode'], doc['key'], doc['name'])
    
