# -*- coding: utf-8 -*-

from operator import itemgetter
import sys
from pprint import pprint
import click

from widukind_common import tags

from dlstats import constants
from dlstats.fetchers import FETCHERS, FETCHERS_DATASETS
from dlstats import client

opt_fetcher = click.option('--fetcher', '-f', 
              required=True, type=click.Choice(FETCHERS.keys()), 
              help='Fetcher choice')

opt_dataset = click.option('--dataset', '-d', 
              required=False, 
              help='Run selected dataset only')

opt_fetcher_not_required = click.option('--fetcher', '-f', 
               type=click.Choice(FETCHERS.keys()), 
               help='Fetcher choice')

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

    f = FETCHERS[fetcher](db=ctx.mongo_database())

    datasets = f.datasets_list()
    if not datasets:
        ctx.log_error("Not datasets for this fetcher")
        return

    for dataset in datasets:
        print(dataset["dataset_code"], dataset["name"])

@cli.command('datatree', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_mongo_url
@click.option('--force', is_flag=True, help="Force update")
@opt_fetcher
def cmd_update_data_tree(fetcher=None, force=False, **kwargs):
    """Create or Update fetcher Data-Tree"""

    ctx = client.Context(**kwargs)

    ctx.log_ok("Run Update Data-Tree for %s fetcher:" % fetcher)

    if ctx.silent or click.confirm('Do you want to continue?', abort=True):

        f = FETCHERS[fetcher](db=ctx.mongo_database())
        f.upsert_data_tree(force_update=force)
        #TODO: lock commun avec tasks ?

@cli.command('calendar', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_mongo_url
@opt_fetcher
def cmd_calendar(fetcher=None, **kwargs):
    """Display calendar for this provider"""
    
    """
    Ouput examples:
    
    $ dlstats fetchers calendar -F BIS
    ---------------------------------------------------------------------------------------------------------------------------
    Provider   | Dataset      | Action          | Type   | Date (yyyy-mm-dd hh:mn)
    ---------------------------------------------------------------------------------------------------------------------------
    BIS        | EERI         | update_node     | date   | 2016-01-18 - 08:00
    BIS        | LBS-DISS     | update_node     | date   | 2016-01-22 - 08:00
    BIS        | CBS          | update_node     | date   | 2016-01-22 - 08:00    
    ---------------------------------------------------------------------------------------------------------------------------

    $ dlstats fetchers calendar -F ECB
    ---------------------------------------------------------------------------------------------------------------------------
    Provider   | Dataset      | Action          | Type   | Date (yyyy-mm-dd hh:mn)
    ---------------------------------------------------------------------------------------------------------------------------
    ECB        | BLS          | update_node     | date   | 2016-01-19 - 10:00
    ECB        | ICP          | update_node     | date   | 2016-01-19 - 11:00
    ECB        | IVF          | update_node     | date   | 2016-01-21 - 10:00
    ECB        | BSI          | update_node     | date   | 2016-01-29 - 10:00    
    ---------------------------------------------------------------------------------------------------------------------------
    """

    ctx = client.Context(**kwargs)

    f = FETCHERS[fetcher](db=ctx.mongo_database())
    if not hasattr(f, 'get_calendar'):
        ctx.log_error("Not implemented get_calendar() method")
        ctx.log_error("Operation cancelled !")
        return False
    
    calendars = [(i, c) for i, c in enumerate(f.get_calendar())]
    dates = [(i, c['period_kwargs']['run_date']) for i, c in enumerate(f.get_calendar())]

    fmt = "{0:10} | {1:12} | {2:15} | {3:6} | {4:10}"
    print("---------------------------------------------------------------------------------------------------------------------------")
    print(fmt.format("Provider", "Dataset", "Action", "Type", "Date (yyyy-mm-dd hh:mn)"))
    print("---------------------------------------------------------------------------------------------------------------------------")
    for entry in sorted(dates, key=itemgetter(1)):
        c = calendars[entry[0]][1]
        action = c['action']
        period_type = c['period_type']
        k = c['kwargs']
        provider_name = fetcher
        dataset_code = k.get('dataset_code', 'ALL')
        if period_type == "date":
            _date = c['period_kwargs']['run_date'].strftime("%Y-%m-%d - %H:%M")
        else:
            _date = c['period_kwargs']['run_date']
        print(fmt.format(provider_name, dataset_code, action, period_type, _date)) 
    print("---------------------------------------------------------------------------------------------------------------------------")

@cli.command('run', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_logger_file
@client.opt_mongo_url
@click.option('--data-tree', is_flag=True,
              help='Update data-tree before run.')
@opt_fetcher
@opt_dataset
def cmd_run(fetcher=None, dataset=None, data_tree=False, **kwargs):
    """Run Fetcher - All datasets or selected dataset"""

    ctx = client.Context(**kwargs)
    
    ctx.log_ok("Run %s fetcher:" % fetcher)
    
    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
        
        f = FETCHERS[fetcher](db=ctx.mongo_database())
        
        if not dataset and not hasattr(f, "upsert_all_datasets"):
            #TODO: translation EN
            ctx.log_error("Ce fetcher n'implémente pas la méthode upsert_all_datasets().")
            ctx.log_error("Vous devez choisir un dataset.")
            ctx.log_error("Operation cancelled !")
            #TODO: click fail ?
            return
        
        if data_tree:
            f.upsert_data_tree(force_update=True)
        
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
@opt_fetcher_not_required
def cmd_report(fetcher=None, **kwargs):
    """Fetchers report"""
        
    """
    Report example:
    ---------------------------------------------------------------------------------------------------------------------------
    MongoDB: mongodb://localhost/widukind :
    ---------------------------------------------------------------------------------------------------------------------------
    Provider             | Version   | Dataset                        | Series     | First Download       | last Download
    ---------------------------------------------------------------------------------------------------------------------------
    BIS                  |         1 | PP-LS                          |         23 | 2016-01-06 - 09:38   | 2016-01-06 - 09:38
    INSEE                |         1 | CNT-2010-PIB-RF                |         11 | 2016-01-06 - 09:37   | 2016-01-06 - 09:37    
    ---------------------------------------------------------------------------------------------------------------------------
    """
    ctx = client.Context(**kwargs)
    db = ctx.mongo_database()
    fmt = "{0:10} | {1:4} | {2:30} | {3:10} | {4:15} | {5:20} | {6:20}"
    print("---------------------------------------------------------------------------------------------------------------------------")
    print("MongoDB: %s :" % ctx.mongo_url)
    print("---------------------------------------------------------------------------------------------------------------------------")
    print(fmt.format("Provider", "Ver.", "Dataset", "Series", "Last Update", "First Download", "last Download"))
    print("---------------------------------------------------------------------------------------------------------------------------")
    query = {}
    if fetcher:
        query["name"] = fetcher
        
    for provider in db[constants.COL_PROVIDERS].find(query):
        
        for dataset in db[constants.COL_DATASETS].find({'provider_name': provider['name']}).sort("dataset_code"):
            
            series_count = db[constants.COL_SERIES].count({'provider_name': provider['name'], 
                                                           "dataset_code": dataset['dataset_code']})
            
            if not provider['enable']:
                _provider = "%s *" % provider['name']
            else: 
                _provider = provider['name']
            
            print(fmt.format(_provider, 
                             provider['version'], 
                             dataset['dataset_code'], 
                             series_count,
                             str(dataset['last_update'].strftime("%Y-%m-%d")), 
                             str(dataset['download_first'].strftime("%Y-%m-%d - %H:%M")), 
                             str(dataset['download_last'].strftime("%Y-%m-%d - %H:%M"))))
    print("---------------------------------------------------------------------------------------------------------------------------")

@cli.command('tags', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_mongo_url
@opt_fetcher
@opt_dataset
@click.option('--max-bulk', '-M', 
              type=click.INT,
              default=20, 
              show_default=True,
              help='Max Bulk')
@click.option('--collection', '-c', 
              required=True,
              default='ALL',
              show_default=True,
              type=click.Choice([constants.COL_DATASETS, constants.COL_SERIES, 'ALL']),
              help='Collection')
@click.option('-g', '--aggregate', is_flag=True, 
              help='Run aggregate tags after update.')
def cmd_update_tags(fetcher=None, dataset=None, collection=None, max_bulk=20, 
                    aggregate=False, **kwargs):
    """Create or Update field tags"""
    
    """
    Examples:
    
    dlstats fetchers update-tags -f BIS -d CNFS -S -c ALL
    dlstats fetchers update-tags -f BEA -d "10101 Ann" -S -c datasets
    dlstats fetchers update-tags -f BEA -d "10101 Ann" -S -c series
    dlstats fetchers update-tags -f Eurostat -d nama_10_a10 -S -c datasets
    dlstats fetchers update-tags -f OECD -d MEI -S -c datasets
    
    dlstats fetchers update-tags -f BIS -d CNFS -S -c ALL --aggregate
    """

    ctx = client.Context(**kwargs)

    ctx.log_ok("Run update tags for %s:" % fetcher)

    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
        
        db = ctx.mongo_database()

        if collection == "ALL":
            cols = [constants.COL_DATASETS, constants.COL_SERIES]
        else:
            cols = [collection]
        
        for col in cols:
            #TODO: serie_key
            #TODO: cumul result et rapport
            result = tags.update_tags(db, 
                                       provider_name=fetcher, 
                                       dataset_code=dataset, 
                                       serie_key=None,
                                       col_name=col,
                                       max_bulk=max_bulk)

        if aggregate:
            result_datasets = tags.aggregate_tags_datasets(db, max_bulk=max_bulk)
            result_series = tags.aggregate_tags_series(db, max_bulk=max_bulk)

@cli.command('search', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_mongo_url
@click.option('--search-type', '-t', 
              required=False, 
              type=click.Choice([constants.COL_DATASETS, constants.COL_SERIES]),
              default=constants.COL_DATASETS,
              show_default=True, 
              help='Search Type')
@click.option('--fetcher', '-f', 
              required=False, 
              type=click.Choice(FETCHERS.keys()), 
              help='Fetcher choice')
@opt_dataset
@click.option('--frequency', '-F', 
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
def cmd_search(search_type=None, fetcher=None, dataset=None, 
               frequency=None, search=None, limit=None, **kwargs):
    """Search in Series"""
    
    #TODO: pretty
    #TODO: csv ?
    #TODO: time limit
    
    """
    dlstats fetchers search -F Q -s "euro Market financial"
    
    dlstats fetchers search -t series -F Q -s "euro Market financial" -D
    """
    
    ctx = client.Context(**kwargs)
    db = ctx.mongo_database()
    
    query = dict()

    result = tags.search_tags(db,
                               search_type=search_type, 
                               provider_name=fetcher, 
                               dataset_code=dataset, 
                               frequency=frequency, 
                               search_tags=search, 
                               limit=limit)
    
    ctx.log("Count result : %s" % result.count())
    for doc in result:
        #TODO: value/release_dates, ...
        if search_type == constants.COL_SERIES:
            fields = [doc['provider_name'], doc['dataset_code'], doc['key'], doc['name']]
        else:
            fields = [doc['provider_name'], doc['dataset_code'], doc['name']]
        if ctx.debug:
            fields.append(doc['tags'])
            
        print(fields)
            
    
