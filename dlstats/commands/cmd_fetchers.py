# -*- coding: utf-8 -*-

import time
from operator import itemgetter
import click

from widukind_common import tags
from widukind_common.tasks import consolidate
from widukind_common import errors

from dlstats import constants
from dlstats.fetchers import FETCHERS
from dlstats import client
from dlstats.utils import last_error

async_frameworks = ["future"]#, "gevent", "mp", "tornado"]

opt_fetcher = click.option('--fetcher', '-f', 
              required=True, type=click.Choice(FETCHERS.keys()), 
              help='Fetcher choice')

opt_dataset = click.option('--dataset', '-d', 
              required=False,
              help='Run selected dataset only')

opt_dataset_multiple = click.option('--dataset', '-d', 
              required=False, multiple=True,
              help='Run selected dataset(s) only')

opt_fetcher_not_required = click.option('--fetcher', '-f', 
               type=click.Choice(FETCHERS.keys()), 
               help='Fetcher choice')

opt_async_mode = click.option('--async-mode', 
              type=click.Choice(async_frameworks), 
              help='Async mode choice')

def _consolidate(ctx, db, fetcher, dataset=None, max_bulk=20):

    start = time.time()
    
    ctx.log("START consolidate for [%s]" % fetcher)
    
    query = {"provider_name": fetcher}
    if dataset:
        query["dataset_code"] = dataset
    
    if dataset:    
        result = consolidate.consolidate_dataset(db=db, **query)
        if result == 1:
            ctx.log("dataset[%s] updated" % dataset)
        else:
            ctx.log_warn("dataset[%s] not updated" % dataset)
    else:
        result = consolidate.consolidate_all_dataset(db=db, max_bulk=max_bulk, **query)
        ctx.log("%(modified_count)s modified on %(matched_count)s matched" % result)
    
    end = time.time() - start
    
    ctx.log("END consolidate for [%s] - time[%.3f]" % (fetcher, end))

def _update_tags(ctx, db, provider_name, dataset=None, max_bulk=100, update_only=False, dry_mode=False, async_mode=None):
        
    start = time.time()

    ctx.log("START update tags for [%s]" % provider_name)
    
    ctx.log("Update provider[%s] Datasets tags..." % provider_name)
    try:
        result = tags.update_tags_datasets(db,
                                  provider_name=provider_name,
                                  dataset_code=dataset, 
                                  max_bulk=max_bulk,
                                  update_only=update_only,
                                  dry_mode=dry_mode)
        ctx.log_ok("Update provider[%s] Datasets tags Success. Docs Updated[%s]" % (provider_name, result["nModified"]))
    except Exception as err:
        ctx.log_error("Update Datasets tags Fail - provider[%s] - [%s]" % (provider_name, str(err)))

    ctx.log("Update provider[%s] Series tags..." % provider_name)
    try:
        result = tags.update_tags_series(db,
                                  provider_name=provider_name,
                                  dataset_code=dataset, 
                                  max_bulk=max_bulk,
                                  update_only=update_only,
                                  async_mode=async_mode,
                                  dry_mode=dry_mode)
        if not async_mode:
            ctx.log_ok("Update provider[%s] Series tags Success. Docs Updated[%s]" % (provider_name, result["nModified"]))
    except Exception:
        ctx.log_error("Update Series tags Fail - provider[%s]: %s" % (provider_name, last_error()))

    end = time.time() - start
        
    ctx.log("END update tags for [%s] - time[%.3f]" % (provider_name, end))
        

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
@client.opt_logger_file
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

    fmt = "{0:20} | {1:70} | {2:10}"
    print("---------------------------------------------------------------------------------------------------------------------------")
    print(fmt.format("Dataset Code", "Dataset Name", "Last Update"))
    print("---------------------------------------------------------------------------------------------------------------------------")

    for dataset in datasets:
        last_update = ""
        if dataset.get('last_update'):
            last_update = str(dataset['last_update'].strftime("%Y-%m-%d"))
        print(fmt.format(dataset["dataset_code"], 
                         dataset["name"], 
                         last_update))

    print("---------------------------------------------------------------------------------------------------------------------------")

@cli.command('datatree', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_logger_file
@client.opt_mongo_url
@client.opt_requests_cache_enable
@client.opt_requests_cache_path
@client.opt_requests_cache_expire
@click.option('--use-files', is_flag=True,
              help='Use existing files in tmpdir')
@click.option('--not-remove', is_flag=True,
              help='Not remove files after process')
@click.option('--force', is_flag=True, help="Force update")
@opt_fetcher
def cmd_datatree(fetcher=None, force=False, 
                 use_files=False, not_remove=False, **kwargs):
    """Create or Update fetcher Data-Tree"""

    ctx = client.Context(**kwargs)

    ctx.log_ok("Run Update Data-Tree for %s fetcher:" % fetcher)

    if ctx.silent or click.confirm('Do you want to continue?', abort=True):

        f = FETCHERS[fetcher](db=ctx.mongo_database(),
                              use_existing_file=use_files,
                              not_remove_files=not_remove)
        
        f.upsert_data_tree(force_update=force)
        #TODO: lock commun avec tasks ?

@cli.command('calendar', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_logger_file
@client.opt_mongo_url
@opt_fetcher
@click.option('--update', is_flag=True,
              help='Update calendar in database')
def cmd_calendar(fetcher=None, update=False, **kwargs):
    """Display calendar for this provider"""
    
    """
    Ouput examples:
    
    $ dlstats fetchers calendar -F BIS
    ---------------------------------------------------------------------------------------------------------------------------
    Provider   | Dataset      | Action          | Type   | Date (yyyy-mm-dd hh:mn)
    ---------------------------------------------------------------------------------------------------------------------------
    BIS        | EERI         | update-dataset     | date   | 2016-01-18 - 08:00
    BIS        | LBS-DISS     | update-dataset     | date   | 2016-01-22 - 08:00
    BIS        | CBS          | update-dataset     | date   | 2016-01-22 - 08:00    
    ---------------------------------------------------------------------------------------------------------------------------

    $ dlstats fetchers calendar -F ECB
    ---------------------------------------------------------------------------------------------------------------------------
    Provider   | Dataset      | Action          | Type   | Date (yyyy-mm-dd hh:mn)
    ---------------------------------------------------------------------------------------------------------------------------
    ECB        | BLS          | update-dataset     | date   | 2016-01-19 - 10:00
    ECB        | ICP          | update-dataset     | date   | 2016-01-19 - 11:00
    ECB        | IVF          | update-dataset     | date   | 2016-01-21 - 10:00
    ECB        | BSI          | update-dataset     | date   | 2016-01-29 - 10:00    
    ---------------------------------------------------------------------------------------------------------------------------
    """

    ctx = client.Context(**kwargs)
    db = ctx.mongo_database()

    f = FETCHERS[fetcher](db=db)
    if not hasattr(f, 'get_calendar'):
        ctx.log_error("Not implemented get_calendar() method")
        ctx.log_error("Operation cancelled !")
        return False
    
    if update:
        if ctx.silent or click.confirm('Do you want to continue?', abort=True):
            try:
                with ctx.lock("calendar-%s" % fetcher, "calendar"):
                    f.upsert_calendar()
            except errors.Locked as err:
                ctx.log_error("calendar command is locked for provider[%s]" % fetcher)
                return False
        
    calendar_list = db[constants.COL_CALENDARS].find({"action": {"$in": ["update-dataset", "update-fetcher"]}, 
                                                      "kwargs.provider_name": fetcher})
        
    calendars = []
    dates = []
    
    for i, c in enumerate(calendar_list):
        calendars.append((i, c))
        if c["period_type"] == "date":
            dates.append((i, c['period_kwargs']['run_date']))

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
@client.opt_quiet
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_logger_file
@client.opt_mongo_url
@client.opt_cache_enable
@client.opt_requests_cache_enable
@client.opt_requests_cache_path
@client.opt_requests_cache_expire
@click.option('--max-errors', '-M', default=5, type=int, 
              show_default=True, help='Max errors accepted.')
@click.option('--datatree', is_flag=True,
              help='Update data-tree before run.')
@click.option('--use-files', is_flag=True,
              help='Use existing files in tmpdir')
@click.option('--not-remove', is_flag=True,
              help='Not remove files after process')
@click.option('--run-full', is_flag=True,
              help='Run tags and consolidate command after run')
@click.option('--dataset-only', is_flag=True,
              help='Load or update dataset only (not series)')
@client.opt_trace
@click.option('--bulk-size', '-B', default=200, type=int, 
              show_default=True, help='Bulk size for batch mode.')
@opt_fetcher
@opt_async_mode
@opt_dataset_multiple
def cmd_run(fetcher=None, dataset=None, 
            max_errors=0, bulk_size=200, datatree=False,             
            async_mode=None, 
            use_files=False, not_remove=False, run_full=False,
            dataset_only=False, 
            **kwargs):
    """Run Fetcher - All datasets or selected dataset"""

    ctx = client.Context(**kwargs)
    
    ctx.log_ok("Run %s fetcher:" % fetcher)
    
    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
        
        lock_key = "run-%s" % fetcher
        if dataset:
            lock_key = lock_key + "-" + ".".join(dataset)
        
        try:
            with ctx.lock(lock_key, "run"):
        
                db = ctx.mongo_database()
        
                f = FETCHERS[fetcher](db=db,
                                      max_errors=max_errors,
                                      bulk_size=bulk_size,
                                      use_existing_file=use_files,
                                      not_remove_files=not_remove,
                                      dataset_only=dataset_only,
                                      async_mode=async_mode)
                
                if not dataset and not hasattr(f, "upsert_all_datasets"):
                    ctx.log_error("upsert_all_datasets method is not implemented for this fetcher.")
                    ctx.log_error("Please choice a dataset.")
                    ctx.log_error("Operation cancelled !")
                    return
                
                if datatree:
                    f.upsert_data_tree(force_update=True)
                
                if dataset:
                    for ds in dataset:
                        f.wrap_upsert_dataset(ds)
                        if run_full:
                            _consolidate(ctx, db, fetcher, dataset=ds)
                            _update_tags(ctx, db, fetcher, dataset=ds)
                else:
                    f.upsert_all_datasets()
                    if run_full:
                        _consolidate(ctx, db, fetcher)
                        _update_tags(ctx, db, fetcher)
                
        except errors.Locked as err:
            ctx.log_error("run command is locked for key[%s]" % lock_key)
            return False

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
    fmt = "{0:10} | {1:4} | {2:30} | {3:10} | {4:15} | {5:20} | {6:20} | {7:7}"
    print("----------------------------------------------------------------------------------------------------------------------------------------")
    print("MongoDB: %s :" % ctx.mongo_url)
    print("----------------------------------------------------------------------------------------------------------------------------------------")
    print(fmt.format("Provider", "Ver.", "Dataset", "Series", "Last Update", "First Download", "last Download", "Enable"))
    print("----------------------------------------------------------------------------------------------------------------------------------------")
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
                             str(dataset['download_last'].strftime("%Y-%m-%d - %H:%M")),
                             str(dataset["enable"])))
    print("---------------------------------------------------------------------------------------------------------------------------")

@cli.command('tags', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_quiet
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_mongo_url
@opt_fetcher_not_required
@opt_dataset
@opt_async_mode
@click.option('--max-bulk', '-M', 
              type=click.INT,
              default=100, 
              show_default=True,
              help='Max Bulk')
@click.option('-u', '--update-only', is_flag=True, 
              help='Update only if not tags in document')
@click.option('-n', '--dry-mode', is_flag=True, help="Dry Mode")
def cmd_update_tags(fetcher=None, dataset=None, max_bulk=100, 
                    update_only=False, async_mode=None, 
                    dry_mode=False, **kwargs):
    """Create or Update field tags"""
    
    """
    Examples:
    
    dlstats fetchers tag -f BIS -d CNFS -S 
    dlstats fetchers tag -f BEA -d "10101 Ann" -S
    dlstats fetchers tag -f BEA -d "10101 Ann" -S
    dlstats fetchers tag -f Eurostat -d nama_10_a10 -S
    dlstats fetchers tag -f OECD -d MEI -S
    
    """

    ctx = client.Context(**kwargs)

    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
        
        db = ctx.mongo_database()
        
        if fetcher:
            fetchers = [fetcher]
        else:
            fetchers = FETCHERS.keys()
            
        for provider in fetchers:

            provider_name = provider
            
            _update_tags(ctx, db, provider_name, dataset, max_bulk, update_only, dry_mode, async_mode)

        
@cli.command('tags-aggs', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_quiet
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_mongo_url
@opt_async_mode
@click.option('--max-bulk', '-M', 
              type=click.INT,
              default=100, 
              show_default=True,
              help='Max Bulk')
@click.option('-u', '--update-only', is_flag=True, 
              help='Update only if not tags in document')
def cmd_aggregate_tags(max_bulk=100, update_only=False, async_mode=None, 
                       **kwargs):
    """Create or Update field tags"""

    ctx = client.Context(**kwargs)

    ctx.log("START aggregate tags")
    
    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
        
        start = time.time()
        
        db = ctx.mongo_database()

        ctx.log("Aggregate Datasets tags...")
        result = tags.aggregate_tags_datasets(db, max_bulk=max_bulk)
        #{'nMatched': 410, 'nModified': 385, 'writeConcernErrors': [], 'nInserted': 0, 'nUpserted': 2452, 'writeErrors': [], 'nRemoved': 0}
        ctx.log_ok("datasets - matched[%(nMatched)s] - modified[%(nModified)s] - inserted[%(nInserted)s] - upserted[%(nUpserted)s]" % result)
        if result["writeErrors"]:
            print(result["writeErrors"][0])

        ctx.log("Aggregate Series tags...")
        result = tags.aggregate_tags_series(db, max_bulk=max_bulk)
        ctx.log_ok("series - matched[%(nMatched)s] - modified[%(nModified)s] - inserted[%(nInserted)s] - upserted[%(nUpserted)s]" % result)
        if result["writeErrors"]:
            print(result["writeErrors"][0])

        end = time.time() - start
        
        ctx.log("END aggregate tags time[%.3f]" % end)        

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
    
    provider_name = None
    if fetcher:
        f = FETCHERS[fetcher](db=db)        
        provider_name = f.provider_name        

    result, query = tags.search_tags(db,
                               search_type=search_type, 
                               provider_name=provider_name, 
                               dataset_code=dataset, 
                               frequency=frequency, 
                               search_tags=search, 
                               limit=limit)
    
    ctx.log("Count result : %s" % result.count())
    for doc in result:
        if search_type == constants.COL_SERIES:
            fields = [doc['provider_name'], doc['dataset_code'], doc['key'], doc['name']]
        else:
            fields = [doc['provider_name'], doc['dataset_code'], doc['name']]
        if ctx.debug:
            fields.append(doc['tags'])
            
        print(fields)

@cli.command('consolidate', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_quiet
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_logger_file
@client.opt_mongo_url
@opt_fetcher
@opt_dataset
@click.option('--max-bulk', '-M', 
              type=click.INT,
              default=20, 
              show_default=True,
              help='Max Bulk')
def cmd_consolidate(fetcher=None, dataset=None, max_bulk=20, **kwargs):
    """Consolidate codelists and concepts one or more dataset"""
    
    ctx = client.Context(**kwargs)

    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
        
        db = ctx.mongo_database()
        
        _consolidate(ctx, db, fetcher, dataset=dataset, max_bulk=max_bulk)
        
            
@cli.command('purge', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_quiet
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_mongo_url
@opt_fetcher
@opt_dataset_multiple
@click.option('--purge-all', is_flag=True,
              help='Delete provider and categories')
def cmd_purge(fetcher=None, dataset=None, purge_all=False, **kwargs):
    """Purge one or more dataset"""
    
    """
    dlstats fetchers purge -f INSEE --purge-all
    dlstats fetchers purge -f INSEE -d IPCH-2015-FR-COICOP
    dlstats fetchers purge -f INSEE -d IPCH-2015-FR-COICOP -d IPC-2015-COICOP
    """

    ctx = client.Context(**kwargs)

    ctx.log("START purge for [%s]" % fetcher)
    
    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
        
        start = time.time()
        
        db = ctx.mongo_database()
        
        from pymongo import DeleteMany
        
        if purge_all:
            query = {"name": fetcher}
            result = db[constants.COL_PROVIDERS].bulk_write([DeleteMany(query)],
                                                            ordered=False)
            ctx.log("Provider [%s] deleted" % fetcher)
            
            query = {"provider_name": fetcher}
            result = db[constants.COL_CATEGORIES].bulk_write([DeleteMany(query)],
                                                             ordered=False)
            
            ctx.log("Categories deleted: %s" % result.deleted_count)
        
        query = {"provider_name": fetcher}
        if not purge_all and dataset:
            query["dataset_code"] = {"$in": dataset}
        
        bulk_requests = [DeleteMany(query)]

        result = db[constants.COL_DATASETS].bulk_write(bulk_requests,
                                                       ordered=False)
        ctx.log("Datasets deleted: %s" % result.deleted_count)
        
        result = db[constants.COL_SERIES].bulk_write(bulk_requests,
                                                     ordered=False)
        ctx.log("Series deleted: %s" % result.deleted_count)

        end = time.time() - start
        
        ctx.log("END purge for [%s] - time[%.3f]" % (fetcher, end))
