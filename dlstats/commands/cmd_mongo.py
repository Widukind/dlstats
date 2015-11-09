# -*- coding: utf-8 -*-

import time
from pprint import pprint

import click

from dlstats import constants
from dlstats import client
from dlstats.fetchers import schemas

#TODO: move to schemas module
CURRENT_SCHEMAS = {
    constants.COL_PROVIDERS: schemas.provider_schema,
    constants.COL_CATEGORIES: schemas.category_schema,
    constants.COL_DATASETS: schemas.dataset_schema,
    constants.COL_SERIES: schemas.series_schema,
}


@click.group()
def cli():
    """MongoDB commands."""
    pass

@cli.command('reindex', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_mongo_url
def cmd_reindex(**kwargs):
    """Reindex collections"""

    ctx = client.Context(**kwargs)
    ctx.log_warn("All Writes operations is blocked pending run !")
    
    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
    
        db = ctx.mongo_database()
        with click.progressbar(constants.COL_ALL,
                               length=len(constants.COL_ALL),
                               label='Reindex collections') as collections:
            for collection in collections:
                print(" [%s]" % collection)
                _col = db[collection]
                _col.reindex()

@cli.command('check', context_settings=client.DLSTATS_SETTINGS)
@client.opt_mongo_url
@client.opt_verbose
@client.opt_debug
@client.opt_pretty
@client.opt_logger
@client.opt_logger_conf
def cmd_check(**kwargs):
    """Verify connection"""
    ctx = client.Context(**kwargs)
    try:
        import pymongo
        mongo_client = ctx.mongo_client()
        db = ctx.mongo_database()
        server_info = mongo_client.server_info()
        host_info = db.command("hostInfo")        
        print("------------------------------------------------------")
        ctx.log_ok("Connection OK")
        print("------------------------------------------------------")
        print("pymongo version : %s" % pymongo.version)
        print("-------------------- Server Infos --------------------")
        pprint(server_info)
        print("-------------------- Host Infos ----------------------")
        pprint(host_info)
        print("------------------------------------------------------")
    except Exception as err:
        ctx.log_error("Connection Error:")
        print("------------------------------------------------------")
        ctx.log_error(str(err))
        print("------------------------------------------------------")
        

#TODO: @cli.command('copydb', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_mongo_url
def cmd_copy_db(**kwargs):
    """Copy database to other database"""
    ctx = client.Context(**kwargs)
    ctx.log_error("Not Implemented")

#TODO: @cli.command('backup', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_mongo_url
def cmd_backup(**kwargs):
    """Backup database or collection(s)"""
    ctx = client.Context(**kwargs)
    ctx.log_error("Not Implemented")

#TODO: @cli.command('restore', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_mongo_url
def cmd_restore(**kwargs):
    """Restore database or collection(s)"""
    ctx = client.Context(**kwargs)
    ctx.log_error("Not Implemented")

#TODO: @cli.command('report', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_mongo_url
def cmd_report(**kwargs):
    """Technical statistic report"""
    ctx = client.Context(**kwargs)
    ctx.log_error("Not Implemented")

#TODO: exclude/include col
#TODO: limit sample docs à un échantillon
@cli.command('check-schemas', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_mongo_url
@click.option('--max-errors', '-M', default=0, type=int, show_default=True)
def cmd_check_schemas(max_errors=None, **kwargs):
    """Check datas in DB with schemas
    """
    ctx = client.Context(**kwargs)
    ctx.log_warn("Attention, opération très longue")
    
    # dlstats mongo check-schemas --mongo-url mongodb://localhost/widukind -M 20 -S
    
    report = {}
    
    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
        
        start = time.time()
        
        db = ctx.mongo_database()
        from pymongo import ReadPreference

        for col in CURRENT_SCHEMAS.keys():
            print("check %s..." % col)
            
            report[col] = {'error': 0, 'verified': 0, 'time': 0}
            report[col]['count'] = db[col].count()
            
            s = time.time()
            
            _schema = CURRENT_SCHEMAS[col]
            
            #coll2 = coll1.with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)
            #find(limit=0)
            #projection={‘_id’: False}
            for doc in db[col].with_options(read_preference=ReadPreference.SECONDARY_PREFERRED).find():
                id = None
                if max_errors and report[col]['error'] >= max_errors:
                    ctx.log_warn("Max error attempt. Skip test !")
                    break
                try:
                    report[col]['verified'] += 1
                    id = str(doc.pop('_id'))
                    _schema(doc)
                except Exception as err:
                    report[col]['error'] += 1
                    if ctx.verbose:
                        ctx.log_error("%s - %s - %s" % (col, id, str(err)))
                    
            report[col]['time'] = "%.3f" % (time.time() - s)
        
        end = time.time() - start

        fmt = "{0:20} | {1:10} | {2:10} | {3:10} | {4:10}"
        print("--------------------------------------------------------------------")
        print(fmt.format("Collection", "Count", "Verified", "Errors", "Time"))
        for col, item in report.items():
            print(fmt.format(col, item['count'], item['verified'], item['error'], item['time']))
        print("--------------------------------------------------------------------")
        print("time elapsed : %.3f seconds " % end)
             
        """
        --------------------------------------------------------------------
        Collection           | Count      | Verified   | Errors     | Time
        providers            |          5 |          5 |          0 | 0.001
        datasets             |         23 |         23 |         17 | 0.017
        series               |     315032 |     315032 |       8786 | 208.991
        categories           |       6875 |       6875 |         37 | 1.022
        --------------------------------------------------------------------
        time elapsed : 210.042 seconds        
        """
    
@cli.command('clean', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_mongo_url
def cmd_clean(**kwargs):
    """Delete MongoDB collections"""

    ctx = client.Context(**kwargs)
    #TODO: translation
    ctx.log_warn("La destruction des données est définitive !")
    
    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
    
        db = ctx.mongo_database()
        
        for col in constants.COL_ALL:
            try:
                print("delete collection [%s]..." % col)
                db.drop_collection(col)
            except:
                pass
