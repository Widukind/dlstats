# -*- coding: utf-8 -*-

import sys
import click

from dlstats import constants
from dlstats import client
from dlstats import utils
from dlstats.tasks import export_files
from dlstats.fetchers import FETCHERS

opt_provider = click.option('--provider', '-p', 
              required=True, 
              type=click.Choice(FETCHERS.keys()), 
              help='Provider Name')

opt_dataset = click.option('--dataset', '-d', 
              required=True, 
              help='Run selected dataset only')

opt_export_filepath = click.option('--filepath', '-P', 
                               type=click.Path(exists=False),
                               #required=True, 
                               help='Export filepath')

@click.group()
def cli():
    """Export File commands."""
    pass

@cli.command('csvfile', context_settings=client.DLSTATS_SETTINGS)
@client.opt_verbose
@client.opt_silent
@client.opt_debug
@client.opt_logger
@client.opt_logger_conf
@client.opt_mongo_url
@opt_provider
@opt_dataset
@opt_export_filepath
@click.option('--create', is_flag=True,
              help='Create csv file if not exist.')
def cmd_export_csvfile(provider=None, dataset=None, filepath=None, 
                       create=False, **kwargs):
    """Download csvfile from one dataset. 

    Examples:
    
    dlstats export csvfile -p Eurostat -d "nama_10_a10" -S
    dlstats export csvfile -p BEA -d "10101 Ann"
    
    widukind-dataset-Eurostat-nama_10_a10-csv
    widukind-dataset-eurostat-nama_10_a10.csv
    """

    ctx = client.Context(**kwargs)

    if ctx.silent or click.confirm('Do you want to continue?', abort=True):
        
        db = ctx.mongo_database()
        fs = ctx.mongo_fs()
        
        filename = export_files.generate_filename_csv(provider_name=provider,
                                           dataset_code=dataset,
                                           prefix="dataset")
        filepath = filepath or filename
        
        csvfile = fs.find_one({"filename": filename})
        
        if not csvfile and create is True:
            ctx.log_warn("%s not exist. creating..." % filename)
            try:
                id = export_files.export_file_csv_dataset_unit(provider=provider, 
                                                               dataset_code=dataset)
                csvfile = fs.get(id)
            except Exception as err:
                ctx.log_error(str(err))
        
        if csvfile:

            created = csvfile.upload_date.strftime("%Y-%m-%d-%H:%M:%S")            
            ctx.log_ok("export to %s - created[%s]" % (filepath, created))
            
            with open(filepath, 'wb') as fp:
                rows = iter(csvfile)
                for row in rows:
                    fp.write(row)
        else:
            ctx.log_error("file not found: %s" % filename)
