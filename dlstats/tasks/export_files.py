# -*- coding: utf-8 -*-

import tempfile
import time
import logging
import csv

import pandas
import gridfs

from dlstats import utils
from dlstats import constants

logger = logging.getLogger(__name__)

def generate_filename(provider_name=None, dataset_code=None, key=None, prefix=None):
    """Generate filename for file (csv, pdf, ...)
    """    
    if key:
        filename = "widukind-%s-%s-%s-%s" % (prefix, provider_name, dataset_code, key)
    else:
        filename = "widukind-%s-%s-%s" % (prefix, provider_name, dataset_code)
        
    return filename.lower().replace(" ", "-")

def generate_filename_csv(**kwargs):
    return "%s.csv" % generate_filename(**kwargs)

def export_series(series):
    """Export one serie (Period and Frequency only)
    """
    #series = dict (doc mongo)
    sd = pandas.Period(ordinal=series['startDate'],
                       freq=series['frequency'])
    values = []
    values.append(["Date", "Value"])
    for val in series['values']:
        values.append([str(sd), val])
        sd += 1
    return values

def export_dataset(db, dataset):
    """Export all series for one Dataset
    
    Return array - one line by serie    
    """
    #TODO: Utiliser une queue Redis car trop de code en RAM ?
    
    start = time.time()
    
    ck = list(dataset['dimensionList'].keys())
    
    cl = sorted(ck, key=lambda t: t.lower())
    #['freq', 'geo', 'na_item', 'nace_r2', 'unit']
    
    headers = ['key'] + cl    
    #['key', 'freq', 'geo', 'na_item', 'nace_r2', 'unit']
    
    # revient à 0 et -1 ?
    dmin = float('inf')
    dmax = -float('inf')

    series = db[constants.COL_SERIES].find({"provider": dataset['provider'],
                                            "datasetCode": dataset['datasetCode']},
                                           {'revisions': 0, 'releaseDates': 0},
                                           )
    
    for s in series:
        #collect la première et dernière date trouvé
        """
        Permet d'avoir ensuite une plage de date la plus ancienne à la plus récente
        car chaque série n'a pas toujours les mêmes dates
        """
        if s['startDate'] < dmin:
            dmin = s['startDate']
        if s['endDate'] > dmax:
            dmax = s['endDate']
        freq = s['frequency']
        
    series.rewind()

    pDmin = pandas.Period(ordinal=dmin, freq=freq);
    pDmax = pandas.Period(ordinal=dmax, freq=freq);
    headers += list(pandas.period_range(pDmin, pDmax, freq=freq).to_native_types())
    #['key', 'freq', 'geo', 'na_item', 'nace_r2', 'unit', '1995', '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014']

    elements = [headers]
    
    series.rewind()
    
    series_count = series.count()

    def row_process(s):
        row = [s['key']]
        
        for c in cl:
            if c in s['dimensions']:
                row.append(s['dimensions'][c])
            else:
                row.append('')        
        #['A.CLV05_MEUR.A.B1G.HR', 'A', 'HR', 'B1G', 'A', 'CLV05_MEUR']
        
        p_start_date = pandas.Period(ordinal=s['startDate'], freq=freq)        
        p_end_date = pandas.Period(ordinal=s['endDate'], freq=freq)
        
        #pandas.period_range(p_start_date, p_end_date, freq=freq).to_native_types()
        """
        pDmin : pandas.Period() la plus ancienne
        p_start_date-1 : périod en cours -1
            >>> p_start_date -1
            Period('1994', 'A-DEC')
            
            Bug: ne renvoi rien si
                p_start_date -1 devient identique à pDmin
        """

        # Les None sont pour les périodes qui n'ont pas de valeur correspondantes
        #row.extend([None for d in pandas.period_range(pDmin, p_start_date-1, freq=freq)])
        _row = [None for d in pandas.period_range(pDmin, p_start_date-1, freq=freq)]
        row.extend(_row)
        
        #row.extend([val for val in s['values']])
        _row = [val for val in s['values']]
        row.extend(_row)
        #20 entrée
        #['1324.7', '1343.7', '1369.5', '1465.4', '1408.5', '1434.1', '1469.0', '1534.6', '1430.5', '1570.0', '1545.9', '1675.4', '1615.0', '1702.7', '1656.8', '1546.8', '1487.7', '1269.7', '1249.7', '1206.0']
        #row.extend([None for d in pandas.period_range(p_end_date+1, pDmax, freq=freq)])
        _row = [None for d in pandas.period_range(p_end_date+1, pDmax, freq=freq)]
        row.extend(_row)
        
        #print(row)

        return row
    
    #greenlets = []
    for s in series:
        elements.append(row_process(s))
        #greenlets.append(pool.spawn(row_process, s))
    #pool.join()    
    #for g in greenlets:
    #    elements.append(g.value)
    
    end = time.time() - start
    logger.info("export_dataset - %s : %.3f" % (dataset['datasetCode'], end))
    
    return elements


def record_csv_file(db, values, provider_name=None, dataset_code=None, key=None, prefix=None):
    """record gridfs and return mongo id of gridfs entry
    """
    
    fs = gridfs.GridFS(db)

    tmp_filepath = tempfile.mkstemp(suffix=".csv", 
                                    prefix='widukind_%s' % prefix, 
                                    text=True)[1]
    
    #TODO: utf8 ?
    #TODO: headers
    with open(tmp_filepath, 'w', newline='', encoding='utf8') as fp:
        writer = csv.writer(fp, quoting=csv.QUOTE_NONNUMERIC)
        for v in values:
            writer.writerow(v)

    filename = "%s.csv" % generate_filename(provider_name=provider_name, 
                                 dataset_code=dataset_code, 
                                 key=key, 
                                 prefix=prefix)

    metadata = {
        "doc_type": prefix,
        "provider": provider_name,
        "datasetCode": dataset_code
    }
    if key: metadata['key'] = key    

    grid_in = fs.new_file(filename=filename, 
                          contentType="text/csv", 
                          metadata=metadata,
                          encoding='utf8')
    
    with open(tmp_filepath, 'r') as fp:
        rows = iter(fp)
        for row in rows:
            grid_in.write(row)
        
    grid_in.close()
    return grid_in._id
    #id = str(grid_in._id)
    #grid_in = None
    #return id

def export_file_csv_series_unit(doc=None, provider=None, dataset_code=None, key=None):
    """Create CSV File from one series and record in MongoDB GridFS
    """

    db = utils.get_mongo_db()

    if not doc:
        if not provider:
            raise ValueError("provider is required")
        if not dataset_code:
            raise ValueError("dataset_code is required")
        if not key:
            raise ValueError("key is required")

        query = {}
        query['provider'] = provider
        query['datasetCode'] = dataset_code
        query['key'] = key
    
        doc = db[constants.COL_SERIES].find_one(query,{'revisions': 0})
        
    if not doc:
        raise Exception("Series not found : %s" % key)
    
    values = export_series(doc)

    return record_csv_file(db,
                         values, 
                         provider_name=doc['provider'],
                         dataset_code=doc["datasetCode"],
                         key=doc["key"], 
                         prefix="series")

def export_file_csv_dataset_unit(doc=None, provider=None, dataset_code=None):
    """Create CSV File from one Dataset and record in MongoDB GridFS
    """

    db = utils.get_mongo_db()
    
    if not doc:
        if not provider:
            raise ValueError("provider is required")
        if not dataset_code:
            raise ValueError("dataset_code is required")
    
        query = {}
        query['provider'] = provider
        query['datasetCode'] = dataset_code
    
        doc = db[constants.COL_DATASETS].find_one(query, {'revisions': 0})
    
    if not doc:
        raise Exception("Document not found for provider[%s] - dataset[%s]" % (provider, dataset_code))
    
    values = export_dataset(db, doc)
    
    id = record_csv_file(db,
                         values, 
                         provider_name=doc['provider'],
                         dataset_code=doc["datasetCode"], 
                         prefix="dataset")
    return id

def export_file_csv_dataset(provider=None, dataset_code=None):
    """Create CSV File from one or more Dataset and record in MongoDB GridFS
    """
    
    db = utils.get_mongo_db()
    
    query = {}
    if provider:
        query['provider'] = provider
    if dataset_code:
        query['datasetCode'] = dataset_code

    datasets = db[constants.COL_DATASETS].find(query,
                               {'revisions': 0})

    ids = []
    for doc in datasets:
        id = export_file_csv_dataset_unit(doc=doc)
        ids.append(str(id))
        
    return ids
    
