# -*- coding: utf-8 -*-

import os
import io
import zipfile
import csv
import datetime
import tempfile
import time
import logging
import re

import pytz
import pandas
from lxml import etree

from dlstats import constants
from dlstats.utils import Downloader
from dlstats.fetchers._commons import Fetcher, Datasets, Providers

__all__ = ['BIS']

VERSION = 2

logger = logging.getLogger(__name__)

def extract_zip_file(filepath):
    """Extract first file in zip file and return absolute path for the file extracted
    
    :param str filepath: Absolute file path of zip file
    
    Example: 
        file1.zip contains one file: file1.csv
    
    >>> extract_zip_file('/tmp/file1.zip')
    '/tmp/file1.csv'
        
    """
    zfile = zipfile.ZipFile(filepath)
    filename = zfile.namelist()[0]
    return zfile.extract(filename, os.path.dirname(filepath))

def csv_dict(headers, array_line):
    """Convert list1 (keys), list2 (values) to dict()
    """
    return dict(zip(headers, array_line))

def local_read_csv(filepath=None, fileobj=None, 
                   headers_line=4, date_format="%a %b %d %H:%M:%S %Z %Y"):
    """CSV reader for bad CSV format (BIS, ?)
    
    Return:
    
        - rows: _csv.reader for iterations - current line is first data line
        - headers: list of headers (Time Period replaced by KEY)
        - release_date: datetime.datetime() instance
        - dimension_keys: List of dimension keys
        - periods: List of periods

    >>> filepath = '/tmp/full_bis_lbs_diss_csv.csv'
    >>> rows, headers, release_date, dimension_keys, periods = local_read_csv(filepath=filepath)
    >>> line1 = csv_dict(headers, next(rows))
    
    """
    if filepath:
        _file = open(filepath)
    else:
        _file = fileobj
    
    rows = csv.reader(_file)
    release_date_txt = None
    dimension_keys = []
    headers = []
    periods = []        
    
    for i in range(headers_line):
        line = next(rows)
        if line and "Retrieved on" in line:
            release_date_txt = line[1]
        if rows.line_num == headers_line:
            break
    
    release_date = datetime.datetime.strptime(release_date_txt, date_format)
    headers_list = next(rows)
    headers_list_copy = headers_list.copy()
    
    for h in headers_list:
        if h == "Time Period":
            headers_list_copy.pop(0)
            break
        dimension_keys.append(headers_list_copy.pop(0))
        
    periods = headers_list_copy    
    headers = dimension_keys + ["KEY"] + periods
    return rows, headers, release_date, dimension_keys, periods

PROVIDER_NAME = "BIS"

#TODO: not implemented calendar/datasets:
"""
- Derivatives statistics OTC: http://www.bis.org/statistics/derstats.htm
- Derivatives statistics Exchange-traded: http://www.bis.org/statistics/extderiv.htm
- Global liquidity indicators: http://www.bis.org/statistics/gli.htm
- Property prices Detailed data: http://www.bis.org/statistics/pp_detailed.htm
- BIS Statistical Bulletin: http://www.bis.org/statistics/bulletin.htm
"""

DATASETS = {
    'LBS-DISS': { 
        'name': 'Locational Banking Statistics - disseminated data',
        'agenda_titles': ['Banking statistics Locational'],
        'doc_href': 'http://www.bis.org/statistics/bankstats.htm',
        'url': 'http://www.bis.org/statistics/full_bis_lbs_diss_csv.zip',
        'filename': 'full_bis_lbs_diss_csv.zip',
        'frequency': 'Q',
        'lines': {
            'release_date': 1,
            'headers': 7
        }
    },
    'CBS': { 
        'name': 'Consolidated banking statistics',
        'agenda_titles': ['Banking statistics Consolidated'],
        'doc_href': 'http://www.bis.org/statistics/consstats.htm',
        'url': 'https://www.bis.org/statistics/full_bis_cbs_csv.zip',
        'filename': 'full_bis_cbs_csv.zip',
        'frequency': 'Q',
        'lines': {
            'release_date': 1,
            'headers': 8
        }
    },
    'DSS': {
        'name': 'Debt securities statistics',
        # same data set for 'Internationa' and 'Domestic and total'
        'agenda_titles': ['Debt securities statistics International', 'Debt securities statistics Domestic and total'],
        'doc_href': 'http://www.bis.org/statistics/secstats.htm',
        'url': 'https://www.bis.org/statistics/full_bis_debt_sec2_csv.zip',
        'filename': 'full_bis_debt_sec2_csv.zip',
        'frequency': 'Q',
        'lines': {
            'release_date': 1,
            'headers': 10
        }
    },     
    'CNFS': {
        'name': 'Credit to the non-financial sector',
        'agenda_titles': ['Credit to non-financial sector'],
        'doc_href': 'http://www.bis.org/statistics/credtopriv.htm',
        'url': 'https://www.bis.org/statistics/full_bis_total_credit_csv.zip',
        'filename': 'full_bis_total_credit_csv.zip',
        'frequency': 'Q',
        'lines': {
            'release_date': 1,
            'headers': 5
        }
    },     
    'DSRP': {
        'name': 'Debt service ratios for the private non-financial sector',
        'agenda_titles': ['Debt service ratio'],
        'doc_href': 'http://www.bis.org/statistics/dsr.htm',
        'url': 'https://www.bis.org/statistics/full_bis_dsr_csv.zip',
        'filename': 'full_bis_dsr_csv.zip',
        'frequency': 'Q',
        'lines': {
            'release_date': 1,
            'headers': 7
        }
    },  
    'PP-SS': {
        'name': 'Property prices - selected series',
        'agenda_titles': ['Property prices Selected'],
        'doc_href': 'http://www.bis.org/statistics/pp_detailed.htm',
        'url': 'https://www.bis.org/statistics/full_bis_selected_pp_csv.zip',
        'filename': 'full_bis_selected_pp_csv.zip',
        'frequency': 'Q',
        'lines': {
            'release_date': 1,
            'headers': 5
        }
    },     
    'PP-LS': {
        'name': 'Property prices - long series',
        'agenda_titles': ['Property prices long'],
        'doc_href': 'http://www.bis.org/statistics/pp_long.htm',
        'url': 'https://www.bis.org/statistics/full_bis_long_pp_csv.zip',
        'filename': 'full_bis_long_pp_csv.zip',
        'frequency': 'Q',
        'lines': {
            'release_date': 1,
            'headers': 6
        }
    },     
    'EERI': {
        'name': 'Effective exchange rate indices',
        'agenda_titles': ['Effective exchange rates'],
        'doc_href': 'http://www.bis.org/statistics/eer/index.htm',
        'url': 'https://www.bis.org/statistics/full_bis_eer_csv.zip',
        'filename': 'full_bis_eer_csv.zip',
        'frequency': 'M',
        'lines': {
            'release_date': 1,
            'headers': 4
        }
    },
}

AGENDA = {'url': 'http://www.bis.org/statistics/relcal.htm?m=6|37|68',
          'filename': 'agenda.html',
          'country': 'ch'
}

def get_agenda():
    download = Downloader(url=AGENDA['url'],
                          filename=AGENDA['filename'])
    with open(download.get_filepath(), 'rb') as fp:
        return fp.read()

class BIS(Fetcher):
    
    def __init__(self, db=None):
        super().__init__(provider_name='BIS', db=db)
        
        if not self.provider:
            self.provider = Providers(name=self.provider_name,
                                      long_name='Bank for International Settlements',
                                      version=VERSION,
                                      region='world',
                                      website='http://www.bis.org', 
                                      fetcher=self)
            self.provider.update_database()
        
        if self.provider.version != VERSION:
            self.provider.update_database()

    def upsert_dataset(self, dataset_code):
        
        start = time.time()
        
        logger.info("upsert dataset[%s] - START" % (dataset_code))
        
        if not DATASETS.get(dataset_code):
            raise Exception("This dataset is unknown" + dataset_code)
        
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code, 
                           name=DATASETS[dataset_code]['name'], 
                           doc_href=DATASETS[dataset_code]['doc_href'],
                           fetcher=self)
        
        fetcher_data = BIS_Data(dataset, 
                                url=DATASETS[dataset_code]['url'], 
                                filename=DATASETS[dataset_code]['filename'])

        if fetcher_data.is_updated():

            dataset.series.data_iterator = fetcher_data
            dataset.update_database()

            #TODO: clean datas (file temp)

            end = time.time() - start
            logger.info("upsert dataset[%s] - END-BEFORE-METAS - time[%.3f seconds]" % (dataset_code, end))

            self.update_metas(dataset_code)

            end = time.time() - start
            logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))
        else:
            logger.info("upsert dataset[%s] bypass because is updated from release_date[%s]" % (dataset_code, fetcher_data.release_date))

    def load_datasets_first(self):
        start = time.time()
        logger.info("first load fetcher[%s] - START" % (self.provider_name))
        
        for dataset_code in DATASETS.keys():
            self.upsert_dataset(dataset_code) 

        end = time.time() - start
        logger.info("first load fetcher[%s] - END - time[%.3f seconds]" % (self.provider_name, end))
        
    def load_datasets_update(self):
        self.load_datasets_first()

    def build_data_tree(self, force_update=False):
        
        if self.provider.count_data_tree() > 1 and not force_update:
            return self.provider.data_tree

        for category_code, dataset in DATASETS.items():
            category_key = self.provider.add_category({"name": dataset["name"],
                                                       "category_code": category_code,
                                                       "doc_href": dataset["doc_href"]})
            _dataset = {"name": dataset["name"], "dataset_code": category_code}
            self.provider.add_dataset(_dataset, category_key)
        
        return self.provider.data_tree
            
    def parse_agenda(self):
        
        agenda = etree.HTML(get_agenda())
        table = agenda.find('.//table')
        # only one table
        rows = table[0].findall('tr')
        # skipping first row
        cells = rows[1].findall('td')
        agenda = []
        months = [None, None]
        
        for c in rows[1].iterfind('td'):
            content = c.find('strong')
            if content.text is None:
                content = content.find('strong')
            months.append(datetime.datetime.strptime(content.text,'%B %Y'))
        agenda.append(months)
        ir = 2
        
        def get_links_text(cell):
            txt = []
            for link in cell.findall('a'):
                if link.text:
                    txt.append(link.text)
            return txt

        def _get_dates(cells):
            item = []
            for ic, c in enumerate(cells):
                if c.text[0] != chr(160):
                    item.append(re.match('\d\d|\d',c.text).group(0))
                else:
                    item.append(None)
            return item
        
        while ir < len(rows):
            cells = rows[ir].findall('td')
            
            content = cells[0]
            if content.text is None:
                content = content.find('a')
            item = [content.text]
            
            if cells[0].get('rowspan') == '2':
                two_rows = True
                content = cells[1].find('a')
                item.append(content.text)
                offset = 2
            else:
                two_rows = False
                item.append(None)
                offset = 1
            
            item.extend(_get_dates(cells[offset:]))
            
            agenda.append(item)
            ir += 1
            
            if two_rows:
                cells = rows[ir].findall('td')
                links = get_links_text(cells[0])
                for content in links:
                    item = [item[0]]
                    item.append(content)
                    item.extend(_get_dates(cells[1:]))
                    agenda.append(item)
                ir += 1
        return agenda

    def get_calendar(self):
        agenda = self.parse_agenda()

        dataset_codes = [d["dataset_code"] for d in self.datasets_list()]

        '''First line - exclude first 2 columns (title1, title2)'''
        months = agenda[0][2:]

        '''All line moins first list'''
        periods = agenda[1:]

        def _get_dataset_code(title):
            for key, d in DATASETS.items():
                if title in d.get("agenda_titles", []):
                    return key
            return None

        for period in periods:
            title = period[0]
            if period[1]:
                title = "%s %s" % (title, period[1])

            dataset_code = _get_dataset_code(title)
            if not dataset_code:
                logger.info("exclude calendar action for not implemented dataset[%s]" % title)
                continue
            if not dataset_code in dataset_codes:
                logger.info("exclude calendar action for dataset[%s]" % title)
                continue

            days = period[2:]
            scheds = [d for d in zip(months, days) if not d[1] is None]

            for date_base, day in scheds:
                yield {'action': "update_node",
                       "kwargs": {"provider_name": self.provider_name,
                                "dataset_code": dataset_code},
                       "period_type": "date",
                       "period_kwargs": {"run_date": datetime.datetime(date_base.year,
                                                                       date_base.month,
                                                                       int(day), 8, 0, 0),
                                         "timezone": pytz.country_timezones(AGENDA['country'])}
                     }

class BIS_Data():
    
    def __init__(self, dataset, url=None, filename=None, store_filepath=None, is_autoload=True):

        self.dataset = dataset
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        
        self.url = url
        self.filename = filename
        self.store_filepath = store_filepath
        
        self.frequency = 'Q'
        
        self.release_date = None
        self.dimension_keys = None
        self.periods = None
        self.start_date = None
        self.end_date = None

        self.rows = None
                
        if is_autoload:
            self._load_datas()
        
    def get_store_path(self):
        return self.store_filepath or os.path.abspath(os.path.join(
                                                                tempfile.gettempdir(), 
                                                                self.dataset.provider_name, 
                                                                self.dataset.dataset_code))
        
    def _load_datas(self, datas=None):
        
        kwargs = {}
        
        if not datas:
            store_filepath = self.get_store_path()
            # TODO: timeout, replace
            download = Downloader(url=self.url, filename=self.filename, store_filepath=store_filepath)
            
            filepath = extract_zip_file(download.get_filepath())
            kwargs['filepath'] = filepath
        else:
            kwargs['fileobj'] = io.StringIO(datas, newline="\n")
        
        kwargs['date_format'] = "%a %b %d %H:%M:%S %Z %Y"
        kwargs['headers_line'] = DATASETS[self.dataset.dataset_code]['lines']['headers']
        self.rows, self.headers, self.release_date, self.dimension_keys, self.periods = local_read_csv(**kwargs)
        
        self.dataset.last_update = self.release_date
        
        self.start_date = pandas.Period(self.periods[0], freq=self.frequency)
        self.end_date = pandas.Period(self.periods[-1], freq=self.frequency)

    def is_updated(self):

        dataset_doc = self.dataset.fetcher.db[constants.COL_DATASETS].find_one(
                                                {'provider_name': self.dataset.provider_name,
                                                "dataset_code": self.dataset.dataset_code})
        if not dataset_doc:
            return True

        if self.release_date > dataset_doc['last_update']:
            return True

        return False

    def __next__(self):
        row = csv_dict(self.headers, next(self.rows)) 
        series = self.build_serie(row)
        if series is None:
            #TODO: close self.rows and delete file ?
            raise StopIteration()
        return(series)
    
    def build_serie(self, row):
        """Build one serie
        
        Return instance of :class:`dict`
        """
        series_key = row['KEY']
        
        logger.debug("provider[%s] - dataset[%s] - serie[%s]" % (self.dataset.provider_name,
                                                                 self.dataset.dataset_code,
                                                                 series_key))

        values = [row[period] for period in self.periods]
        dimensions = {}
        
        for d in self.dimension_keys:
            dim_short_id = row[d].split(":")[0]
            dim_long_id = row[d].split(":")[1]
            dimensions[d] = self.dimension_list.update_entry(d, dim_short_id, dim_long_id)

        series_name = " - ".join([row[d].split(":")[1] for d in self.dimension_keys])
        
        data = {'provider_name': self.dataset.provider_name,
                'dataset_code': self.dataset.dataset_code,
                'name': series_name,
                'key': series_key,
                'values': values,
                'attributes': {},
                'dimensions': dimensions,
                'last_update': self.release_date,
                'start_date': self.start_date.ordinal,
                'end_date': self.end_date.ordinal,
                'frequency': self.frequency}

        return(data)
        
    
def download_all_sources():
    """Download all datasets files (if not exist) and store local temp directory
    
    Store in /[TMP_DIR]/[PROVIDER_NAME]/[DATASET_CODE]/[FILENAME]
    
    return a dict with key is filename and value is full filepath
    """
    
    filepaths = {}
    
    for dataset_code, dataset in DATASETS.items():
        store_filepath = os.path.abspath(os.path.join(tempfile.gettempdir(), PROVIDER_NAME, dataset_code))
        download = Downloader(url=dataset['url'], filename=dataset['filename'], store_filepath=store_filepath)# TODO:, timeout, replace)
        filepaths[dataset['filename']] = os.path.abspath(os.path.join(store_filepath, dataset['filename']))
        logger.info("Download file[%s]" % download.get_filepath())
        
    return filepaths
        
if __name__ == '__main__':
    b = BIS()
    b.get_calendar_()
