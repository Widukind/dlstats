# -*- coding: utf-8 -*-

from collections import OrderedDict
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
from lxml import etree

from dlstats import constants
from dlstats.utils import Downloader
from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator

__all__ = ['BIS']

VERSION = 3

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
    
    def __init__(self, **kwargs):
        super().__init__(provider_name='BIS', **kwargs)
        
        if not self.provider:
            self.provider = Providers(name=self.provider_name,
                                      long_name='Bank for International Settlements',
                                      version=VERSION,
                                      region='world',
                                      website='http://www.bis.org', 
                                      fetcher=self)
        
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
                                filename=DATASETS[dataset_code]['filename'],
                                frequency=DATASETS[dataset_code]['frequency'],
                                fetcher=self)

        result = None

        if fetcher_data.is_updated():

            dataset.series.data_iterator = fetcher_data
            result = dataset.update_database()

            #TODO: clean datas (file temp)

            end = time.time() - start
            logger.info("upsert dataset[%s] - END - time[%.3f seconds]" % (dataset_code, end))
        else:
            logger.info("upsert dataset[%s] bypass because is updated from release_date[%s]" % (dataset_code, fetcher_data.release_date))

        return result

    def load_datasets_first(self):
        start = time.time()
        logger.info("first load fetcher[%s] - START" % (self.provider_name))
        
        for dataset in self.datasets_list():
            self.upsert_dataset(dataset["dataset_code"])
        
        end = time.time() - start
        logger.info("first load fetcher[%s] - END - time[%.3f seconds]" % (self.provider_name, end))
        
    def load_datasets_update(self):
        self.load_datasets_first()

    def build_data_tree(self, force_update=False):
        
        categories = []
        
        for category_code, dataset in DATASETS.items():
            cat = {
                "category_code": category_code,
                "name": dataset["name"],
                "doc_href": dataset["doc_href"],
                "datasets": [{
                    "name": dataset["name"], 
                    "dataset_code": category_code,
                    "last_update": None, 
                    "metadata": None
                }]
            }
            categories.append(cat)
        
        return categories

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

class BIS_Data(SeriesIterator):
    
    def __init__(self, dataset, url=None, filename=None, 
                 is_autoload=True, frequency=None, fetcher=None):
        super().__init__()
        
        self.dataset = dataset
        self.url = url
        self.filename = filename
        self.frequency = frequency
        self.fetcher = fetcher

        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        
        self.release_date = None
        self.dimension_keys = None
        self.periods = None
        self.start_date = None
        self.end_date = None

        self.rows = None
                
        if is_autoload:
            self._load_datas()
        
    def _load_datas(self, datas=None):
        
        kwargs = {}
        
        if not datas:
            # TODO: timeout, replace
            download = Downloader(url=self.url, filename=self.filename)
            
            filepath = extract_zip_file(download.get_filepath())
            kwargs['filepath'] = filepath
        else:
            kwargs['fileobj'] = io.StringIO(datas, newline="\n")
        
        kwargs['date_format'] = "%a %b %d %H:%M:%S %Z %Y"
        kwargs['headers_line'] = DATASETS[self.dataset.dataset_code]['lines']['headers']
        self._rows, self.headers, self.release_date, self.dimension_keys, self.periods = local_read_csv(**kwargs)
        
        self.dataset.dimension_keys = self.dimension_keys
        
        self.rows = self._process()
        
        self.dataset.last_update = self.release_date
        
        self.start_date = self.fetcher.get_ordinal_from_period(self.periods[0], freq=self.frequency)
        self.end_date = self.fetcher.get_ordinal_from_period(self.periods[-1], freq=self.frequency)

    def is_updated(self):

        dataset_doc = self.dataset.fetcher.db[constants.COL_DATASETS].find_one(
                                                {'provider_name': self.dataset.provider_name,
                                                "dataset_code": self.dataset.dataset_code})
        if not dataset_doc:
            return True

        if self.release_date > dataset_doc['last_update']:
            return True

        return False

    def _process(self):
        for row in self._rows:
            yield csv_dict(self.headers, row), None

        self.dataset.concepts = dict(zip(self.dimension_keys, self.dimension_keys))

        for k, dimensions in self.dimension_list.get_dict().items():
            self.dataset.codelists[k] = dimensions

        #for k, attributes in self.attribute_list.get_dict().items():
        #    self.dataset.codelists[k] = attributes

    def build_series(self, row):
        series_key = row['KEY']

        dimensions = OrderedDict()
        
        for d in self.dimension_keys:
            dim_short_id = row[d].split(":")[0]
            dim_long_id = row[d].split(":")[1]
            dimensions[d] = self.dimension_list.update_entry(d, dim_short_id, dim_long_id)

        series_name = " - ".join([row[d].split(":")[1] for d in self.dimension_keys])

        values = []
        
        for period in self.periods:
            value = {
                'attributes': None,
                'release_date': self.release_date,
                'ordinal': self.fetcher.get_ordinal_from_period(period, freq=self.frequency),
                'period_o': period,
                'period': period,
                'value': row[period]
            }
            values.append(value)
        
        bson = {'provider_name': self.dataset.provider_name,
                'dataset_code': self.dataset.dataset_code,
                'name': series_name,
                'key': series_key,
                'values': values,
                'attributes': None,
                'dimensions': dimensions,
                'last_update': self.release_date,
                'start_date': self.start_date,
                'end_date': self.end_date,
                'frequency': self.frequency}

        return bson
        
