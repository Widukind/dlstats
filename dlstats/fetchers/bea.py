# -*- coding: utf-8 -*-
"""
Created on Thu Sep 10 11:35:26 2015

@author: salimeh
"""

from collections import OrderedDict
import itertools
from datetime import datetime
import zipfile
import logging

import xlrd
import pandas

from widukind_common import errors

from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats.utils import Downloader, clean_datetime
from dlstats import constants

VERSION = 2

logger = logging.getLogger(__name__)

CATEGORIES = {
    "national": {
        "name": "National Data",
        "doc_href": "http://www.bea.gov/national/index.htm",
        "parent": None,
        "all_parents": None,
    },
    "international": {
        "name": "International Data",
        "doc_href": "http://www.bea.gov/international/index.htm",
        "parent": None,
        "all_parents": None,
    },
    "industry": {
        "name": "Industry Data",
        "doc_href": "http://www.bea.gov/industry/index.htm",
        "parent": None,
        "all_parents": None,
    },
    #"gdp": {
    #    "name": "GDP by Industry",
    #    "parent": "international",
    #    "all_parents": ["international"],
    #    "doc_href": None,
    #},
    "nipa": {
        "name": "National Income and Product Accounts",
        "parent": "national",
        "all_parents": ["national"],
        "doc_href": "http://www.bea.gov/iTable/index_nipa.cfm"
    },
    "nipa-underlying": {
        "name": "Underlying",
        "parent": "national",
        "all_parents": ["national"],
        "doc_href": "http://www.bea.gov/national/index.htm"
    },
    "fa2004": {
        "name": "Fixed Assets",
        "parent": "national",
        "all_parents": ["national"],
        "doc_href": "http://www.bea.gov/iTable/index_FA.cfm"
    },
    "nipa-underlying-section0": {
        "name": "NIPA - Underlying - Section 0",
        "parent": "nipa-underlying",
        "all_parents": ["national", "nipa-underlying"],
        "url": "https://www.bea.gov/national/nipaweb/nipa_underlying/SS_Data/Section0All_xls.zip",
        "doc_href": None
    },
    "nipa-underlying-section2": {
        "name": "NIPA - Underlying - Section 2",
        "parent": "nipa-underlying",
        "all_parents": ["national", "nipa-underlying"],
        "url": "https://www.bea.gov/national/nipaweb/nipa_underlying/SS_Data/Section2All_xls.zip",
        "doc_href": None
    },
    "nipa-underlying-section3": {
        "name": "NIPA - Underlying - Section 3",
        "parent": "nipa-underlying",
        "all_parents": ["national", "nipa-underlying"],
        "url": "https://www.bea.gov/national/nipaweb/nipa_underlying/SS_Data/Section3All_xls.zip",
        "doc_href": None
    },
    "nipa-underlying-section4": {
        "name": "NIPA - Underlying - Section 4",
        "parent": "nipa-underlying",
        "all_parents": ["national", "nipa-underlying"],
        "url": "https://www.bea.gov/national/nipaweb/nipa_underlying/SS_Data/Section4All_xls.zip",
        "doc_href": None
    },
    "nipa-underlying-section5": {
        "name": "NIPA - Underlying - Section 5",
        "parent": "nipa-underlying",
        "all_parents": ["national", "nipa-underlying"],
        "url": "https://www.bea.gov/national/nipaweb/nipa_underlying/SS_Data/Section5All_xls.zip",
        "doc_href": None
    },
    "nipa-underlying-section7": {
        "name": "NIPA - Underlying - Section 7",
        "parent": "nipa-underlying",
        "all_parents": ["national", "nipa-underlying"],
        "url": "https://www.bea.gov/national/nipaweb/nipa_underlying/SS_Data/Section7All_xls.zip",
        "doc_href": None
    },
    "nipa-underlying-section9": {
        "name": "NIPA - Underlying - Section 9",
        "parent": "nipa-underlying",
        "all_parents": ["national", "nipa-underlying"],
        "url": "https://www.bea.gov/national/nipaweb/nipa_underlying/SS_Data/Section9All_xls.zip",
        "doc_href": None
    },
    "fa2004-section1": {
        "name": "SECTION 1 - FIXED ASSETS AND CONSUMER DURABLE GOODS",
        "parent": "fa2004",
        "all_parents": ["national", "fa2004"],
        "url": "https://www.bea.gov/national/FA2004/SS_Data/Section1All_xls.zip",
        "doc_href": None
    },
    "fa2004-section2": {
        "name": "SECTION 2 - PRIVATE FIXED ASSETS BY TYPE",
        "parent": "fa2004",
        "all_parents": ["national", "fa2004"],
        "url": "https://www.bea.gov/national/FA2004/SS_Data/Section2All_xls.zip",
        "doc_href": None
    },
    "fa2004-section3": {
        "name": "SECTION 3 - PRIVATE FIXED ASSETS BY INDUSTRY",
        "parent": "fa2004",
        "all_parents": ["national", "fa2004"],
        "url": "https://www.bea.gov/national/FA2004/SS_Data/Section3All_xls.zip",
        "doc_href": None
    },
    "fa2004-section4": {
        "name": "SECTION 4 - NONRESIDENTIAL FIXED ASSETS",
        "parent": "fa2004",
        "all_parents": ["national", "fa2004"],
        "url": "https://www.bea.gov/national/FA2004/SS_Data/Section4All_xls.zip",
        "doc_href": None
    },
    "fa2004-section5": {
        "name": "SECTION 5 - RESIDENTIAL FIXED ASSETS",
        "parent": "fa2004",
        "all_parents": ["national", "fa2004"],
        "url": "https://www.bea.gov/national/FA2004/SS_Data/Section5All_xls.zip",
        "doc_href": None
    },
    "fa2004-section6": {
        "name": "SECTION 6 - PRIVATE FIXED ASSETS",
        "parent": "fa2004",
        "all_parents": ["national", "fa2004"],
        "url": "https://www.bea.gov/national/FA2004/SS_Data/Section6All_xls.zip",
        "doc_href": None
    },
    "fa2004-section7": {
        "name": "SECTION 7 - GOVERNMENT FIXED ASSETS",
        "parent": "fa2004",
        "all_parents": ["national", "fa2004"],
        "url": "https://www.bea.gov/national/FA2004/SS_Data/Section7All_xls.zip",
        "doc_href": None
    },
    "fa2004-section8": {
        "name": "SECTION 8 - CONSUMER DURABLE GOODS",
        "parent": "fa2004",
        "all_parents": ["national", "fa2004"],
        "url": "https://www.bea.gov/national/FA2004/SS_Data/Section8All_xls.zip",
        "doc_href": None
    },
    "fa2004-section9": {
        "name": "SECTION 9 - CHAINED DOLLAR TABLES",
        "parent": "fa2004",
        "all_parents": ["national", "fa2004"],
        "url": "https://www.bea.gov/national/FA2004/SS_Data/Section9All_xls.zip",
        "doc_href": None
    },
    "nipa-section1": {
        "name": "NIPA - Section 1 - Domestic Product and Income",
        "parent": "nipa",
        "all_parents": ["national", "nipa"],
        "url": "https://www.bea.gov/national/nipaweb/SS_Data/Section1All_xls.zip",
        "doc_href": None
    },
    "nipa-section2": {
        "name": "NIPA - Section 2 - Personal Income and Outlays",
        "parent": "nipa",
        "all_parents": ["national", "nipa"],
        "url": "https://www.bea.gov/national/nipaweb/SS_Data/Section2All_xls.zip",
        "doc_href": None
    },
    "nipa-section3": {
        "name": "NIPA - Section 3 - Government Current Receipts and Expenditures",
        "parent": "nipa",
        "all_parents": ["national", "nipa"],
        "url": "https://www.bea.gov/national/nipaweb/SS_Data/Section3All_xls.zip",
        "doc_href": None
    },
    "nipa-section4": {
        "name": "NIPA - Section 4 - Foreign Transactions",
        "parent": "nipa",
        "all_parents": ["national", "nipa"],
        "url": "https://www.bea.gov/national/nipaweb/SS_Data/Section4All_xls.zip",
        "doc_href": None
    },
    "nipa-section5": {
        "name": "NIPA - Section 5 - Saving and Investment",
        "parent": "nipa",
        "all_parents": ["national", "nipa"],
        "url": "https://www.bea.gov/national/nipaweb/SS_Data/Section5All_xls.zip",
        "doc_href": None
    },
    "nipa-section6": {
        "name": "NIPA - Section 6 - Income and Employment by Industry",
        "parent": "nipa",
        "all_parents": ["national", "nipa"],
        "url": "https://www.bea.gov/national/nipaweb/SS_Data/Section6All_xls.zip",
        "doc_href": None
    },
    "nipa-section7": {
        "name": "NIPA - Section 7 - Supplemental Tables",
        "parent": "nipa",
        "all_parents": ["national", "nipa"],
        "url": "https://www.bea.gov/national/nipaweb/SS_Data/Section7All_xls.zip",
        "doc_href": None
    },
}

def _get_frequency(sheet_name):
    if 'Qtr' in sheet_name :
        return "Quarterly", "Q"
    elif 'Ann'  in sheet_name or 'Annual' in sheet_name:
        return "Annually", "A"
    elif 'Month'in sheet_name:
        return "Monthly", "M"

    return None, None

class BEA(Fetcher):

    def __init__(self, **kwargs):
        super().__init__(provider_name='BEA', version=VERSION, **kwargs)

        self.provider = Providers(name=self.provider_name ,
                                  long_name='Bureau of Economic Analysis',
                                  region='USA',
                                  version=VERSION,
                                  website='http://www.bea.gov',
                                  terms_of_use='http://www.bea.gov/about/BEAciting.htm',
                                  fetcher=self)

        self._datasets_settings = None
        self._current_urls = {}

    def _get_release_date(self, url, sheet):
        if 'Section' in  url :
            release_datesheet = sheet.cell_value(4,0)[15:] #April 28, 2016
        elif 'ITA-XLS' in url or 'IIP-XLS' in url :
            release_datesheet = sheet.cell_value(3,0)[14:].split('-')[0]
        else :
            release_datesheet = sheet.cell_value(3,0)[14:]

        return clean_datetime(datetime.strptime(release_datesheet.strip(), "%B %d, %Y"))

    def _get_sheet(self, url, filename, sheet_name):

        if url in self._current_urls:
            filepath = self._current_urls[url]
        else:
            download = Downloader(url=url,
                                  filename=filename,
                                  store_filepath=self.store_path,
                                  use_existing_file=self.use_existing_file)

            filepath = download.get_filepath()
            #self.for_delete.append(filepath)
            self._current_urls[url] = filepath

        zipfile_ = zipfile.ZipFile(filepath)
        section = zipfile_.namelist()[0]

        file_contents = zipfile_.read(section)

        excel_book = xlrd.open_workbook(file_contents=file_contents)

        return excel_book.sheet_by_name(sheet_name)

    def upsert_dataset(self, dataset_code):

        settings = self._get_datasets_settings()[dataset_code]

        dataset = Datasets(provider_name=self.provider_name,
                           dataset_code=dataset_code,
                           name=settings["name"],
                           doc_href='http://www.bea.gov',
                           fetcher=self)

        url = settings["metadata"]["url"]
        filename = settings["metadata"]["filename"]
        sheet_name = settings["metadata"]["sheet_name"]

        sheet = self._get_sheet(url, filename, sheet_name)
        fetcher_data = BeaData(dataset, url=url, sheet=sheet)

        if dataset.last_update and fetcher_data.release_date >= dataset.last_update and not self.force_update:
            comments = "update-date[%s]" % fetcher_data.release_date
            raise errors.RejectUpdatedDataset(provider_name=self.provider_name,
                                              dataset_code=dataset_code,
                                              comments=comments)


        dataset.last_update = fetcher_data.release_date
        dataset.series.data_iterator = fetcher_data

        return dataset.update_database()

    def _get_datasets_settings(self):
        if not self._datasets_settings:
            self._datasets_settings = dict([(d["dataset_code"], d) for d in self.datasets_list()])
        return self._datasets_settings

    def load_datasets_first(self):
        self._get_datasets_settings()
        return super().load_datasets_first()

    def build_data_tree(self):

        categories = []

        for category_code, values in sorted(CATEGORIES.items()):
            if "url" in values:
                continue

            cat = {
                "category_code": category_code,
                "name": values["name"],
                "parent": values["parent"],
                "all_parents": values["all_parents"],
                "doc_href": values["doc_href"],
                "datasets": []
            }
            categories.append(cat)

        for category_code, category in sorted(CATEGORIES.items()):
            if not "url" in category:
                continue

            url = category["url"]
            #filename = category["filename"]
            filename = "%s.xls.zip" % category_code

            download = Downloader(url=url,
                                  filename=filename,
                                  store_filepath=self.store_path,
                                  use_existing_file=self.use_existing_file)
            filepath = download.get_filepath()
            #self.for_delete.append(filepath)

            self._current_urls[url] = filepath

            try:
                zipfile_ = zipfile.ZipFile(filepath)
            except Exception as err:
                logger.error("bea zip error - url[%s] - filepath[%s] - error[%s]" % (url, filepath, str(err)))
                continue

            for section in zipfile_.namelist():

                if section in ['Iip_PrevT3a.xls', 'Iip_PrevT3b.xls', 'Iip_PrevT3c.xls']:
                    continue

                file_contents = zipfile_.read(section)
                excel_book = xlrd.open_workbook(file_contents=file_contents)

                try:
                    sheet = excel_book.sheet_by_name('Contents')

                    cat = {
                        "category_code": category_code,
                        "name": category["name"],
                        "parent": category.get("parent"),
                        "all_parents": category.get("all_parents"),
                        "doc_href": None,
                        "datasets": []
                    }

                    dataset_base_names = {}

                    first_line = 0

                    for i, cell in enumerate(sheet.col(1)):
                        if "Code" in cell.value:
                            first_line = i+2
                            break

                    for i, cell in enumerate(sheet.col(1)):
                        if i < first_line:
                            continue

                        cell_row = sheet.row(i)
                        if cell_row[1].value != '':
                            dataset_code = cell_row[1].value
                            dataset_name = cell_row[2].value

                            dataset_base_names[dataset_code] = dataset_name

                    for sheet_name in excel_book.sheet_names():

                        _dataset_code = sheet_name.split()[0]

                        if not _dataset_code in dataset_base_names:
                            continue

                        _dataset_name = dataset_base_names[_dataset_code]

                        frequency_name, frequency_code = _get_frequency(sheet_name)

                        if not frequency_name:
                            msg = "not frequency name for sheet[%s] - url[%s] - filename[%s]" % (sheet_name, url, filename)
                            logger.critical(msg)
                            raise Exception(msg)

                        dataset_code = "%s-%s-%s" % (category_code, _dataset_code, frequency_code.lower())
                        dataset_name = "%s - %s" % (_dataset_name, frequency_name)

                        cat["datasets"].append({
                            "name": dataset_name,
                            "dataset_code": dataset_code,
                            "last_update": self._get_release_date(url, excel_book.sheet_by_name(sheet_name)),
                            "metadata": {
                                "url": url,
                                "filename": filename,
                                "sheet_name": sheet_name
                            }
                        })

                    categories.append(cat)

                except Exception as err:
                    logger.error(str(err))

        return categories

class BeaData(SeriesIterator):

    def __init__(self, dataset, url=None, sheet=None):
        super().__init__(dataset)

        self.url = url
        self.sheet = sheet

        self.dataset.dimension_keys = ['concept', 'frequency']
        self.dataset.concepts["concept"] = "Concept"
        self.dataset.concepts["frequency"] = "Frequency"

        self.dataset.set_dimension_frequency("frequency")

        if not "concept" in self.dataset.codelists:
            self.dataset.codelists["concept"] = {}

        if not "frequency" in self.dataset.codelists:
            self.dataset.codelists["frequency"] = {}

        cell_value = self.sheet.cell_value(2,0) #Annual data from 1969 To 2015
        self.frequency = None

        #retrieve frequency from url
        #if 'AllTablesQTR' in url :
        #    self.frequency = 'Q'
        #elif 'AllTables.' in url :
        #    self.frequency = 'A'

        if 'Qtr' in self.sheet.name :
            self.frequency = 'Q'
        elif 'Ann'  in self.sheet.name or 'Annual' in self.sheet.name:
            self.frequency = 'A'
        elif 'Month'in self.sheet.name:
            self.frequency = 'M'

        if self.frequency is None:
            raise Exception(dataset.name + " " + self.sheet.name + " (" + url + "): frequency can't be found")

        self.years = [int(s) for s in cell_value.split() if s.isdigit()] #[1969, 2015]

        self.release_date = self.fetcher._get_release_date(self.url, self.sheet)
        self.dimensions = {}

        if 'Section' in  self.url :
            row_start = sheet.col_values(0).index(1)
        else:
            col_values_ = [cell.strip(' ') for cell in sheet.col_values(0)]
            if 'A1' in col_values_:
                row_start = col_values_.index('A1')
            else :
                row_start = col_values_.index('1')

        self.row_ranges = list(iter(range(row_start, sheet.nrows)))

        row_notes = self.sheet.row_values(1)
        if row_notes and len(row_notes[0].strip()) > 0:
            self.dataset.notes = row_notes[0].strip()

        self.keys = []
        self.rows = self._get_datas()
        self.last_title = OrderedDict()
        self.name = None

    def _get_datas(self):
        try:
            for i, row_num in enumerate(self.row_ranges):

                row = self.sheet.row_values(row_num)

                key = row[2]
                name = row[1]

                count_space = sum( 1 for _ in itertools.takewhile(str.isspace, name) )
                if i == 0 or count_space == 0:
                    self.last_title = OrderedDict()
                    self.last_title[0] = name.strip()
                else:
                    self.last_title[count_space] = name.strip()

                _name = []
                for c in list(self.last_title.keys()):
                    if c <= count_space:
                        _name.append(self.last_title[c])
                self.name = " - ".join(_name)

                # skip lines without key or with ZZZZZZx key
                if len(key.replace(' ','')) == 0 or key.startswith('ZZZZZZ'):
                    continue
                elif key in self.keys:
                    continue
                else:
                    self.keys.append(key)

                yield row, None
        finally:
            try:
                self.sheet.book.release_resources()
            except Exception as err:
                logger.error(str(err))

    def build_series(self, row):
        dimensions = {}

        series = {}
        series_values = []

        series_key = "%s-%s" % (row[2], self.frequency)
        series_name = "%s - %s" % (self.name, constants.FREQUENCIES_DICT[self.frequency])

        dimensions['concept'] = row[2]
        dimensions['frequency'] = self.frequency

        if not dimensions["frequency"] in self.dataset.codelists["frequency"]:
            self.dataset.codelists["frequency"][dimensions["frequency"]] = constants.FREQUENCIES_DICT[self.frequency]

        if not dimensions["concept"] in self.dataset.codelists["concept"]:
            self.dataset.codelists["concept"][dimensions["concept"]] = self.name

        start_date = pandas.Period(self.years[0], freq=self.frequency)
        end_date = pandas.Period(self.years[1], freq=self.frequency)

        series['provider_name'] = self.provider_name
        series['dataset_code'] = self.dataset_code
        series['name'] = series_name
        series['key'] = series_key
        series['start_date'] = start_date.ordinal
        series['end_date'] = end_date.ordinal
        series['last_update'] = self.release_date
        series['dimensions'] = dimensions
        series['frequency'] = self.frequency
        series['attributes'] = {}

        for v in row[3:]:
            value = {
                'attributes': None,
                'period': str(start_date),
                'value': str(v)
            }
            series_values.append(value)
            start_date += 1

        series['values'] = series_values

        self.dataset.add_frequency(self.frequency)

        return series

