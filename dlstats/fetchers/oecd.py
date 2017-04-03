# -*- coding: utf-8 -*-

import logging

import requests

from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats.utils import Downloader, clean_datetime
from dlstats.xml_utils import (XMLStructure_2_0 as XMLStructure,
                               XMLGenericData_2_0_OECD as XMLData,
                               dataset_converter,
                               select_dimension,
                               get_dimensions_from_dsd)

"""
FIXME: Attention à EO dont le dataset NAME change à chaque publication !
- use hook dataset mais attention à series qui use eo
"""

VERSION = 2

logger = logging.getLogger(__name__)

DATASETS = {
    'MEI': {
        'name': 'Main Economics Indicators',
        'doc_href': 'http://www.oecd-ilibrary.org/economics/data/main-economic-indicators_mei-data-en',
        'sdmx_filter': "LOCATION"
    },
    'EO': {
        'name': 'Economic Outlook',
        'doc_href': 'http://www.oecd.org/eco/outlook/',
        'sdmx_filter': "LOCATION"
    },
}

FREQUENCIES_SUPPORTED = ['A', 'Q', 'M']
FREQUENCIES_REJECTED = []

class OECD(Fetcher):

    def __init__(self, **kwargs):
        super().__init__(provider_name='OECD', version=VERSION, **kwargs)

        self.provider = Providers(name=self.provider_name,
                                  long_name='Organisation for Economic Co-operation and Development',
                                  version=VERSION,
                                  region='World',
                                  website='http://www.oecd.org',
                                  terms_of_use='http://www.oecd.org/termsandconditions/',
                                  fetcher=self)

        self.requests_client = requests.Session()

    def build_data_tree(self):

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

    def upsert_dataset(self, dataset_code):

        if not DATASETS.get(dataset_code):
            raise Exception("This dataset is unknown" + dataset_code)

        dataset = Datasets(provider_name=self.provider_name,
                           dataset_code=dataset_code,
                           name=DATASETS[dataset_code]['name'],
                           doc_href=DATASETS[dataset_code]['doc_href'],
                           fetcher=self)
        dataset.last_update = clean_datetime()

        dataset.series.data_iterator = OECD_Data(dataset,
                                                 sdmx_filter=DATASETS[dataset_code]['sdmx_filter'])

        return dataset.update_database()


"""
Pour EO: générer le last_update d'après name:
    <KeyFamily id="EO79_TRADE" agencyID="OECD">
        <Name xml:lang="en">
            Economic Outlook No 79 - June 2006 - Annual Trade and Payments Projections
        </Name>
    </KeyFamily>
"""

class OECD_Data(SeriesIterator):

    def __init__(self, dataset=None, sdmx_filter=None):
        super().__init__(dataset)

        self.real_dataset_code = self.dataset_code

        self.sdmx_filter = sdmx_filter

        self.store_path = self.get_store_path()
        self.xml_dsd = XMLStructure(provider_name=self.provider_name)

        self._load_dsd()
        self.rows = self._get_data_by_dimension()

    def _get_url_dsd(self):
        """
        if self.dataset_code == "EO":
            self.dataset_code = "EO95_LTB"
            return "http://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/EO95_LTB"
        """
        return "http://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/%s" % self.dataset_code

    def _get_url_data(self):
        """
        if self.dataset_code == "EO":
            self.dataset_code = "EO95_LTB"
            return "http://stats.oecd.org/restsdmx/sdmx.ashx/GetData/EO95_LTB"

        db.datasets.bulkWrite([{ deleteMany:  { "filter" : {provider_name: "OECD", dataset_code: "EO"} } }], { ordered : false })
        db.series.bulkWrite([{ deleteMany:  { "filter" : {provider_name: "OECD", dataset_code: "EO"} } }], { ordered : false })
        """
        return "http://stats.oecd.org/restsdmx/sdmx.ashx/GetData/%s" % self.dataset_code

    def _load_dsd(self):
        url = self._get_url_dsd()
        download = Downloader(store_filepath=self.store_path,
                              url=url,
                              filename="dsd-%s.xml" % self.dataset_code,
                              use_existing_file=self.fetcher.use_existing_file,
                              client=self.fetcher.requests_client)
        filepath = download.get_filepath()
        self.fetcher.for_delete.append(filepath)

        self.xml_dsd.process(filepath)
        self._set_dataset()

    def _set_dataset(self):

        dataset = dataset_converter(self.xml_dsd, self.dataset_code)
        self.dataset.dimension_keys = dataset["dimension_keys"]
        self.dataset.attribute_keys = dataset["attribute_keys"]
        self.dataset.concepts = dataset["concepts"]
        self.dataset.codelists = dataset["codelists"]

    def _get_dimensions_from_dsd(self):
        return get_dimensions_from_dsd(self.xml_dsd, self.provider_name, self.dataset_code)

    def _get_data_by_dimension(self):

        self.xml_data = XMLData(provider_name=self.provider_name,
                                dataset_code=self.dataset_code,
                                xml_dsd=self.xml_dsd,
                                dsd_id=self.dataset_code,
                                frequencies_supported=FREQUENCIES_SUPPORTED)

        dimension_keys, dimensions = self._get_dimensions_from_dsd()

        position, _key, dimension_values = select_dimension(dimension_keys, dimensions, choice="max")

        count_dimensions = len(dimension_keys)

        for dimension_value in dimension_values:

            sdmx_key = []
            for i in range(count_dimensions):
                if i == position:
                    sdmx_key.append(dimension_value)
                else:
                    sdmx_key.append(".")
            key = "".join(sdmx_key)

            url = "%s/%s" % (self._get_url_data(), key)
            filename = "data-%s-%s.xml" % (self.dataset_code, key.replace(".", "_"))
            download = Downloader(url=url,
                                  filename=filename,
                                  store_filepath=self.store_path,
                                  client=self.fetcher.requests_client
                                  )
            filepath, response = download.get_filepath_and_response()

            if filepath:
                self.fetcher.for_delete.append(filepath)

            if response.status_code >= 400 and response.status_code < 500:
                continue
            elif response.status_code >= 500:
                raise response.raise_for_status()

            for row, err in self.xml_data.process(filepath):
                yield row, err

            #self.dataset.update_database(save_only=True)

        yield None, None

    def build_series(self, bson):
        bson["last_update"] = self.dataset.last_update
        bson["dataset_code"] = self.real_dataset_code
        self.dataset.add_frequency(bson["frequency"])
        return bson


