# -*- coding: utf-8 -*-

import time
import os
import tempfile
from operator import itemgetter
from datetime import datetime
import logging
import pprint
from collections import OrderedDict
from itertools import groupby

import pymongo
from pymongo import ReturnDocument
from pymongo import InsertOne, UpdateOne
from slugify import slugify
import pandas

from widukind_common.utils import get_mongo_db, create_or_update_indexes

from dlstats import constants
from dlstats.fetchers import schemas
from dlstats import errors
from dlstats.utils import (last_error, 
                           clean_datetime, 
                           remove_file_and_dir, 
                           make_store_path,
                           get_year)

logger = logging.getLogger(__name__)

IS_SCHEMAS_VALIDATION_DISABLE = constants.SCHEMAS_VALIDATION_DISABLE == "true"

class Fetcher(object):
    """Abstract base class for all fetchers"""
    
    def __init__(self, 
                 provider_name=None, 
                 db=None, 
                 is_indexes=True,
                 version=0,
                 max_errors=5,
                 use_existing_file=False,
                 not_remove_files=False,
                 **kwargs):
        """
        :param str provider_name: Provider Name
        :param pymongo.database.Database db: MongoDB Database instance        
        :param bool is_indexes: Bypass create_or_update_indexes() if False 

        :raises ValueError: if provider_name is None
        """        
        if not provider_name:
            raise ValueError("provider_name is required")

        self.provider_name = provider_name
        self.db = db or get_mongo_db()
        self.version = version
        self.max_errors = max_errors
        self.use_existing_file = use_existing_file
        self.not_remove_files = not_remove_files
        
        self.provider = None
        
        self.errors = 0

        self.categories_filter = [] #[category_code]
        self.datasets_filter = []   #[dataset_code]
        
        self.selected_datasets = {}
        
        self.store_path = os.path.abspath(os.path.join(tempfile.gettempdir(), 
                                                       self.provider_name))
        self.for_delete = []
        
        if is_indexes:
            create_or_update_indexes(self.db)
            
        if IS_SCHEMAS_VALIDATION_DISABLE:
            logger.warning("schemas validation is disable")
    
    def upsert_data_tree(self, data_tree=None, force_update=False):
        #TODO: bulk

        if data_tree and not isinstance(data_tree, list):
            raise TypeError("data_tree is not instance of list")

        if not data_tree or force_update:
            data_tree = self.build_data_tree()
            Categories.remove_all(self.provider_name, db=self.db)

        results = []            
        for data in data_tree:
            cat = Categories(fetcher=self, **data)
            results.append(cat.update_database())
            
        return results

    def get_selected_datasets(self, force=False):
        
        if self.selected_datasets and not force:
            return self.selected_datasets  

        if Categories.count(self.provider_name, db=self.db) == 0:
            self.upsert_data_tree()
        
        query = {
            "datasets.0": {"$exists": True}
        }
        
        #TODO: enable ?
        if self.categories_filter:
            query["$or"] = [
                {"category_code": {"$in": self.categories_filter}},
                {"all_parents": {"$in": self.categories_filter}},
            ]

        categories = Categories.categories(self.provider_name, 
                                           db=self.db, **query)
        for category in categories.values():
            for d in category["datasets"]:
                if self.datasets_filter and not d['dataset_code'] in self.datasets_filter:
                    continue
                self.selected_datasets[d['dataset_code']] = d
        
        return self.selected_datasets

    def datasets_list(self):
        self.get_selected_datasets()
        datasets = self.selected_datasets.values()
        return sorted(datasets, key=itemgetter("dataset_code"))

    def provider_verify(self):
        
        if self.provider.from_db:
            return
        
        query = {"name": self.provider_name}
        
        provider = self.db[constants.COL_PROVIDERS].find_one(query)
        if not provider:
            self.provider.update_database()
        
        self.provider = self.load_provider_from_db()
    
        if self.provider.version != self.version:
            self.provider.update_database()
            
    def load_provider_from_db(self):
        """Load and set provider fields from DB
        """
        query = {"name": self.provider_name}
        doc = self.db[constants.COL_PROVIDERS].find_one(query)
        if not doc:
            return

        doc.pop('_id')
        return Providers(fetcher=self, from_db=True, **doc)
    
    def upsert_all_datasets(self):
        start = time.time()        
        msg_op = "update"
        
        self.provider_verify()
        
        try:
            query = {"provider_name": self.provider_name}
            
            if self.db[constants.COL_DATASETS].count(query) == 0:
                msg_op = "load"
                msg = "fetcher load START: provider[%s]"
                logger.info(msg % self.provider_name)
                return self.load_datasets_first()
            else:
                msg = "fetcher update START: provider[%s]"
                logger.info(msg % self.provider_name)
                return self.load_datasets_update()

        except Exception:
            msg = "fetcher %s ERROR: provider[%s] - error[%s]"
            logger.critical(msg % (msg_op, self.provider_name, last_error()))

        finally:
            end = time.time() - start
            msg = "fetcher %s END: provider[%s] - time[%.3f seconds]"
            logger.info(msg % (msg_op, self.provider_name, end))
        
    def wrap_upsert_dataset(self, dataset_code):

        start = time.time()
        msg = " dataset upsert START: provider[%s] - dataset[%s]"
        logger.info(msg % (self.provider_name, dataset_code))

        self.provider_verify()

        try:
            query = {"provider_name": self.provider_name,
                     "dataset_code": dataset_code}
            projection = {"lock": True}
            dataset_doc = self.db[constants.COL_DATASETS].find_one(query, 
                                                                   projection)
            if dataset_doc and dataset_doc["lock"] is True:
                raise errors.LockedDataset("dataset is locked",
                                           provider_name=self.provider_name,
                                           dataset_code=dataset_code)

            return self.upsert_dataset(dataset_code)

        except errors.RejectUpdatedDataset:
            msg = "Reject dataset updated for provider[%s] - dataset[%s]"
            logger.info(msg % (self.provider_name, dataset_code))
        finally:
            end = time.time() - start
            msg = "dataset upsert END: provider[%s] - dataset[%s] - time[%.3f seconds]"
            logger.info(msg % (self.provider_name, dataset_code, end))
        
    def _hook_remove_temp_files(self, dataset):
        if dataset and dataset.for_delete and not self.not_remove_files:
            for filepath in dataset.for_delete:
                try:
                    remove_file_and_dir(filepath)
                except Exception:
                    logger.warning("not remove filepath[%s]" % filepath)

        if not self.not_remove_files:
            for filepath in self.for_delete:
                try:
                    remove_file_and_dir(filepath)
                except Exception:
                    logger.warning("not remove filepath[%s]" % filepath)
    
    def hook_before_dataset(self, dataset):
        pass

    def hook_after_dataset(self, dataset):
        self._hook_remove_temp_files(dataset)

    def load_datasets_first(self):

        for dataset in self.datasets_list():
            dataset_code = dataset["dataset_code"]
            try:
                self.wrap_upsert_dataset(dataset_code)
            except Exception as err:
                if isinstance(err, errors.MaxErrors):
                    raise
                msg = "error for provider[%s] - dataset[%s]: %s"
                logger.critical(msg % (self.provider_name, 
                                       dataset_code, 
                                       str(err)))

    def load_datasets_update(self):
        #TODO: log and/or warning
        return self.load_datasets_first()

    def build_data_tree(self):
        raise NotImplementedError()
    
    def get_calendar(self):
        raise NotImplementedError()

    def upsert_dataset(self, dataset_code):
        """Upsert a dataset in MongoDB
        
        :param str dataset_code: ID of :class:`Datasets`
        """        
        raise NotImplementedError("This method from the Fetcher class must"
                                  "be implemented.")
        
class DlstatsCollection(object):
    """Abstract base class for objects that are stored and indexed by dlstats
    """
    
    def __init__(self, fetcher=None):
        """
        :param Fetcher fetcher: Fetcher instance 

        :raises ValueError: if fetcher is None
        :raises TypeError: if not instance of :class:`Fetcher`  
        """
        
        if not fetcher:
            raise ValueError("fetcher is required")

        if not isinstance(fetcher, Fetcher):
            raise TypeError("Bad type for fetcher")
        
        self.fetcher = fetcher
        
    def update_mongo_collection(self, collection, keys, bson):
        """Update one document

        :param str collection: Collection name
        :param list keys: List of value for unique key
        :param dict bson: Document values
        :param int log_level: Default logging level
        
        :return: Instance of :class:`bson.objectid.ObjectId`  
        """
        key = {k: bson[k] for k in keys}
        result = None
        try:
            result = self.fetcher.db[collection].find_one_and_replace(key, bson, upsert=True,
                                                                      return_document=ReturnDocument.AFTER)
            result = result['_id']
        except Exception:
            logger.critical('%s.update_database() failed for %s error[%s]' % (collection, str(key), last_error()))
            return None
        else:
            logger.debug(collection + ' ' + str(key) + ' updated.')
            return result
        
class Providers(DlstatsCollection):
    """Providers class
    
    Inherit from :class:`DlstatsCollection`
    """

    def __init__(self,
                 name=None,
                 long_name=None,
                 version=0,
                 region=None,
                 website=None,
                 metadata=None,
                 enable=True,
                 lock=False,
                 from_db=None,
                 slug=None,
                 fetcher=None):
        """        
        :param str name: Provider Short Name
        :param str long_name: Provider Long Name
        :param str region: Region
        :param str website: Provider Web Site
        :param Fetcher fetcher: Fetcher instance
        """        
        super().__init__(fetcher=fetcher)
        self.name = name
        self.long_name = long_name
        self.version = version
        self.region = region
        self.website = website
        self.metadata = metadata
        self.enable = enable
        self.lock = lock
        self.from_db = from_db

        self.validate = schemas.provider_schema(self.bson)

    def __repr__(self):
        return pprint.pformat([(key, self.validate[key]) for key in sorted(self.validate.keys()) if not key in ["data_tree", "slug"]])

    def slug(self):
        if not self.name:
            return 
        return slugify(self.name, word_boundary=False, save_order=True)

    @property
    def bson(self):
        return {'name': self.name,
                'long_name': self.long_name,
                'version': self.version,
                'slug': self.slug(),
                'region': self.region,
                'website': self.website,
                'metadata': self.metadata,
                "enable": self.enable,
                "lock": self.lock}

    def update_database(self):
        schemas.provider_schema(self.bson)
        return self.update_mongo_collection(constants.COL_PROVIDERS, 
                                            ['name'], 
                                            self.bson)

class Categories(DlstatsCollection):
    """Categories class
    
    Inherit from :class:`DlstatsCollection`
    """
    
    def __init__(self,
                 provider_name=None,
                 category_code=None,
                 name=None,
                 position=0,
                 parent=None,
                 all_parents=None,
                 datasets=[],
                 doc_href=None,
                 metadata=None,
                 fetcher=None,
                 **kwargs):
        """
        :param str provider_name: Provider name
        :param str category_code: Unique Category Code
        :param str name: Category name
        :param list datasets: Array of dataset_code        
        :param str doc_href: (Optional) Category - web link
        :param Fetcher fetcher: Fetcher instance
        """        
        super().__init__(fetcher=fetcher)
        self.provider_name = provider_name
        if not provider_name:
            self.provider_name = self.fetcher.provider_name
        
        self.category_code = category_code
        self.name = name
        self.position = position
        self.parent = parent
        self.all_parents = all_parents
        self.datasets = datasets
        self.doc_href = doc_href
        self.metadata = metadata

        self.enable = True
        self.lock = False

    def slug(self):
        txt = "-".join([self.provider_name, self.category_code])
        return slugify(txt, word_boundary=False, save_order=True)

    @property
    def bson(self):
        return {'provider_name': self.provider_name,
                'category_code': self.category_code,
                'name': self.name,
                'position': self.position,
                'parent': self.parent,
                'all_parents': self.all_parents,
                'slug': self.slug(),
                'datasets': self.datasets,
                'doc_href': self.doc_href,
                'metadata': self.metadata,
                "enable": self.enable,
                "lock": self.lock}

    @classmethod
    def categories(cls, provider_name, db=None, **query):
        db = db or get_mongo_db()
        if not "provider_name" in query:
            query["provider_name"] = provider_name
        cursor = db[constants.COL_CATEGORIES].find(query)
        return dict([(doc["category_code"], doc) for doc in cursor])

    @classmethod
    def count(cls, provider_name, db=None):
        db = db or get_mongo_db()
        query = {"provider_name": provider_name}
        return db[constants.COL_CATEGORIES].count(query)

    @classmethod
    def remove_all(cls, provider_name, db=None):
        db = db or get_mongo_db()
        query = {"provider_name": provider_name}
        logger.info("remove all categories for [%s]" % provider_name)
        return db[constants.COL_CATEGORIES].remove(query)
    
    @classmethod
    def search_category_for_dataset(cls, provider_name, dataset_code, db=None):
        db = db or get_mongo_db()
        query = {"provider_name": provider_name,
                 "datasets.0": {"$exists": True},
                 "datasets.dataset_code": dataset_code}
        return db[constants.COL_CATEGORIES].find_one(query)

    @classmethod
    def root_categories(cls, provider_name, db=None):
        db = db or get_mongo_db()
        query = {"provider_name": provider_name, "parent": None}
        cursor = db[constants.COL_CATEGORIES].find(query)
        return cursor.sort([("position", 1), ("category_code", 1)])
    
    @classmethod
    def _iter_parent(cls, category, categories):
        if not categories:
            categories = cls.categories()
        parents_keys = []
        if category.get("parent"):
            parent_id = category.get("parent")
            parents_keys.append(parent_id)
            parent_category = categories.get(parent_id)
            if parent_category:
                parents_keys.extend(cls._iter_parent(parent_category, 
                                                     categories))
        return parents_keys

    @classmethod
    def iter_parent(cls, category, categories):
        """Recursive function for retrieve all parents
        for one category
        
        Return array of category_code for all parents
        """
        parents_keys = cls._iter_parent(category, categories)
        parents_keys.reverse()
        return parents_keys
        
    def update_database(self):
        schemas.category_schema(self.bson)
        return self.update_mongo_collection(constants.COL_CATEGORIES, 
                                            ['provider_name', 'category_code'],
                                            self.bson)
class Datasets(DlstatsCollection):
    """Abstract base class for datasets
    
    Inherit from :class:`DlstatsCollection`
    """
    
    def __init__(self, 
                 provider_name=None,
                 dataset_code=None, 
                 name=None,
                 doc_href=None,
                 last_update=None,
                 metadata=None,
                 bulk_size=100,
                 fetcher=None, 
                 is_load_previous_version=True,
                 **kwargs):
        """
        :param str provider_name: Provider name
        :param str dataset_code: Dataset code
        :param str name: Dataset name
        :param str doc_href: Dataset link
        :param int bulk_size: Batch size for mongo bulk
        :param datetime.datetime last_update: Dataset Last updated 
        :param Fetcher fetcher: Fetcher instance
        :param bool is_load_previous_version: Bypass load previous version if False        
        """        
        super().__init__(fetcher=fetcher)
        self.provider_name = provider_name
        self.dataset_code = dataset_code
        self.name = name
        self.doc_href = doc_href
        self.last_update = last_update
        self.metadata = metadata or {}
        self.bulk_size = bulk_size
        self.dimension_list = CodeDict()
        self.attribute_list = CodeDict()
        
        self.dimension_keys = []
        self.attribute_keys = []
        self.codelists = {}
        self.concepts = {}

        self.enable = False        
        self.lock = False
        
        self.tags = []

        self.download_first = None
        self.download_last = None

        self.for_delete = []

        self.from_db = False
        if is_load_previous_version:
            self.load_previous_version(provider_name, dataset_code)
            
        self.notes = None
        
        self.series = Series(dataset=self,
                             provider_name=self.provider_name, 
                             dataset_code=self.dataset_code, 
                             last_update=self.last_update, 
                             bulk_size=self.bulk_size, 
                             fetcher=self.fetcher)

    def __repr__(self):
        return pprint.pformat([('provider_name', self.provider_name),
                               ('dataset_code', self.dataset_code)])
        
    def slug(self):
        txt = "-".join([self.provider_name, self.dataset_code])
        return slugify(txt, word_boundary=False, save_order=True)
        
    @property
    def bson(self):
        return {'provider_name': self.provider_name,
                'name': self.name,
                'dataset_code': self.dataset_code,
                'slug': self.slug(),
                'dimension_keys': self.dimension_keys,
                'attribute_keys': self.attribute_keys,
                'codelists': self.codelists,
                'concepts': self.concepts,
                'metadata': self.metadata,
                'doc_href': self.doc_href,
                'last_update': self.last_update,
                'download_first': self.download_first,
                'download_last': self.download_last,
                'notes': self.notes,
                "enable": self.enable,
                "lock": self.lock,
                "tags": self.tags}

    def load_previous_version(self, provider_name, dataset_code):
        dataset = self.fetcher.db[constants.COL_DATASETS].find_one(
                                            {'provider_name': provider_name,
                                             'dataset_code': dataset_code})
        if dataset:
            # convert to dict of dict
            self.dimension_keys = dataset.get("dimension_keys", [])
            self.attribute_keys = dataset.get("attribute_keys", [])
            self.codelists = dataset.get("codelists", {})
            self.concepts = dataset.get("concepts", {})
            self.metadata = dataset.get('metadata', {}) or {}
            self.doc_href = dataset.get('doc_href')
            self.last_update = dataset.get('last_update')
            self.download_last = dataset.get('download_last')
            self.download_first = dataset.get('download_first')
            self.notes = dataset.get('notes')
            self.enable = dataset.get('enable')
            self.lock = dataset.get('lock')
            self.tags = dataset.get('tags')
            
            dimension_list = {}
            attribute_list = {}
            
            if not self.codelists:
                msg = "load previous version fail. provider[%s] - dataset[%s]"
                raise Exception(msg % (self.provider_name, self.dataset_code))

            for key in self.dimension_keys:
                dimension_list[key] = OrderedDict([(k, v) for k, v in self.codelists.get(key).items() if key in self.codelists])

            for key in self.attribute_keys:
                attribute_list[key] = OrderedDict([(k, v) for k, v in self.codelists.get(key).items() if key in self.codelists])
            
            self.dimension_list.set_dict(dimension_list)
            self.attribute_list.set_dict(attribute_list)
            
            self.from_db = True
            
        else:
            msg = "dataset not found for previous loading. provider[%s] - dataset[%s]"
            logger.warning(msg % (provider_name, dataset_code))

    def add_frequency(self, frequency):
        if not frequency:
            return
        if not self.metadata:
            self.metadata = {}
        if not "frequencies" in self.metadata:
            self.metadata["frequencies"] = []
        if not frequency in self.metadata["frequencies"]:
            self.metadata["frequencies"].append(frequency)
    
    def is_recordable(self):

        if self.fetcher.max_errors and self.fetcher.errors >= self.fetcher.max_errors:
            return False
        
        if self.fetcher.db[constants.COL_PROVIDERS].count({"name": self.provider_name}) == 0:
            logger.critical("provider[%s] not found in DB" % self.provider_name)
            return False
        
        query = {"provider_name": self.provider_name,
                 "dataset_code": self.dataset_code}
        if self.fetcher.db[constants.COL_SERIES].count(query) > 0:
            return True

        return False
        
    def update_database(self, save_only=False):

        self.fetcher.hook_before_dataset(self)
        
        try:
            if not save_only:
                self.series.process_series_data()
        except Exception:
            self.fetcher.errors += 1
            logger.critical(last_error())
            if self.fetcher.max_errors and self.fetcher.errors >= self.fetcher.max_errors:
                msg = "The maximum number of errors is exceeded for provider[%s] - dataset[%s]. MAX[%s]"
                raise errors.MaxErrors(msg % (self.provider_name,
                                              self.dataset_code,
                                              self.fetcher.max_errors))
        finally:
            now = clean_datetime()
    
            if not self.download_first:
                self.download_first = now
    
            self.download_last = now
    
            schemas.dataset_schema(self.bson)
            
            if not self.is_recordable():
                self.enable = False
                msg = "disable dataset[%s] for provider[%s]"
                logger.warning(msg % (self.dataset_code, 
                                      self.provider_name))
            else:
                self.enable = True

            if logger.isEnabledFor(logging.INFO):    
                msg_stats = "STATS dataset-update: provider[%s] - dataset[%s] - accepts[%s] - rejects[%s] - inserts[%s] - updates[%s]"
                logger.info(msg_stats % (self.provider_name,
                                         self.dataset_code,
                                         self.series.count_accepts,
                                         self.series.count_rejects,
                                         self.series.count_inserts,
                                         self.series.count_updates))
            
            if save_only:
                self.series.reset_counters()
                        
            result = self.update_mongo_collection(constants.COL_DATASETS,
                                                  ['provider_name', 
                                                   'dataset_code'],
                                                  self.bson)
    
            self.fetcher.hook_after_dataset(self)
    
            return result

class SeriesIterator:
    """Base class for all Fetcher data class
    """
    
    def __init__(self, dataset):
        """
        :param Datasets dataset: Datasets instance
        """
        
        if not isinstance(dataset, Datasets):
            raise TypeError("dataset is not instance of Datasets")
        
        self.dataset = dataset
        self.fetcher = self.dataset.fetcher
        self.dataset_code = self.dataset.dataset_code
        self.provider_name = self.fetcher.provider_name
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        
        self.rows = None
        
    def get_store_path(self):
        return make_store_path(base_path=self.fetcher.store_path,
                               dataset_code=self.dataset_code)

    def __next__(self):
        bson, err = next(self.rows)
        if err:
            return err
        
        if not bson:
            raise StopIteration()

        try:
            return self.clean_field(self.build_series(bson))
        except Exception as err:
            return err

    def clean_field(self, bson):

        if not "start_ts" in bson or not bson.get("start_ts"):
            if bson["frequency"] == "A":
                year = int(get_year(bson["values"][0]["period"]))
                bson["start_ts"] = clean_datetime(datetime(year, 1, 1), rm_hour=True, rm_minute=True, rm_second=True, rm_microsecond=True, rm_tzinfo=True) 
            else:
                bson["start_ts"] = clean_datetime(pandas.Period(ordinal=bson["start_date"], freq=bson["frequency"]).start_time.to_datetime())

        if not "end_ts" in bson or not bson.get("end_ts"):
            if bson["frequency"] == "A":
                year = int(get_year(bson["values"][-1]["period"]))
                bson["end_ts"] = clean_datetime(datetime(year, 12, 31), rm_hour=True, rm_minute=True, rm_second=True, rm_microsecond=True, rm_tzinfo=True) 
            else:
                bson["end_ts"] = clean_datetime(pandas.Period(ordinal=bson["end_date"], freq=bson["frequency"]).end_time.to_datetime())
        
        dimensions = bson.pop("dimensions")
        attributes = bson.pop("attributes", {})
        new_dimensions = {}
        new_attributes = {}
        
        for key, value in dimensions.items():
            new_dimensions[slugify(key, save_order=True)] = slugify(value, save_order=True)

        if attributes:
            for key, value in attributes.items():
                new_attributes[slugify(key, save_order=True)] = slugify(value, save_order=True)
            
        bson["dimensions"] = new_dimensions

        if attributes:
            bson["attributes"] = new_attributes
        else:
            bson["attributes"] = None
            
        for value in bson["values"]:
            if not value.get("attributes"):
                continue
            attributes_obs = {}
            for k, v in value.get("attributes").items():
                attributes_obs[slugify(k, save_order=True)] = slugify(v, save_order=True)
            value["attributes"] = attributes_obs
        
        return bson

    def build_series(self, bson):
        raise NotImplementedError()

def series_set_release_date(bson, last_update):
    
    if not bson or not isinstance(bson, dict):
        raise ValueError("no bson or not dict instance")            
    
    if not last_update or not isinstance(last_update, datetime):
        raise ValueError("no last_update or not datetime instance")            
    
    if not "values" in bson:
        raise ValueError("not values field in bson")
    
    for obs in bson["values"]:
        if not obs.get("release_date"):
            obs["release_date"] = last_update

def series_revisions(new_bson, old_bson, last_update):
    
    if not new_bson or not isinstance(new_bson, dict):
        raise ValueError("no new_bson or not dict instance")            

    if not old_bson or not isinstance(old_bson, dict):
        raise ValueError("no old_bson or not dict instance")            
    
    if not last_update or not isinstance(last_update, datetime):
        raise ValueError("no last_update or not datetime instance")            

    if not "values" in new_bson:
        raise ValueError("not values field in new_bson")

    if not "values" in old_bson:
        raise ValueError("not values field in old_bson")
    
    old_values = old_bson["values"]
    changed = False
    
    old_values_by_periods = {}    
    for old_value in old_values:
        old_values_by_periods[old_value["period"]] = old_value
    
    keyfunc = lambda x: x["ordinal"]
    groups = []
    uniquekeys = []
    data = sorted(old_bson["values"] + new_bson["values"], key=keyfunc)

    for k, g in groupby(data, keyfunc):
        groups.append(list(g))
        uniquekeys.append(k)

    new_bson["values"] = []

    for group in groups:
        if len(group) == 1:
            new_bson["values"].append(group[0])
            continue
        
        old_obs = group[0]
        new_obs = group[1]

        '''load old revisions if exists'''        
        if "revisions" in old_obs:
            new_obs["revisions"] = old_obs["revisions"] 
        
        '''Search if new revision'''
        is_new_revision = False
        
        '''is value change'''
        if old_obs["value"] != new_obs["value"]:
            is_new_revision = True
            
        '''is attributes change'''
        if old_obs.get("attributes") != new_obs.get("attributes"):
            is_new_revision = True
        
        if not is_new_revision:
            new_bson["values"].append(group[0])
            continue
        
        changed = True
        
        if not new_obs.get("revisions"):
            new_obs["revisions"] = []
        
        new_obs["revisions"].append({ 
            "revision_date": old_obs["release_date"],
            "value": old_obs["value"],
            "attributes": old_obs.get("attributes") 
        })
        
        '''new release date'''
        new_obs["release_date"] = last_update

        new_bson["values"].append(new_obs)

    return changed

def series_is_changed(new_bson, old_bson):

    if not new_bson or not isinstance(new_bson, dict):
        raise ValueError("no new_bson or not dict instance")            

    if not old_bson or not isinstance(old_bson, dict):
        raise ValueError("no old_bson or not dict instance")            

    if not "values" in new_bson:
        raise ValueError("not values field in new_bson")

    if not "values" in old_bson:
        raise ValueError("not values field in old_bson")

    # Already in revisions process - before run series_is_changed
    """    
    if len(new_bson["values"]) != len(old_bson["values"]):
        return True
    
    if new_bson["values"][0]["period"] != old_bson["values"][0]["period"]:
        return True 

    if new_bson["values"][-1]["period"] != old_bson["values"][-1]["period"]:
        return True 
    """
    
    if new_bson.get("notes") != old_bson.get("notes"):
        return True

    if sorted(list(new_bson.get("dimensions", {}).keys())) != sorted(list(old_bson.get("dimensions", {}).keys())):
        return True

    if sorted(list(new_bson.get("dimensions", {}).values())) != sorted(list(old_bson.get("dimensions", {}).values())):
        return True

    if new_bson.get("attributes") and not old_bson.get("attributes"):
        return True

    if old_bson.get("attributes") and not new_bson.get("attributes"):
        return True

    if new_bson.get("attributes") and old_bson.get("attributes"):
        if sorted(list(new_bson.get("attributes", {}).keys())) != sorted(list(old_bson.get("attributes", {}).keys())):
            return True
    
        if sorted(list(new_bson.get("attributes", {}).values())) != sorted(list(old_bson.get("attributes", {}).values())):
            return True
    
    if new_bson["start_date"] != old_bson["start_date"]:
        return True 

    if new_bson["end_date"] != old_bson["end_date"]:
        return True

    return False

def series_update(new_bson, old_bson=None, last_update=None):

    if not new_bson or not isinstance(new_bson, dict):
        raise ValueError("no new_bson or not dict instance")            

    if old_bson and not isinstance(old_bson, dict):
        raise ValueError("old_bson is not dict instance")            

    if not "values" in new_bson:
        raise ValueError("not values field in new_bson")

    if old_bson and not "values" in old_bson:
        raise ValueError("not values field in old_bson")
    
    if not isinstance(new_bson["values"][0], dict):
        raise ValueError("Invalid format for this series : %s" % new_bson)

    if new_bson["start_date"] > new_bson["end_date"]:
        raise errors.RejectInvalidSeries("Invalid dates. start_date > end_date",
                                         provider_name=new_bson["provider_name"],
                                         dataset_code=new_bson["dataset_code"],
                                         bson=new_bson) 

    #FIXME:
    """
    if new_bson["frequency"] != "D" and len(new_bson["values"]) > 1:
        count_obs = (new_bson["end_date"] - new_bson["start_date"]) +1
        if len(new_bson["values"]) != count_obs:
            msg = "Missing values for provider[%s] - dataset[%s] - current[%s] - attempt[%s]" % (new_bson["provider_name"],
                                                                     new_bson["dataset_code"],
                                                                     len(new_bson["values"]),
                                                                     count_obs)
            raise Exception(msg)
    """

    _last_update = None
    if new_bson.get('last_update'):
        _last_update = clean_datetime(new_bson.pop('last_update', None))
    else:
        _last_update = clean_datetime(last_update)
    
    new_bson.pop('last_update', None)
        
    #TODO: valeurs manquantes à remplacer par chaine Unique: NaN

    series_set_release_date(new_bson, _last_update)

    if not old_bson:
        if not IS_SCHEMAS_VALIDATION_DISABLE:
            schemas.series_schema(new_bson)
        return new_bson
    else:
        changed = series_revisions(new_bson, old_bson, _last_update)
        
        if not changed:
            changed = series_is_changed(new_bson, old_bson)
            
        if not changed:
            return

        if not IS_SCHEMAS_VALIDATION_DISABLE:
            schemas.series_schema(new_bson)
        
    return new_bson

class Series:
    """Time Series class
    """
    
    def __init__(self, 
                 dataset=None,
                 provider_name=None, 
                 dataset_code=None, 
                 last_update=None, 
                 bulk_size=100,
                 fetcher=None):
        """        
        :param str provider_name: Provider name
        :param str dataset_code: Dataset code
        :param datetime.datetime last_update: Last updated date
        :param int bulk_size: Batch size for mongo bulk
        :param Fetcher fetcher: Fetcher instance
        """
        self.dataset = None
        self.provider_name = provider_name
        self.dataset_code = dataset_code
        self.last_update = last_update
        self.bulk_size = bulk_size
        
        if not fetcher:
            raise ValueError("fetcher is required")

        if not isinstance(fetcher, Fetcher):
            raise TypeError("Bad type for fetcher")

        if not dataset:
            raise ValueError("dataset is required")

        if not isinstance(dataset, Datasets):
            raise TypeError("Bad type for dataset")
        
        self.fetcher = fetcher        
        self.dataset = dataset

        # temporary storage necessary to get old_bson in bulks
        self.series_list = []
        self.fatal_error = False
        
        self.count_accepts = 0
        self.count_rejects = 0
        self.count_inserts = 0
        self.count_updates = 0

    def reset_counters(self):
        self.count_accepts = 0
        self.count_rejects = 0
        self.count_inserts = 0
        self.count_updates = 0
            
    def __repr__(self):
        return pprint.pformat([('provider_name', self.provider_name),
                               ('dataset_code', self.dataset_code),
                               ('last_update', self.last_update)])

    def process_series_data(self):
        
        try:
            while True:
                
                self.fatal_error = False
                try:
                    data = next(self.data_iterator)
    
                    if isinstance(data, dict):
                        self.count_accepts += 1
                        self.series_list.append(data)
    
                    elif isinstance(data, errors.RejectFrequency):
                        self.count_rejects += 1
                        msg = "Reject frequency for provider[%s] - dataset[%s] - frequency[%s]"
                        logger.warning(msg % (self.provider_name, 
                                              self.dataset_code, 
                                              data.frequency))
                        continue
                    
                    elif isinstance(data, errors.RejectUpdatedSeries):
                        self.count_rejects += 1
                        if logger.isEnabledFor(logging.DEBUG):
                            msg = "Reject series updated for provider[%s] - dataset[%s] - key[%s]"
                            logger.debug(msg % (self.provider_name, 
                                                self.dataset_code, 
                                                data.key))
                        continue
    
                    elif isinstance(data, errors.RejectEmptySeries):
                        self.count_rejects += 1
                        msg = "Reject empty series for provider[%s] - dataset[%s]"
                        logger.warning(msg % (self.provider_name, 
                                              self.dataset_code))
                        continue
                        
                    elif isinstance(data, Exception):
                        self.fatal_error = True
                        raise errors.InterruptProcessSeriesData(str(data))

                    if len(self.series_list) >= self.bulk_size:
                        self.update_series_list()
                    
                except StopIteration:
                    break
                except Exception:
                    raise
        finally:
            if not self.fatal_error and len(self.series_list) > 0:
                self.update_series_list()
            self.update_dataset_lists_finalize()

    def slug(self, key):
        txt = "-".join([self.provider_name, self.dataset_code, key])
        return slugify(txt, word_boundary=False, save_order=True)

    def update_dataset_lists_finalize(self):
        
        concepts = {}
        codelists = {}
        dimension_keys = []
        attribute_keys = []
        
        for key, value in self.dataset.concepts.items():
            key_slug = slugify(key, save_order=True)
            concepts[key_slug] = value
            
        for key, value in self.dataset.codelists.items():
            new_value = {}
            for k, v in value.items():
                new_value[slugify(k, save_order=True)] = v
            codelists[slugify(key, save_order=True)] = new_value
            
        for key in self.dataset.dimension_keys:
            dimension_keys.append(slugify(key, save_order=True))

        if self.dataset.attribute_keys:
            for key in self.dataset.attribute_keys:
                attribute_keys.append(slugify(key, save_order=True))
            
        self.dataset.concepts = concepts
        self.dataset.codelists = codelists
        self.dataset.dimension_keys = dimension_keys

        if self.dataset.attribute_keys:
            self.dataset.attribute_keys = attribute_keys
        
    def update_series_list(self):

        keys = [s['key'] for s in self.series_list]

        query = {
            'provider_name': self.provider_name,
            'dataset_code': self.dataset_code,
            'key': {'$in': keys}
        }
        projection = {"tags": False}

        cursor = self.fetcher.db[constants.COL_SERIES].find(query, projection)

        old_series = {s['key']:s for s in cursor}

        bulk_requests = []
        for data in self.series_list:

            key = data['key']

            if not data.get("slug", None):
                data['slug'] = self.slug(key)

            if not key in old_series:
                bson = series_update(data, last_update=self.last_update)
                bulk_requests.append(InsertOne(bson))
                self.count_inserts += 1
            else:
                old_bson = old_series[key]
                
                bson = series_update(data, old_bson=old_bson, 
                                     last_update=self.last_update)

                if bson:
                    query_update = {
                        "start_date": bson["start_date"],     
                        "end_date": bson["end_date"],     
                        "values": bson["values"],     
                        "attributes": bson["attributes"],     
                        "dimensions": bson["dimensions"],     
                        "notes": bson.get("notes"),     
                    }
                    bulk_requests.append(UpdateOne({'_id': old_bson['_id']}, 
                                              {'$set': query_update}))
                    self.count_updates += 1
                else:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug("series[%s] not changed" % old_bson["slug"])                    
                    

        result = None        
        if len(bulk_requests) > 0:
            try:
                result = self.fetcher.db[constants.COL_SERIES].bulk_write(bulk_requests)
                bulk_requests = []
            except pymongo.errors.BulkWriteError as err:
                logger.critical(last_error())
                #TODO: use pprint and StringIO for err.details output
                #logger.critical(str(err.details))
                raise
                 
        self.series_list = []
        return result

class CodeDict():
    """Class for handling code lists
    
    >>> code_list = {'Country': {'FR': 'France'}}
    >>> print(code_list)
    {'Country': {'FR': 'France'}}
    """    
    
    def __init__(self):
        # code_dict is a dict of OrderedDict
        self.code_dict = {}
        if not IS_SCHEMAS_VALIDATION_DISABLE:
            schemas.codedict_schema(self.code_dict)
        
    def update(self, arg):
        if not IS_SCHEMAS_VALIDATION_DISABLE:
            schemas.codedict_schema(arg.code_dict)
        self.code_dict.update(arg.code_dict)
        
    def update_entry(self, dim_name, dim_short_id, dim_long_id):

        if not dim_name in self.code_dict:
            self.code_dict[dim_name] = OrderedDict()
            
        for k, v in self.code_dict[dim_name].items():
            if v == dim_long_id:
                return k
            
        if not dim_short_id:
            if dim_name in self.code_dict:
                dim_short_id = str(len(self.code_dict[dim_name]))
            else:
                dim_short_id = '0' # numerical short id starts with 0

        if not dim_long_id:
            dim_short_id = 'None'

        self.code_dict[dim_name].update({dim_short_id: dim_long_id})
        
        return dim_short_id

    def get_dict(self):
        return self.code_dict

    def get_list(self):
        return {d1: list(d2.items()) for d1,d2 in self.code_dict.items()}

    def set_dict(self, arg):
        self.code_dict = arg
        
    def set_from_list(self, **kwargs):
        self.code_dict = {d1: OrderedDict(d2) for d1, d2 in kwargs.items()}
    

