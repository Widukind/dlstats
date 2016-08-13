# -*- coding: utf-8 -*-

import time
import os
import tempfile
from operator import itemgetter
from datetime import datetime, timedelta
import logging
import pprint
from collections import OrderedDict, deque
from itertools import groupby
import hashlib
import json

import pymongo
from pymongo import ReturnDocument
from bson.json_util import dumps as json_dumps
import pandas

from widukind_common.utils import get_mongo_db, load_klass, series_archives_store
from widukind_common import errors
from widukind_common.tags import generate_tags_series
from widukind_common.debug import timeit, TRACE_ENABLE

from dlstats import constants
from dlstats.fetchers import schemas
from dlstats.utils import (last_error, 
                           clean_datetime, 
                           remove_file_and_dir, 
                           make_store_path,
                           get_url_hash,
                           json_dump_convert,
                           get_datetime_from_period,
                           slugify)

logger = logging.getLogger(__name__)

IS_SCHEMAS_VALIDATION_DISABLE = constants.SCHEMAS_VALIDATION_DISABLE == "true"

class Fetcher(object):
    """Abstract base class for all fetchers"""
    
    def __init__(self, 
                 provider_name=None, 
                 db=None, 
                 is_indexes=True,
                 version=0,
                 max_errors=10,
                 use_existing_file=False,
                 not_remove_files=False,
                 force_update=False,
                 dataset_only=False,
                 refresh_meta=False,
                 refresh_dsd=False,
                 async_mode=None,
                 bulk_size=500,
                 pool_size=20,
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
        self.dataset_only = dataset_only
        self.refresh_meta = refresh_meta
        self.refresh_dsd = refresh_dsd
        self.force_update = force_update
        
        self.async_mode = async_mode
        self.bulk_size = bulk_size
        self.pool_size = pool_size
        
        if self.async_mode:
            logger.info("ASYNC MODE [%s]" % self.async_mode)
        else:
            logger.info("ASYNC MODE DISABLE")
        
        self.provider = None
        
        self.errors = 0

        self.categories_filter = [] #[category_code]
        self.datasets_filter = []   #[dataset_code]
        
        self.selected_datasets = {}
        
        self.store_path = os.path.abspath(os.path.join(tempfile.gettempdir(), 
                                                       self.provider_name))
        self.for_delete = []
        
        self.provider_verified = False
        
        if IS_SCHEMAS_VALIDATION_DISABLE:
            logger.warning("schemas validation is disable")
    
    def upsert_calendar(self):
        try:
            for entry in self.get_calendar():
                entry_str = json.dumps(entry, default=json_dump_convert)
                key = hashlib.md5(entry_str.encode('utf_8')).hexdigest()
                entry["key"] = key
                self.db[constants.COL_CALENDARS].find_one_and_replace({"key": key}, 
                                                                      entry, 
                                                                      upsert=True)
                
        except NotImplementedError:
            pass
        except Exception as err:
            logger.critical('upsert_calendar failed for %s error[%s]' % (self.provider_name, last_error()))
    
    def upsert_data_tree(self, data_tree=None, force_update=False):
        #TODO: bulk

        if data_tree and not isinstance(data_tree, list):
            raise TypeError("data_tree is not instance of list")

        if data_tree is None or force_update is True:
            data_tree = self.build_data_tree()

        results = []
        if data_tree:
            Categories.remove_all(self.provider_name, db=self.db)
                        
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
        
        if self.provider_verified:
            return
        
        if self.provider.from_db:
            return
        
        query = {"name": self.provider_name}
        
        provider = self.db[constants.COL_PROVIDERS].find_one(query)
        if not provider:
            self.provider.update_database()
        
        provider = self.load_provider_from_db()
        if provider:
            self.provider = provider
        
        if provider and self.provider.version != self.version:
            self.provider.update_database()

        self.provider_verified = True
            
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
                msg = "fetcher load START: provider[%s] - bulk-size[%s]"
                logger.info(msg % (self.provider_name, self.bulk_size))
                return self.load_datasets_first()
            else:
                msg = "fetcher update START: provider[%s] - bulk-size[%s]"
                logger.info(msg % (self.provider_name, self.bulk_size))
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
        msg = " dataset upsert START: provider[%s] - dataset[%s] - bulk-size[%s] - dataset-only[%s]"
        logger.info(msg % (self.provider_name, dataset_code, self.bulk_size, self.dataset_only))

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

        except errors.RejectUpdatedDataset as err:
            msg = "Reject dataset updated for provider[%s] - dataset[%s]"
            if err.comments:
                msg = "%s - %s" % (msg, err.comments)
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

    def _add_to_metadata(self, key, bson):
        if not self.provider.metadata:
            self.provider.metadata = {}
        self.provider.metadata[key] = bson
        self.provider.update_database()
            
    def _structure_put(self, key, url, **values):
        #TODO: compress ?
        bson = {
            "url": url, #{"original": url, "hashed": get_url_hash(url)},
            "values": values
        }
        self._add_to_metadata(key, bson)

    def _structure_get(self, key):
        #TODO: uncompress ?
        if self.provider.metadata and key in self.provider.metadata:
            return self.provider.metadata[key]["values"]

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

    @timeit("commons.DlstatsCollection.update_mongo_collection")        
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
                 terms_of_use=None,
                 metadata={},
                 enable=True,
                 lock=False,
                 from_db=False,
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
        self.terms_of_use = terms_of_use
        self.metadata = metadata or {}
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
                'terms_of_use': self.terms_of_use,
                'metadata': self.metadata,
                "enable": self.enable,
                "lock": self.lock}

    def update_database(self):
        schemas.provider_schema(self.bson)
        return self.update_mongo_collection(constants.COL_PROVIDERS, 
                                            ['slug'], 
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
                                            ['slug'],
                                            self.bson)
class Datasets(DlstatsCollection):
    """Abstract base class for datasets
    
    Inherit from :class:`DlstatsCollection`
    """
    
    series_klass = "dlstats.fetchers._commons.Series"
    
    def __init__(self, 
                 provider_name=None,
                 dataset_code=None, 
                 name=None,
                 doc_href=None,
                 last_update=None,
                 metadata=None,
                 bulk_size=500,
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
        self.bulk_size = self.fetcher.bulk_size
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
        
        self.series = None
        
        self.set_series_class()
    
    def set_series_class(self):
        if self.fetcher.async_mode:
            if self.fetcher.async_mode == "future":
                self.series_klass = "dlstats.async._concurrent_futures.AsyncSeries"
        
        series_klass = load_klass(self.series_klass)
        self.series = series_klass(dataset=self,
                                   provider_name=self.provider_name, 
                                   dataset_code=self.dataset_code, 
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
                if key in self.codelists:
                    attribute_list[key] = OrderedDict([(k, v) for k, v in self.codelists.get(key).items()])
            
            self.dimension_list.set_dict(dimension_list)
            self.attribute_list.set_dict(attribute_list)
            
            self.from_db = True
            
        else:
            msg = "dataset not found for previous loading. provider[%s] - dataset[%s]"
            logger.warning(msg % (provider_name, dataset_code))

    def set_dimension_frequency(self, dimension_name):
        '''Identify frequency field in dataset dimensions'''
        if not dimension_name:
            return
        if not self.metadata:
            self.metadata = {}
        self.metadata["dim_frequency"] = dimension_name

    def set_dimension_country(self, dimension_name):
        '''Identify country field in dataset dimensions'''
        if not dimension_name:
            return
        if not self.metadata:
            self.metadata = {}
        self.metadata["dim_country"] = dimension_name
    
    def add_frequency(self, frequency):
        '''Add used frequency for this dataset'''
        if not frequency:
            return
        if not self.metadata:
            self.metadata = {}
        if not "frequencies" in self.metadata:
            self.metadata["frequencies"] = []
        if not frequency in self.metadata["frequencies"]:
            self.metadata["frequencies"].append(frequency)
    
    def is_recordable(self):
        
        msg = None

        try:
            #if self.fetcher.max_errors and self.fetcher.errors >= self.fetcher.max_errors:
            #    msg = "fetcher max errors exceeded [%s]" % self.fetcher.errors
            #    return False
            
            if self.fetcher.db[constants.COL_PROVIDERS].count({"name": self.provider_name}) == 0:
                msg = "provider[%s] not found in DB" % self.provider_name
                logger.critical(msg)
                return False
            
            if not self.codelists or len(self.codelists) == 0:
                msg = "empty codelists for provider[%s] - dataset[%s]" % (self.provider_name, self.dataset_code)
                logger.critical(msg)                
                return False
            
            query = {"provider_name": self.provider_name,
                     "dataset_code": self.dataset_code}
            if self.fetcher.db[constants.COL_SERIES].count(query) == 0:
                msg = "not series for this dataset"
                return False
    
            return True
        finally:
            if msg:
                self.metadata["disable_reason"] = msg
            
    @timeit("commons.Datasets.update_database")
    def update_database(self, save_only=False):

        self.fetcher.hook_before_dataset(self)
        
        start = time.time()
        
        try:
            if not save_only and not self.fetcher.dataset_only:
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
            now = self.series.now
    
            if not self.download_first:
                self.download_first = now

            self.download_last = now
            
            if not self.is_recordable():
                self.enable = False
                msg = "disable dataset[%s] for provider[%s]"
                logger.warning(msg % (self.dataset_code, 
                                      self.provider_name))
            else:
                self.enable = True

            if self.fetcher.dataset_only:
                self.download_last = self.download_last - timedelta(seconds=1)
                self.last_update = self.last_update - timedelta(seconds=1)

            end = time.time()
            avg_all = 0
            avg_write = 0
            duration = end - start 
            try:
                avg_all = (self.series.count_accepts + self.series.count_rejects) / duration
                avg_write = (self.series.count_inserts + self.series.count_updates) / duration 
            except:
                pass
            
            _stats = {
                 "created": clean_datetime(),
                 "tags": ["fetcher", "Datasets.update_database", "commons"],
                 "provider_name": self.provider_name,
                 "dataset_code": self.dataset_code,
                 "fetcher_errors": self.fetcher.errors,
                 "fetcher_version": self.fetcher.version,
                 "count_accepts": self.series.count_accepts,
                 "count_rejects": self.series.count_rejects,
                 "count_inserts": self.series.count_inserts,
                 "count_updates": self.series.count_updates,
                 "count_errors": self.series.count_errors,
                 "avg_all": round(avg_all, 2),
                 "avg_write": round(avg_write, 2),
                 "duration": round(duration, 2),
                 "bulk_size": self.fetcher.bulk_size,
                 "pool_size": self.fetcher.pool_size,            
                 "dataset_only": self.fetcher.dataset_only,
                 "is_trace": TRACE_ENABLE,
                 "logger_level": logger.getEffectiveLevel(),
                 "async_mode": self.fetcher.async_mode,
                 "schema_validation_disable": IS_SCHEMAS_VALIDATION_DISABLE
            }
            
            try:
                self.fetcher.db[constants.COL_STATS_RUN].insert_one(_stats)
            except Exception as err:
                logger.error("record stats : %s" % str(err))
            
            if logger.isEnabledFor(logging.WARN):
                msg_stats = "STATS dataset-update: provider[%s] - dataset[%s] - accepts[%s] - rejects[%s] - inserts[%s] - updates[%s] - avg-all[%.3f] - avg-write[%.3f] - time[%.3f] - bulk-size[%s] - dataset-only[%s]"
                logger.warn(msg_stats % (self.provider_name,
                                         self.dataset_code,
                                         self.series.count_accepts,
                                         self.series.count_rejects,
                                         self.series.count_inserts,
                                         self.series.count_updates,
                                         avg_all,
                                         avg_write,
                                         duration,
                                         self.fetcher.bulk_size,
                                         self.fetcher.dataset_only))
            
            if self.series.count_inserts + self.series.count_updates > 0:
                schemas.dataset_schema(self.bson)
                result = self.update_mongo_collection(constants.COL_DATASETS,
                                                      ['slug'],
                                                      self.bson)
            else:
                result = self.minimal_update_database()
    
            self.fetcher.hook_after_dataset(self)
    
            return result
        
    def minimal_update_database(self):
        query = {"slug": self.slug()}
        query_update =  {"$set": {
                            "download_last": self.series.now,
                            "metadata": self.metadata
                        }}
        result = self.fetcher.db[constants.COL_DATASETS].update_one(query,
                                                                    query_update)
        result = result.upserted_id
        logger.warn("minimal update for dataset[%s]" % self.dataset_code)
        

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

    def _add_url_cache(self, url, status_code=0):
        key = get_url_hash(url)
        if not "cache_url" in self.dataset.metadata:
            self.dataset.metadata["cache_url"] = {}
        if not key in self.dataset.metadata["cache_url"]:
            self.dataset.metadata["cache_url"][key] = {"url": url, "status_code": status_code}
        
    def _is_good_url(self, url, good_codes=[200]):
        """
        FIXME: prendre en compte autre que 200
        """
        key = get_url_hash(url)
        if "cache_url" in self.dataset.metadata:
            if key in self.dataset.metadata["cache_url"]:
                return self.dataset.metadata["cache_url"][key]["status_code"] in good_codes
        return True
    
    def clean_field(self, bson):
        return series_clean_field(bson)

    def build_series(self, bson):
        raise NotImplementedError()

@timeit("commons.series_clean_field", stats_only=True)
def series_clean_field(bson):
    
    if not "start_ts" in bson or not bson.get("start_ts"):
        if bson["frequency"] in ["A", "M", "D", "Q", "S"]:
            bson["start_ts"] = get_datetime_from_period(bson["values"][0]["period"], freq=bson["frequency"])
        else:
            bson["start_ts"] = clean_datetime(pandas.Period(ordinal=bson["start_date"], freq=bson["frequency"]).start_time.to_datetime())

    if not "end_ts" in bson or not bson.get("end_ts"):
        if bson["frequency"] in ["A", "M", "D", "Q", "S"]:
            bson["end_ts"] = get_datetime_from_period(bson["values"][-1]["period"], freq=bson["frequency"])
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
        
        #TODO: datetime
        #if not "datetime" in bson or bson.get("datetime") is None:
        #    value["datetime"] = get_datetime_from_period(bson["period"], freq=bson["frequency"])
        
        if not value.get("attributes"):
            continue
        attributes_obs = {}
        for k, v in value.get("attributes").items():
            attributes_obs[slugify(k, save_order=True)] = slugify(v, save_order=True)
        value["attributes"] = attributes_obs
    
    return bson


@timeit("commons.series_is_changed", stats_only=True)
def series_is_changed(new_bson, old_bson):
    """Verify if series change(s)"""

    '''Add or remove period'''
    if len(new_bson["values"]) != len(old_bson["values"]):
        return True
    
    if len(new_bson["values"]) > 0 and len(old_bson["values"]) > 0:

        '''First period change'''
        if new_bson["values"][0]["period"] != old_bson["values"][0]["period"]:
            return True 
    
        '''Last period change'''
        if new_bson["values"][-1]["period"] != old_bson["values"][-1]["period"]:
            return True
    
        '''Value(s) change'''    
        old_values = [v['value'] for v in old_bson['values']]
        new_values = [v['value'] for v in new_bson['values']]
        if old_values != new_values:
            return True

    '''values.$.attributes change(s)'''
    old_obs_attrs = [v['attributes'] for v in old_bson['values']]
    new_obs_attrs = [v['attributes'] for v in new_bson['values']]
    for i, v in enumerate(old_obs_attrs):
        if v != new_obs_attrs[i]:
            return True

    '''change start_date'''
    if new_bson["start_date"] != old_bson["start_date"]:
        return True 

    '''change end_date'''
    if new_bson["end_date"] != old_bson["end_date"]:
        return True

    '''change notes'''
    if new_bson.get("notes") != old_bson.get("notes"):
        return True

    '''change name'''
    if new_bson.get("name") != old_bson.get("name"):
        return True
    
    '''change dimensions'''
    if new_bson.get("dimensions") != old_bson.get("dimensions"):
        return True
    
    '''change attributes'''
    if new_bson.get("attributes") != old_bson.get("attributes"):
        return True

    return False

@timeit("commons.series_verify", stats_only=True)
def series_verify(new_bson, old_bson=None):

    if not new_bson or not isinstance(new_bson, dict):
        raise ValueError("no new_bson or not dict instance")            

    if old_bson and not isinstance(old_bson, dict):
        raise ValueError("old_bson is not dict instance")            

    if new_bson and not "values" in new_bson:
        raise ValueError("not values field in new_bson")

    if old_bson and not "values" in old_bson:
        raise ValueError("not values field in old_bson")
    
    if not isinstance(new_bson["values"][0], dict):
        raise ValueError("Invalid format for this series")

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

@timeit("commons.series_get_last_update_dataset", stats_only=True)
def series_get_last_update_dataset(new_bson, last_update=None):
    """Return valid last_update value"""
    _last_update = None
    if new_bson.get('last_update'):
        _last_update = clean_datetime(new_bson.pop('last_update', None))
    else:
        _last_update = clean_datetime(last_update)
        
    new_bson.pop('last_update', None)
    return _last_update

@timeit("commons.series_set_codelists", stats_only=True)
def series_set_codelists(bson, codelists):
    """set/update codelists field in series"""
    
    search_codelists = {}
    
    if not "codelists" in bson:
        bson["codelists"] = {}
        
    for k, v in bson.get("dimensions", {}).items():
        if not k in search_codelists:
            search_codelists[k] = []
        if not v in search_codelists[k]:
            search_codelists[k].append(v)
    
    if bson.get("attributes"):
        for k, v in bson.get("attributes").items():
            if not k in search_codelists:
                search_codelists[k] = []
            if not v in search_codelists[k]:
                search_codelists[k].append(v)

    for value in bson["values"]:
        if value.get("attributes"):
            for k, v in value.get("attributes").items():
                if not k in search_codelists:
                    search_codelists[k] = []
                if not v in search_codelists[k]:
                    search_codelists[k].append(v)

    for k, items in search_codelists.items():
        if k in codelists:
            for i in items:
                value = codelists[k].get(i)
                if value:
                    if not k in bson["codelists"]:
                        bson["codelists"][k] = {}
                    bson["codelists"][k][i] = value
    
def clean_values(bson):
    for value in bson["values"]:
        value.pop('ordinal', None)
        value.pop('release_date', None)
        value.pop('revisions', None)
    
class Series:
    """Time Series class
    """
    
    def __init__(self, 
                 dataset=None,
                 provider_name=None, 
                 dataset_code=None, 
                 bulk_size=500,
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

        self.bulk_size = bulk_size

        # temporary storage necessary to get old_bson in bulks
        self.series_list = deque()
        self.fatal_error = False
        
        self.now = clean_datetime()
        
        self.dataset_finalized = False
        
        self.count_accepts = 0
        self.count_rejects = 0
        self.count_inserts = 0
        self.count_updates = 0
        self.count_errors = 0
        
    def reset_counters(self):
        self.count_accepts = 0
        self.count_rejects = 0
        self.count_inserts = 0
        self.count_updates = 0
        self.count_errors = 0
            
    def __repr__(self):
        return pprint.pformat([('provider_name', self.provider_name),
                               ('dataset_code', self.dataset_code),
                               ('last_update', self.dataset.last_update)])

    @timeit("commons.Series.process_series_data")
    def process_series_data(self):
        
        try:
            while True:
                
                self.fatal_error = False
                try:
                    data = next(self.data_iterator)
                    
                    if isinstance(data, dict):
                        if not "values" in data or len(data["values"]) == 0:
                            self.count_rejects += 1
                            msg = "Reject empty series for provider[%s] - dataset[%s]"
                            logger.warning(msg % (self.provider_name, 
                                                  self.dataset_code))
                            continue
                        else:
                            self.count_accepts += 1
                            self.series_list.append(data)
                    
                    elif isinstance(data, errors.InterruptBatchSeriesData):
                        self.update_series_list()
    
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
            """
            consolidate.consolidate_dataset(db=self.fetcher.db, {"provider_name": self.provider_name,
                                                    "dataset_code": self.dataset_code})
            tags.update_tags_datasets(self.fetcher.db,
                                      provider_name=self.provider_name,
                                      dataset_code=self.dataset_code, 
                                      max_bulk=self.bulk_size,
                                      #update_only=update_only,
                                      )
            """

    @timeit("commons.Series.update_dataset_lists_finalize")
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
    
    def get_db(self):
        return self.fetcher.db
        #TODO: settings for new connection
        #return get_mongo_db()

    @timeit("commons.Series.update_series_list", stats_only=True)
    def update_series_list(self):

        #if not self.dataset_finalized:
        #    self.update_dataset_lists_finalize()
        
        keys = [s['key'] for s in self.series_list]

        query = {
            'provider_name': self.provider_name,
            'dataset_code': self.dataset_code,
            'key': {'$in': keys}
        }

        db = self.get_db()
        cursor = db[constants.COL_SERIES].find(query)

        old_series = {s['key']:s for s in cursor}

        bulk_requests = db[constants.COL_SERIES].initialize_ordered_bulk_op()
        bulk_requests_archives = db[constants.COL_SERIES_ARCHIVES].initialize_ordered_bulk_op()
        is_operation = False
        is_operation_archives = False
        
        for bson in self.series_list:
            
            key = bson['key']

            if not "version" in bson:
                bson["version"] = 0

            if not bson.get("slug", None):
                txt = "-".join([self.provider_name, self.dataset_code, key])
                bson['slug'] = slugify(txt, word_boundary=False, save_order=True)
            
            last_update_ds = series_get_last_update_dataset(bson, 
                                                            last_update=self.dataset.last_update)
            
            clean_values(bson)
            
            if not key in old_series:
                series_verify(bson)
                bson["last_update_ds"] = last_update_ds 
                bson["last_update_widu"] = clean_datetime()
                series_set_codelists(bson, self.dataset.codelists)
                if not IS_SCHEMAS_VALIDATION_DISABLE:
                    schemas.series_schema(bson)
                bulk_requests.insert(bson)
                is_operation = True
                self.count_inserts += 1
            else:
                old_bson = old_series[key]
                series_verify(bson, old_bson=old_bson)
                clean_values(old_bson)
                
                _id = old_bson.pop('_id')
                tags = old_bson.pop('tags', None)

                if series_is_changed(bson, old_bson): 
                    old_bson["tags"] = tags
                    if not "version" in old_bson:
                        old_bson["version"] = 0
                    old_version = old_bson["version"]
                    bulk_requests_archives.insert(series_archives_store(old_bson))
                    is_operation = True
                    is_operation_archives = True
                    self.count_updates += 1
                    bson["tags"] = tags
                    bson["last_update_ds"] = last_update_ds 
                    bson["last_update_widu"] = clean_datetime()
                    bson["version"] = old_version + 1
                    
                    series_set_codelists(bson, self.dataset.codelists)
                    
                    if not IS_SCHEMAS_VALIDATION_DISABLE:
                        schemas.series_schema(bson)
                    
                    bson["_id"] = _id
                    bulk_requests.find({"_id": _id}).replace_one(bson)
                else:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug("series[%s] not changed" % old_bson["slug"])                    

        result = None        
        if is_operation is True:
            try:
                @timeit("commons.Series.update_series_list.execute")
                def _execute():
                    bulk_requests.execute()
                _execute()
            except pymongo.errors.BulkWriteError as err:
                self.dataset.enable = False
                self.dataset.metadata["disable_reason"] = "critical bulk error"
                logger.critical(str(err.details))
                raise

        if is_operation_archives is True:
            try:
                @timeit("commons.Series.update_series_list.execute_archives")
                def _execute_archives():
                    bulk_requests_archives.execute()
                _execute_archives()
            except pymongo.errors.BulkWriteError as err:
                #self.dataset.enable = False
                #self.dataset.metadata["disable_reason"] = "critical bulk error"
                logger.critical(str(err.details))
                raise
                 
        self.series_list = deque()
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
    

