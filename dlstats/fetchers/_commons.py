# -*- coding: utf-8 -*-

from operator import itemgetter
from datetime import datetime
import logging
import pprint
from collections import OrderedDict

import pymongo
from pymongo import ReturnDocument
from slugify import slugify
import pandas

from widukind_common.utils import get_mongo_db, create_or_update_indexes

from dlstats import constants
from dlstats.fetchers import schemas
from dlstats import errors
from dlstats.utils import last_error, clean_datetime

logger = logging.getLogger(__name__)

class Fetcher(object):
    """Abstract base class for all fetchers"""
    
    def __init__(self, 
                 provider_name=None, 
                 db=None, 
                 is_indexes=True,
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
        self.max_errors = kwargs.pop("max_errors", 5)
        
        self._provider = None
        
        self.errors = 0
        
        self.period_cache = {}

        if is_indexes:
            create_or_update_indexes(self.db)
    
    def upsert_data_tree(self, data_tree=None, force_update=False):
        #TODO: bulk

        if data_tree and not isinstance(data_tree, list):
            raise TypeError("data_tree is not instance of list")

        if not data_tree or force_update:
            data_tree = self.build_data_tree(force_update=force_update)

        results = []            
        for data in data_tree:
            cat = Categories(fetcher=self, **data)
            results.append(cat.update_database())
            
        return results

    def load_provider_from_db(self):
        """Load and set provider fields from DB
        """
        query = {"name": self.provider_name}
        doc = self.db[constants.COL_PROVIDERS].find_one(query)
        if not doc:
            return

        doc.pop('_id')
        return Providers(fetcher=self, from_db=True, **doc)

    def datasets_list(self, **query):
        
        if Categories.count(self.provider_name, db=self.db) == 0:
            self.upsert_data_tree()
        
        if not "provider_name" in query:
            query["provider_name"] = self.provider_name
        query["datasets.0"] = {"$exists": True}
        
        #TODO: enable
        
        fields = {"_id": 0, "provider_name": 1, "slug": 1, "parent": 1, 
                  "all_parents": 1, 
                  "datasets": 1}
        
        cursor = self.db[constants.COL_CATEGORIES].find(query, fields)
        datasets = []
        datasets_keys = []
        
        for doc in cursor:
            for d in doc["datasets"]:
                if not d["dataset_code"] in datasets_keys:
                    datasets.append(d)
                    datasets_keys.append(d["dataset_code"])
        
        return sorted(datasets, key=itemgetter("dataset_code"))

    @property
    def provider(self):
        if not self._provider:
            self._provider = self.load_provider_from_db()
        return self._provider
    
    @provider.setter
    def provider(self, provider):
        if not isinstance(provider, Providers):
            raise TypeError("provider is not instance of Providers")
        
        self._provider = provider

        if not self._provider.from_db:
            self._provider.update_database()
            self._provider = self.load_provider_from_db()
            
    def upsert_all_datasets(self):
        query = {"provider_name": self.provider_name}
        if self.db[constants.COL_DATASETS].count(query) == 0:
            return self.load_datasets_first()
        else:
            return self.load_datasets_update()
        
    def get_ordinal_from_period(self, date_str, freq=None):
        if not freq in ['Q', 'M', 'A', 'W']:
            return pandas.Period(date_str, freq=freq).ordinal
        
        key = "%s.%s" % (date_str, freq)
        if key in self.period_cache:
            return self.period_cache[key]
        
        self.period_cache[key] = pandas.Period(date_str, freq=freq).ordinal
        return self.period_cache[key]
        
    def load_datasets_first(self):
        raise NotImplementedError()

    def load_datasets_update(self):
        raise NotImplementedError()

    def build_data_tree(self, force_update=False):
        raise NotImplementedError()

    def upsert_categories(self):
        """Upsert the categories in MongoDB
        
        TODO: remove function. replace with upsert_data_tree()
        """
        self.upsert_data_tree()
    
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
        except Exception as err:
            logger.critical('%s.update_database() failed for %s error[%s]' % (collection, str(key), str(err)))
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

    def populate_obj(self, doc):
        self.name = doc['name']
        self.long_name = doc['long_name']
        self.version = doc['version']
        self.slug = doc['slug']
        self.region = doc['region']
        self.website = doc['website']
        self.metadata = doc['metadata']
        self.enable = doc['enable']
        self.lock = doc['lock']
        return self

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
                 fetcher=None):
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
                'tags': [],
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
                 is_load_previous_version=True):
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
        self.metadata = metadata
        self.bulk_size = bulk_size
        self.dimension_list = CodeDict()
        self.attribute_list = CodeDict()
        
        self.dimension_keys = []
        self.attribute_keys = []
        self.codelists = {}
        self.concepts = {}

        self.enable = True
        self.lock = False

        self.download_first = None
        self.download_last = None
        
        if is_load_previous_version:
            self.load_previous_version(provider_name, dataset_code)
            
        self.notes = ''
        
        self.series = Series(provider_name=self.provider_name, 
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
                
                'dimension_list': self.dimension_list.get_list(),
                'attribute_list': self.attribute_list.get_list(),

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
                "lock": self.lock}

    def load_previous_version(self, provider_name, dataset_code):
        dataset = self.fetcher.db[constants.COL_DATASETS].find_one(
                                            {'provider_name': provider_name,
                                             'dataset_code': dataset_code})
        if dataset:
            # convert to dict of dict
            self.download_first = dataset.get('download_first')
            #self.download_last = dataset.get('download_last')
            self.dimension_list.set_from_list(dataset['dimension_list'])
            self.attribute_list.set_from_list(dataset['attribute_list'])

    def is_recordable(self):
        
        query = {"provider_name": self.provider_name,
                 "dataset_code": self.dataset_code}
        projection = {"provider_name": True, "dataset_code": True}
        
        if self.fetcher.db[constants.COL_DATASETS].find_one(query, projection):
            return True
        
        if self.fetcher.db[constants.COL_SERIES].count(query) > 0:
            return True

        return False
        
    def update_database(self):
        try:
            self.series.process_series_data()
        except Exception:
            self.fetcher.errors += 1
            logger.critical(last_error())
            if self.fetcher.max_errors and self.fetcher.errors >= self.fetcher.max_errors:
                raise errors.MaxErrors("The maximum number of errors is exceeded. MAX[%s]" % self.fetcher.max_errors)

        now = clean_datetime()
        
        if not self.download_first:
            self.download_first = now

        if not self.download_last:
            self.download_last = now

        schemas.dataset_schema(self.bson)
        
        if not self.is_recordable():
            logger.warning("Not recordable dataset[%s] for provider[%s]" % (self.dataset_code, self.provider_name))
            return
        
        return self.update_mongo_collection(constants.COL_DATASETS,
                                                ['provider_name', 'dataset_code'],
                                                self.bson)

class SeriesIterator:
    """Base class for all Fetcher data class
    """
    
    def __init__(self):
        self.rows = None

    def __next__(self):
        series, err = next(self.rows)
        if err:
            return err
        
        if not series:
            raise StopIteration()

        try:
            return self.clean_field(self.build_series(series))
        except Exception as err:
            return err

    def clean_field(self, bson):
        """
        if bson:
            bson.pop('version', None)
            bson.pop('series_attributes', None)
        """
        return bson

    def build_series(self, bson):
        raise NotImplementedError()


class Series:
    """Time Series class
    """
    
    def __init__(self, 
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
        self.provider_name = provider_name
        self.dataset_code = dataset_code
        self.last_update = last_update
        self.bulk_size = bulk_size
        
        if not fetcher:
            raise ValueError("fetcher is required")

        if not isinstance(fetcher, Fetcher):
            raise TypeError("Bad type for fetcher")
        
        self.fetcher = fetcher        

        # temporary storage necessary to get old_bson in bulks
        self.series_list = []
    
    def __repr__(self):
        return pprint.pformat([('provider_name', self.provider_name),
                               ('dataset_code', self.dataset_code),
                               ('last_update', self.last_update)])

    def process_series_data(self):
        while True:
            try:
                data = next(self.data_iterator)

                if not data:
                    logger.warning("series None for dataset[%s]" % self.dataset_code)

                elif isinstance(data, dict):
                    self.series_list.append(data)

                elif isinstance(data, errors.RejectUpdatedSeries):
                    if logger.isEnabledFor(logging.DEBUG):
                        msg_tmpl = "Reject series updated for provider[%s] - dataset[%s] - key[%s]"
                        msg = msg_tmpl % (self.provider_name, 
                                          self.dataset_code, 
                                          data.key)
                        logger.debug(msg)

                elif isinstance(data, errors.RejectFrequency):
                    msg_tmpl = "Reject frequency for provider[%s] - dataset[%s] - frequency[%s]"
                    msg = msg_tmpl % (self.provider_name, 
                                      self.dataset_code, 
                                      data.frequency)
                    logger.warning(msg)
                
                elif isinstance(data, errors.RejectEmptySeries):
                    logger.warning("Reject empty series for provider[%s] - dataset[%s]" % (self.provider_name,
                                                                                           self.dataset_code))
                
            except StopIteration:
                break
            except Exception:
                logger.critical("Not captured exception for provider[%s] - dataset[%s] : error[%s]" % (self.provider_name,
                                                                                                       self.dataset_code, 
                                                                                                       last_error()))
                raise
            finally:
                if len(self.series_list) > self.bulk_size:
                    self.update_series_list()

        if len(self.series_list) > 0:
            self.update_series_list()

    def slug(self, key):
        txt = "-".join([self.provider_name, self.dataset_code, key])
        return slugify(txt, word_boundary=False, save_order=True)
        
    def update_series_list(self):

        #TODO: gestion erreur bulk (BulkWriteError)

        keys = [s['key'] for s in self.series_list]

        cursor = self.fetcher.db[constants.COL_SERIES].find({
                                        'provider_name': self.provider_name,
                                        'dataset_code': self.dataset_code,
                                        'key': {'$in': keys}})
        old_series = {s['key']:s for s in cursor}
        
        bulk = self.fetcher.db[constants.COL_SERIES].initialize_unordered_bulk_op()
        bulk_ops = 0
        
        for data in self.series_list:
            
            key = data['key']
            
            if not key in old_series:
                bson = self.update_series(data, is_bulk=True)
                bulk.insert(bson)
                bulk_ops += 1
            else:
                old_bson = old_series[key]
                bson = self.update_series(data, old_bson=old_bson, is_bulk=True)                
                
                if not bson:
                    if logger.isEnabledFor(logging.DEBUG):
                        msg = "series[%s] not changed for dataset[%s] - provider[%s]"
                        logger.debug(msg % (key, 
                                            self.dataset_code, 
                                            self.provider_name))                    
                
                else:
                    query_update = {
                        "start_date": bson["start_date"],     
                        "end_date": bson["end_date"],     
                        "values": bson["values"],     
                        "attributes": bson["attributes"],     
                        "dimensions": bson["dimensions"],     
                        "notes": bson.get("notes"),     
                    }
                    bulk.find({'_id': old_bson['_id']}).update_one({'$set': query_update})
                    bulk_ops += 1

        result = None        
        if bulk_ops > 0:
            try:
                result = bulk.execute()
            except pymongo.errors.BulkWriteError as err:
                logger.critical(last_error())
                #TODO: use pprint and StringIO for err.details output
                #logger.critical(str(err.details))
                raise
                 
        self.series_list = []
        return result

    def format_last_update(self, date_value):
        if not date_value:
            return None
        return datetime(date_value.year, date_value.month, date_value.day, date_value.hour, date_value.minute)

    def set_release_date(self, bson, last_update):
        for obs in bson["values"]:
            if not obs.get("release_date"):
                obs["release_date"] = last_update

    def is_changed_series(self, bson, old_bson):
        if bson["start_date"] != old_bson["start_date"]:
            return True 

        if bson["end_date"] != old_bson["end_date"]:
            return True
        
        if sorted(list(bson["dimensions"].keys())) != sorted(list(old_bson["dimensions"].keys())):
            return True

        if sorted(list(bson["dimensions"].values())) != sorted(list(old_bson["dimensions"].values())):
            return True
        
        if bson["attributes"] and not old_bson["attributes"]:
            return True 

        if bson["attributes"] and old_bson["attributes"]:
            if sorted(list(bson["attributes"].keys())) != sorted(list(old_bson["attributes"].keys())):
                return True
        
        if bson.get("notes") != old_bson.get("notes"):
            return True
        
        return False

    def update_series(self, bson, old_bson=None, is_bulk=False):
        
        if not isinstance(bson["values"][0], dict):
            raise TypeError("Invalid format for this series : %s" % bson)

        if not 'slug' in bson:
            bson['slug'] = self.slug(bson['key'])
            
        last_update = self.format_last_update(bson.pop('last_update', None))
        if not last_update:                                
            last_update = self.format_last_update(self.last_update)
            
        col = self.fetcher.db[constants.COL_SERIES]
        
        #TODO: valeurs manquantes à remplacer par chaine Unique: NaN

        self.set_release_date(bson, last_update)

        if not old_bson:
            schemas.series_schema(bson)
            if is_bulk:
                return bson
            return col.insert(bson)
        else:
            """
            1. add value                 : implemented
            2. modify value              : implemented
            3. remove value ?            : Not implemented
            4. insert value ?            : Not implemented            
            """
            old_values = old_bson["values"]
            count_old_values = len(old_values)
            changed = False
            
            for position, obs in enumerate(bson["values"]):
                
                if position >= count_old_values:
                    break
                
                old_obs = old_values[position]
                
                if old_obs["period"] != obs["period"]:
                    msg = "Period diff for same observation - series-slug[%s] - period[%s] - oldperiod[%s]" % (bson["slug"],
                                                                                                               obs["period"],
                                                                                                               old_obs["period"])
                    raise Exception(msg)
                
                if old_obs["value"] == obs["value"]:
                    continue
                
                if not changed:
                    changed = True
                
                if not old_obs.get("revisions", None):
                    obs["revisions"] = []
                else:
                    obs["revisions"] = old_obs["revisions"]
                    
                obs["revisions"].append({ 
                    "revision_date": old_obs["release_date"],
                    "value": old_obs["value"], 
                })
                
                obs["release_date"] = last_update

            '''Verify others changes'''
            if not changed:
                changed = self.is_changed_series(bson, old_bson)
                
            if not changed:
                return

            schemas.series_schema(bson)
            if is_bulk:
                return bson
            return col.find_one_and_update({'_id': old_bson['_id']}, {'$set': bson})

class CodeDict():
    """Class for handling code lists
    
    >>> code_list = {'Country': {'FR': 'France'}}
    >>> print(code_list)
    {'Country': {'FR': 'France'}}
    """    
    
    def __init__(self):
        # code_dict is a dict of OrderedDict
        self.code_dict = {}
        schemas.codedict_schema(self.code_dict)
        
    def update(self,arg):
        schemas.codedict_schema(arg.code_dict)
        self.code_dict.update(arg.code_dict)
        
    def update_entry(self, dim_name, dim_short_id, dim_long_id):
        if dim_name in self.code_dict:
            if not dim_long_id:
                dim_short_id = 'None'
                if 'None' not in self.code_dict[dim_name]:
                    self.code_dict[dim_name].update({'None': 'None'})
            elif not dim_short_id:
                # find the next (numerical) short id in self.code_dict[dim_name]
                try:
                    dim_short_id = next(key for key,value in self.code_dict[dim_name].items() if value == dim_long_id)
                except StopIteration:
                    dim_short_id = str(len(self.code_dict[dim_name]))
                    self.code_dict[dim_name].update({dim_short_id: dim_long_id})
            elif not dim_short_id in self.code_dict[dim_name]:
                self.code_dict[dim_name].update({dim_short_id: dim_long_id})
        else:
            if not dim_short_id:
                # numerical short id starts with 0
                dim_short_id = '0'
            self.code_dict[dim_name] = OrderedDict({dim_short_id: dim_long_id})
        return(dim_short_id)

    def get_dict(self):
        return self.code_dict

    def get_list(self):
        return {d1: list(d2.items()) for d1,d2 in self.code_dict.items()}

    def set_dict(self,arg):
        self.code_dict = arg
        
    def set_from_list(self,dimension_list):
        self.code_dict = {d1: OrderedDict(d2) for d1,d2 in dimension_list.items()}
    

