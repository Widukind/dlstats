# -*- coding: utf-8 -*-

from operator import itemgetter
import re
import os
import pymongo
from pymongo import ReturnDocument
from datetime import datetime
import logging
import pprint
from collections import defaultdict, OrderedDict
from copy import deepcopy

from slugify import slugify

from widukind_common.utils import get_mongo_db, create_or_update_indexes

from dlstats import constants
from dlstats.fetchers import schemas

logger = logging.getLogger(__name__)

class Fetcher(object):
    """Abstract base class for all fetchers"""
    
    def __init__(self, 
                 provider_name=None, 
                 db=None, 
                 is_indexes=True):
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
        self.provider = None
        
        if is_indexes:
            create_or_update_indexes(self.db)
        
    def upsert_data_tree(self, data_tree=None):

        if data_tree and not isinstance(data_tree, list):
            raise TypeError("data_tree is not instance of list")

        if not data_tree:
            data_tree = self.build_data_tree()

        self.provider.data_tree = data_tree

        for d in data_tree:
            schemas.data_tree_schema(d)

        query_filter = {'name': self.provider_name}
        query_update = {'$set': {'data_tree': data_tree}}

        return self.db[constants.COL_PROVIDERS].find_one_and_update(query_filter,
                                                                    query_update,
                                                                    return_document=pymongo.ReturnDocument.AFTER)

    def load_provider_from_db(self, provider_obj=None):
        """Load and set provider fields from DB
        """
        doc = self.db[constants.COL_PROVIDERS].find_one({"name": self.provider_name})
        if not doc:
            return doc
        if provider_obj:
            return provider_obj.populate_obj(doc)
        else:
            doc.pop('_id')
            return Providers.factory(fetcher=self, **doc)

    def filter_datasets(self, datasets):
        """Return one unique list of dataset

        TODO: translation EN
        Surchargez cette méthode pour effectuer un autre filtrage.
        """
        keys = []
        for d in sorted(datasets, key=itemgetter("dataset_code")):
            if not d["dataset_code"] in keys:
                keys.append(d["dataset_code"])
                yield d

    def get_selected_dataset_codes(self):
        """Return array of tuple(dataset_code, name)
        """
        #TODO: self.provider._is_loaded
        if not self.provider:
            self.provider = self.load_provider_from_db()

        if self.provider.count_data_tree() <= 1:
            self.upsert_data_tree()

        datasets = []

        for cat in self.provider.data_tree:
            if len(cat["datasets"]) > 0:
                datasets.extend(cat['datasets'])

        return [d for d in self.filter_datasets(datasets)]

    def datasets_list(self):
        """Return a list of tuple (dataset code, name)
        """
        return self.get_selected_dataset_codes()

    def upsert_all_datasets(self):
        if self.db[constants.COL_DATASETS].count({"provider_name": self.provider_name}) == 0:
            return self.load_datasets_first()
        else:
            return self.load_datasets_update()

    def load_datasets_first(self):
        raise NotImplementedError()

    def load_datasets_update(self):
        raise NotImplementedError()

    def build_data_tree(self):
        raise NotImplementedError()

    def upsert_categories(self):
        """Upsert the categories in MongoDB
        """        
        raise NotImplementedError("This method from the Fetcher class must"
                                  "be implemented.")
    
    def upsert_series(self):
        """Upsert all the series in MongoDB
        
        .. versionchanged:: 0.3.0
           Remove function. (Not used in fetchers)                
        """        
        raise NotImplementedError("This method from the Fetcher class must"
                                  "be implemented.")

    #TODO: not used function ?    
    def upsert_a_series(self, id):
        """Upsert the series in MongoDB
        
        .. versionchanged:: 0.3.0
           Remove function. (Not used in fetchers)
        
        :param id: :class:`str` - ID of :class:`Series`        
        """        
        raise NotImplementedError("This method from the Fetcher class must"
                                  "be implemented.")
    
    def upsert_dataset(self, dataset_code, datas=None):
        """Upsert a dataset in MongoDB
        
        :param str dataset_code: ID of :class:`Datasets`
        """        
        raise NotImplementedError("This method from the Fetcher class must"
                                  "be implemented.")
    
    #TODO: not used function ?    
    def insert_provider(self):
        """Insert the provider in MongoDB
        """        
        self.provider.update_database()
        
    def update_metas(self, dataset_code):
        """Update Meta datas to ElasticSearch
        
        :param str dataset_code: ID of :class:`Datasets`
        """        
        
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
        
    def update_mongo_collection(self, collection, keys, bson, 
                                log_level=logging.INFO):
        """Update one document

        :param str collection: Collection name
        :param list keys: List of value for unique key
        :param dict bson: Document values
        :param int log_level: Default logging level
        
        :return: Instance of :class:`bson.objectid.ObjectId`  
        """
        lgr = logging.getLogger(__name__)
        key = {k: bson[k] for k in keys}
        try:
            result = self.fetcher.db[collection].find_one_and_replace(key, bson, upsert=True,
                                                                      return_document=ReturnDocument.AFTER)
            result = result['_id']
        except Exception as err:
            lgr.critical('%s.update_database() failed for %s - %s [%s]' % (collection, str(key), str(result), str(err)))
            return None
        else:
            lgr.log(log_level,collection + ' ' + str(key) + ' updated.')
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
                 fetcher=None,
                 **kwargs):
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

        self.__data_tree = OrderedDict()
        self.validate = schemas.provider_schema(self.bson)
        self.set_root_category()

    def __repr__(self):
        return pprint.pformat([(key, self.validate[key]) for key in sorted(self.validate.keys()) if not key in ["data_tree", "slug"]])

    def slug(self):
        if not self.name:
            return 
        return slugify(self.name, word_boundary=False, save_order=True)

    @classmethod
    def factory(cls, data_tree=None, **kwargs):
        provider = cls(**kwargs)
        if data_tree:
            provider.data_tree = data_tree
        return provider

    def populate_obj(self, doc):
        self.name = doc['name']
        self.long_name = doc['long_name']
        self.version = doc['version']
        self.slug = doc['slug']
        self.region = doc['region']
        self.website = doc['website']
        if doc.get('data_tree'):
            self.data_tree = doc.get('data_tree')

    @property
    def bson(self):
        return {'name': self.name,
                'long_name': self.long_name,
                'version': self.version,
                'slug': self.slug(),
                'region': self.region,
                'website': self.website,
                'data_tree': self.data_tree}

    def set_root_category(self):
        if not self.name in self.__data_tree:
            self.__data_tree[self.name] = DataTreeEntry(name=self.name, 
                                                        category_code=self.name, 
                                                        doc_href=self.website, 
                                                        is_root=True) 

    def add_category(self, category, parent_code=None):
        self.set_root_category()

        if isinstance(category, dict):
            category = DataTreeEntry(**category)

        key = category.category_code

        if parent_code:
            parent = self.__data_tree[str(parent_code)]
            key = "%s.%s" % (parent.category_code, key)

        if not category.is_root:
            if not key.startswith(self.name + "."):
                key = "%s.%s" % (self.name, key)

        if key in self.__data_tree:
            raise Exception("Duplicate category")

        category.category_code = key
        self.__data_tree[key] = category

        return key
    
    def add_dataset(self, dataset, category_code):
        category_code = str(category_code)

        if not category_code in self.__data_tree:
            raise Exception("category not found[%s]" % category_code)

        category = self.__data_tree[category_code]
        category.add_dataset(**dataset)

    def count_data_tree(self):
        return len(self.__data_tree)

    @property
    def data_tree(self):
        return sorted([d.bson for d in self.__data_tree.values()], key=itemgetter("category_code"))

    @data_tree.setter
    def data_tree(self, data_tree):
        if not isinstance(data_tree, list):
            raise TypeError("data_tree is not instance of list")
        
        self.__data_tree = OrderedDict()
        for d in data_tree:
            self.__data_tree[d["category_code"]] = DataTreeEntry(**d)

    def _find_datasets(self, _filter=None):
        filter_reg = None
        if _filter:
            filter_reg = re.compile(_filter)
        
        for category_code, d in self.__data_tree.items():
            if len(d.datasets) > 0:
                if filter_reg and not filter_reg.match(category_code):
                    continue
                for dataset in d.datasets:
                    yield (dataset["dataset_code"], dataset)
        
    def datasets(self, _filter=None, sorted_attr="dataset_code"):        
        datasets = OrderedDict([d for d in self._find_datasets(_filter)])
        return sorted(datasets.values(), key=itemgetter(sorted_attr))
    
    def get_categories(self, base=None):
        """Return hierarchy of categories
        
        ECB
            ECB.MOBILE_NAVI
                ECB.MOBILE_NAVI.07
                
        ECB -> MOBILE_NAVI -> 07
        
        Si base = base de départ:
            ex: ECB.MOBILE_NAVI
                
        """
        raise NotImplementedError()
                
        if base:
            first_category = base
        else:
            first_category = self.name
        
        data_tree = self.data_tree.copy()
        
        categories = data_tree[first_category]
        categories["children"] = []
        
        for key in data_tree.keys():
            if key == first_category: 
                continue
            if key.startswith(first_category + "."):
                categories["children"].append(data_tree[key])
    
    def update_database(self):
        schemas.provider_schema(self.bson)
        return self.update_mongo_collection(constants.COL_PROVIDERS, 
                                            ['name'], 
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
        self.bulk_size = bulk_size
        self.dimension_list = CodeDict()
        self.attribute_list = CodeDict()

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
                'doc_href': self.doc_href,
                'last_update': self.last_update,
                'download_first': self.download_first,
                'download_last': self.download_last,
                'notes': self.notes}

    def load_previous_version(self, provider_name, dataset_code):
        dataset = self.fetcher.db[constants.COL_DATASETS].find_one(
                                            {'provider_name': provider_name,
                                             'dataset_code': dataset_code})
        if dataset:
            # convert to dict of dict
            self.download_first = dataset['download_first']
            self.dimension_list.set_from_list(dataset['dimension_list'])
            self.attribute_list.set_from_list(dataset['attribute_list'])

    def is_recordable(self):
        
        query = {"provider_name": self.provider_name,
                 "dataset_code": self.dataset_code}
        projection = {"provider_name": True, "dataset_code": True}
        
        doc = self.fetcher.db[constants.COL_DATASETS].find_one(query, projection)
        if doc:
            return True
        
        count_series = self.fetcher.db[constants.COL_SERIES].count(query)
        if count_series > 0:
            return True
        
        return False
        
    def update_database(self):
        try:
            self.series.process_series_data()
        except Exception as err:
            logger.critical(err)

        now = datetime.now()
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

class Series(DlstatsCollection):
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
        super().__init__(fetcher=fetcher)
        self.provider_name = provider_name
        self.dataset_code = dataset_code
        self.last_update = last_update
        self.bulk_size = bulk_size
        # temporary storage necessary to get old_bson in bulks
        self.series_list = []
    
    def __repr__(self):
        return pprint.pformat([('provider_name', self.provider_name),
                               ('dataset_code', self.dataset_code),
                               ('last_update', self.last_update)])

    def process_series_data(self):
        count = 0
        while True:
            try:
                # append result from __next__ method in fetchers
                # one iteration by serie
                data = next(self.data_iterator)
                self.series_list.append(data)
            except StopIteration:
                break
            count += 1
            if count > self.bulk_size:
                self.update_series_list()
                count = 0
        if count > 0:
            self.update_series_list()

    def slug(self, key):
        txt = "-".join([self.provider_name, self.dataset_code, key])
        return slugify(txt, word_boundary=False, save_order=True)
        
    def update_series_list(self):

        #TODO: gestion erreur bulk (BulkWriteError)

        keys = [s['key'] for s in self.series_list]

        old_series = self.fetcher.db[constants.COL_SERIES].find({
                                        'provider_name': self.provider_name,
                                        'dataset_code': self.dataset_code,
                                        'key': {'$in': keys}})

        old_series = {s['key']:s for s in old_series}
        
        bulk = self.fetcher.db[constants.COL_SERIES].initialize_ordered_bulk_op()

        for data in self.series_list:
            key = data['key']
            if not key in old_series:
                bson = self.update_series(data,is_bulk=True)
                bulk.insert(bson)
            else:
                old_bson = old_series[key]
                bson = self.update_series(data,
                                          old_bson=old_bson,
                                          is_bulk=True)                
                bulk.find({'_id': old_bson['_id']}).update({'$set': bson})
        
        try:
            result = bulk.execute()
        except pymongo.errors.BulkWriteError as err:
            logger.critical(str(err.details))
            pprint.pprint(err.details)
            raise
                 
        self.series_list = []
        return result

    def format_last_update(self, date_value):
        if not date_value:
            return None
        return datetime(date_value.year, date_value.month, date_value.day, date_value.hour, date_value.minute)
            
    def update_series(self, bson, old_bson=None, is_bulk=False):
        # gets either last_update passed to Datasets or the one provided
        # in the bson
        if not 'slug' in bson:
            bson['slug'] = self.slug(bson['key'])                                
        
        last_update = self.format_last_update(self.last_update)
        if 'last_update' in bson:
            last_update = self.format_last_update(bson.pop('last_update'))

        col = self.fetcher.db[constants.COL_SERIES]

        if not old_bson:
            bson['release_dates'] = [last_update for v in bson['values']]
            schemas.series_schema(bson)

            self.check_values_attributes_releasedates(bson)
            
            if is_bulk:
                return bson
            return col.insert(bson)
        else:
            revisions_is_present = False
            if 'revisions' in old_bson and len(old_bson['revisions']) > 0:
                bson['revisions'] = old_bson['revisions']
                revisions_is_present = True

            start_date = bson['start_date']
            old_start_date = old_bson['start_date']
            bson['release_dates'] = deepcopy(old_bson['release_dates'])
            
            iv1 = iv2 = 0
            if start_date < old_start_date:
                # index of old_bson values in bson values 
                iv2 = old_start_date - start_date
                # update all positions
                if revisions_is_present:
                    offset = old_start_date - start_date
                    ikeys = [int(k) for k in bson['revisions']]
                    for p in sorted(ikeys,reverse=True):
                        bson['revisions'][str(p+offset)] = bson['revisions'][str(p)]
                        bson['revisions'].pop(str(p))
                # add last_update in fron of release_dates
                bson['release_dates'] = [last_update for r in range(iv2)] + bson['release_dates']
                        
            elif start_date > old_start_date:
                # previous, longer, series is kept
                # fill beginning with na
                for p in range(start_date-old_start_date):
                    # insert in front of the values, release_dates and attributes
                    bson['values'].insert(0,'na')
                    bson['release_dates'][p] = last_update
                    for a in bson['attributes']:
                        bson['attributes'][a].insert(0,"") 
                bson['start_date'] = old_bson['start_date']
                
            if bson['end_date'] < old_bson['end_date']:
                for p in range(old_bson['end_date']-bson['end_date']):
                    bson['values'].append('na')
                    bson['release_dates'][p] = last_update
                    for a in bson['attributes']:
                        bson['attributes'][a].append("")
                bson['end_date'] = old_bson['end_date']

            for position,values in enumerate(zip(old_bson['values'][iv1:],bson['values'][iv2:])):
                if values[0] != values[1]:
                    bson['release_dates'][position+iv2] = last_update
                    if not revisions_is_present:
                        bson['revisions'] = {}
                        revisions_is_present = True
                    rev = {'value':values[0],
                           'release_date':old_bson['release_dates'][position+iv1]}
                    if str(position+iv2) in bson['revisions']:
                        bson['revisions'][str(position+iv2)].append(rev)
                    else:
                        bson['revisions'][str(position+iv2)] = [rev]

            if bson['end_date'] > old_bson['end_date']:
                for p in range(bson['end_date']-old_bson['end_date']):
                    bson['release_dates'].append(last_update)

            self.check_values_attributes_releasedates(bson)
            
            schemas.series_schema(bson)
            if is_bulk:
                return bson
            return col.find_one_and_update({'_id': old_bson['_id']}, {'$set': bson})

    def check_values_attributes_releasedates(self,bson):
        # checking consistency of values, release_dates and attributes
        n = len(bson['values'])
        if len(bson['release_dates']) != n:
            logger.critical('release_dates has not the right length')
            logger.critical('series key: ' + bson['key'])
            logger.critical('values length: ' + str(len(bson['values'])))
            logger.critical('release_dates length: ' + str(len(bson['release_dates'])))
            raise Exception('release_dates has not the right length')
        """
        for a in bson['attributes']:
            if len(bson['attributes'][a]) != n:
                logger.critical('attributes has not the right length')
                logger.critical('series key: ' + bson['key'])
                logger.critical('values length: ' + str(len(bson['values'])))
                logger.critical('attributes length: ' + str(len(bson['release_dates'])))
                raise Exception('attributes has not the right length')
        """
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
        
    def update_entry(self,dim_name,dim_short_id,dim_long_id):
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
        return(self.code_dict)

    def get_list(self):
        return({d1: list(d2.items()) for d1,d2 in self.code_dict.items()})

    def set_dict(self,arg):
        self.code_dict = arg
        
    def set_from_list(self,dimension_list):
        self.code_dict = {d1: OrderedDict(d2) for d1,d2 in dimension_list.items()}
    

class DataTreeEntry(object):

    def __init__(self, name=None, category_code=None, doc_href=None,
                 last_update=None, datasets=None, description=None,
                 exposed=False, is_root=False):
        self.name = name
        self.category_code = category_code
        self.doc_href = doc_href
        self.last_update = last_update ##TODO: voir si utile pour dire qu'au moins 1 dataset dans la branche est recent ?
        self.datasets = datasets or []
        self.description = description
        self.exposed = exposed
        self.is_root = is_root
        
    def add_dataset(self, dataset_code=None, name=None, 
                    last_update=None, exposed=True):                
        dataset = OrderedDict()
        dataset["dataset_code"] = dataset_code
        dataset["name"] = name
        dataset["last_update"] = last_update
        dataset["exposed"] = exposed
        self.datasets.append(dataset)
        self.exposed = True

    @property        
    def bson(self):
        return {
            "name": self.name,
            "category_code": self.category_code,
            "doc_href": self.doc_href,
            "last_update": self.last_update,
            "datasets": sorted([d for d in self.datasets], key=itemgetter("dataset_code")),
            "description": self.description,
            "exposed": self.exposed
        }

