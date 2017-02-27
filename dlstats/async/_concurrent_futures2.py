
import logging
from collections import deque
import concurrent.futures

import pymongo

from widukind_common.debug import timeit

from dlstats import constants
from dlstats.utils import last_error

logger = logging.getLogger(__name__)

from dlstats.fetchers._commons2 import Series, update_series_list_unit

@timeit("async.concurrent_futures.Series.update_series_list.execute", stats_only=True)
def bulk_execute(bulk_requests):
    try:
        bulk_requests.execute()
    except pymongo.errors.BulkWriteError as err:
        logger.critical(str(err.details))
        raise

class AsyncSeries(Series):
    
    @timeit("async.concurrent_futures.Series.update_series_list", stats_only=True)
    def update_series_list(self):
        
        keys = [s['key'] for s in self.series_list]
    
        query = {
            'provider_name': self.provider_name,
            'dataset_code': self.dataset_code,
            'key': {'$in': keys}
        }
        projection = {"tags": False}
    
        db = self.get_db()
        cursor = db[constants.COL_SERIES].find(query, projection)
    
        old_series = {s['key']:s for s in cursor}
    
        bulk_insert = []
        bulk_update = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.fetcher.pool_size) as executor:
                
            tasks = []    
            for data in self.series_list:
                tasks.append(executor.submit(update_series_list_unit, 
                                             data, 
                                             old_series=old_series, 
                                             last_update=self.last_update, 
                                             provider_name=self.provider_name, 
                                             dataset_code=self.dataset_code))
            
            for future in concurrent.futures.as_completed(tasks):
                try:
                    result = future.result()
                    if not result:
                        continue
                    
                    _insert, _update = result
                    if _insert:
                        bulk_insert.append(_insert)
                        self.count_inserts += 1
                    elif _update:
                        bulk_update.append(_update)
                        self.count_updates += 1
                except Exception as err:
                    self.count_errors += 1
                    logger.critical(last_error())
    
        result = None        
        if len(bulk_insert) + len(bulk_update) > 0:
    
            bulk_requests = db[constants.COL_SERIES].initialize_unordered_bulk_op()
            
            for bulk in bulk_insert:
                bulk_requests.insert(bulk)
            
            for bulk in bulk_update:
                bulk_requests.find(bulk[0]).update_one(bulk[1])
                
            bulk_execute(bulk_requests) 
            
        self.series_list = deque()
        return result
    
    
