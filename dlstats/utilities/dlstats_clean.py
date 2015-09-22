import sys
from pymongo import MongoClient
from elasticsearch import Elasticsearch

def clean_mongo(db,filter,cleanProvider):
    if cleanProvider:
        result = db.providers.delete_one(filter)
        print(result.deleted_count,' document deleted in Mongo provider with ',filter)
    result = db.datasets.delete_many(filter)
    print(result.deleted_count,' documents deleted in Mongo collection datasets with ',filter)
    result = db.series.delete_many(filter)
    print(result.deleted_count,' document deleted in Mongo collection series with ',filter)

def clean_elastic(es,filter):
    res = es.search(index = 'widukind', scroll='1m', search_type='scan', size=1000, body={'_source': 'false','filter': {'term': filter}})
    sid = res['_scroll_id']
    scroll_size = res['hits']['total']
    while (scroll_size > 0):
        res = es.scroll(scroll_id=sid, scroll='1m')
        if len(res['hits']['hits']) == 0:
            break
        sid = res['_scroll_id']
        bulk = []
        for r in res['hits']['hits']:
            bulk.append({'delete': {'_index': str(r['_index']),
                                    '_type': str(r['_type']),
                                    '_id': str(r['_id'])}})
        res1 = es.bulk(body=bulk)
#        print(res1)
        
provider = ''
datasetCode = ''
for arg in sys.argv:        
       t = arg.partition('=')
       if t[0].lower() == 'provider':
           provider = t[2]
       elif t[0].lower() == 'dataset':
           datasetCode = t[2]

if not datasetCode and not provider:
    print('Usage:\n  provider=<provider_name>\n  dataset=<datasetCode>\n')
    sys.exit()

if datasetCode and not provider:
    print('ERROR: provider is missing')
    sys.exit()

filter = {'provider': provider}
cleanProvider = True
if datasetCode:
    cleanProvider = False
    filter.update({'datasetCode': datasetCode})
db = MongoClient().widukind
es = Elasticsearch()
clean_mongo(db,filter,cleanProvider)
filter.update({'provider': filter['provider'].lower()})
if datasetCode:
    filter.update({'datasetCode': filter['datasetCode'].lower()})
clean_elastic(es,filter)
