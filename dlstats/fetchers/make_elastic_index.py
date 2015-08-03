import pymongo
from elasticsearch import Elasticsearch

class ElasticIndex():
    def __init__(self):
        self.db = pymongo.MongoClient().widukind
        self.elasticsearch_client = Elasticsearch()

    def make_index(self,provider_name,dataset_code):
        mb_dataset = self.db.datasets.find_one({'provider': provider_name, 'datasetCode': dataset_code})
        mb_series = self.db.series.find({'provider': provider_name, 'datasetCode': dataset_code},{'key': 1, 'dimensions': 1, 'name': 1})
    
        es_data = self.elasticsearch_client.search(index = 'widukind', doc_type = 'datasets',
                                                   body= { "filter":
                                                           { "term":
                                                             { "_id": provider_name + '.' + dataset_code}}})
        if es_data['hits']['total'] == 0:
            es_dataset = {}
        else:
            es_dataset = es_data['hits']['hits'][0]['_source']

        es_dataset['name'] = mb_dataset['name']
        es_dataset['docHref'] = mb_dataset['docHref']
        es_dataset['lastUpdate'] = mb_dataset['lastUpdate']

        es_series = self.elasticsearch_client.search(index = 'widukind', doc_type = 'series',
                            body= { "filter":
                                    { "term":
                                      { "provider": provider_name.lower(), "datasetCode": dataset_code.lower()}}})
        es_series_dict = {e['_source']['key']: e['_source'] for e in es_series['hits']['hits']}

        mb_dimension_dict = {d1: {d2[0]: d2[1] for d2 in mb_dataset['dimensionList'][d1]} for d1 in mb_dataset['dimensionList']}
        # updating long names in ES index
        es_dimension_list = {d1: {d2[0]: mb_dimension_dict[d1][d2[0]] for d2 in es_dataset['codeList'][d1]} for d1 in es_dataset['codeList']}
        
        es_bulk = EsBulk(mb_dimension_dict)
        for s in mb_series:
            if s['key'] not in es_series_dict:
                es_bulk.add_to_index(provider_name,dataset_code,s)
            else:
                es_bulk.update_index(provider_name,dataset_code,s,es_series_dict[s['key']])
#        es_bulk.update_database()
        
class EsBulk():
    def __init__(self,mb_dimension_dict):
        self.es_bulk = []
        self.mb_dimension_dict = mb_dimension_dict
        
    def add_to_index(self,provider_name,dataset_code,s):
        mb_dim = s['dimensions']
        print(mb_dim)
        dimensions = {d: [mb_dim[d],self.mb_dimension_dict[d][mb_dim[d]]] for d in mb_dim}
        
        op_dict = {
            "index": {
                "_index": 'widukind',
                "_type": 'series',
                "_id": provider_name + '.' + dataset_code + '.' + s['key']
            }
        }
        self.es_bulk.append(op_dict)
        bson = {'provider': provider_name,
                'key': s['key'],
                'name': s['name'],
                'datasetCode': dataset_code,
                'dimensions': dimensions
        }
        print('add',bson)
        self.es_bulk.append(bson)
                                     
    def update_index(self,provider_name,dataset_code,s,es_s):
        update = False
        mb_dim = s['dimensions']
        new_bson = {}
        if es_s['name'] != s['name']:
            new_bson['name'] = s['name']
            update = True
        update1 = False
        for d1 in es_s['dimensions']:
            if es_s['dimensions'][d1] != mb_dim[d1]:
                es_s['dimensions'][d1] = mb_dim[d1]
                update1 = True
        if update1:
                new_bson['dimensions'] = es_s['dimensions']
                update = True
                
        if update:
            op_dict = {
                "update": {
                    "_index": 'widukind',
                    "_type": 'series',
                    "_id": provider_name + '.' + dataset_code + '.' + s['key']
                }
            }
            self.es_bulk.append(op_dict)
            self.es_bulk.append(new_bson)
            print(new_bson)
            
    def update_database(self):
        print(self.es_bulk)
        
if __name__ == "__main__":
    e = ElasticIndex()
    e.make_index('IMF','WEO')
