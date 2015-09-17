import pymongo
from elasticsearch import Elasticsearch, helpers
from collections import OrderedDict

class ElasticIndex():
    def __init__(self):
        self.db = pymongo.MongoClient().widukind
        self.elasticsearch_client = Elasticsearch()

    def make_index(self,provider_name,dataset_code):
        mb_dataset = self.db.datasets.find_one({'provider': provider_name, 'datasetCode': dataset_code})
        mb_series = self.db.series.find({'provider': provider_name, 'datasetCode': dataset_code},
                                        {'key': 1, 'dimensions': 1, 'name': 1, 'frequency': 1})
    
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
        es_dataset['provider'] = mb_dataset['provider']
        es_dataset['datasetCode'] = mb_dataset['datasetCode']
        es_dataset['frequencies'] = mb_series.distinct('frequency')
        
        es_series = self.elasticsearch_client.search(index = 'widukind', doc_type = 'series',
                            body= { "filter":
                                    { "term":
                                      { "provider": provider_name.lower(), "datasetCode": dataset_code.lower()}}})
        es_series_dict = {e['_source']['key']: e['_source'] for e in es_series['hits']['hits']}

        mb_dimension_dict = {d1: {d2[0]: d2[1] for d2 in mb_dataset['dimensionList'][d1]} for d1 in mb_dataset['dimensionList']}
        # updating long names in ES index
        if 'codeList' in es_dataset:
            es_dimension_dict = {d1: {d2[0]: mb_dimension_dict[d1][d2[0]] for d2 in es_dataset['codeList'][d1]} for d1 in es_dataset['codeList']}
        else:
            es_dimension_dict = {}
            
        es_bulk = EsBulk(self.elasticsearch_client,mb_dimension_dict)
        for s in mb_series:
            mb_dim = s['dimensions']
            s['dimensions'] = {d: [mb_dim[d],mb_dimension_dict[d][mb_dim[d]]] for d in mb_dim}
        
            if s['key'] not in es_series_dict:
                es_bulk.add_to_index(provider_name,dataset_code,s)
            else:
                es_bulk.update_index(provider_name,dataset_code,s,es_series_dict[s['key']])
            dim = s['dimensions']
            for d in dim:
                if d not in es_dimension_dict:
                    es_dimension_dict[d] = {dim[d][0]:dim[d][1]}
                elif dim[d][0] not in es_dimension_dict[d]:
                    es_dimension_dict[d].update({dim[d][0]:dim[d][1]})
        es_bulk.update_database()
        es_dataset['codeList'] = {d1: [[d2[0], d2[1]] for d2 in es_dimension_dict[d1].items()] for d1 in es_dimension_dict}
        self.elasticsearch_client.index(index = 'widukind',
                                  doc_type='datasets',
                                  id = provider_name + '.' + dataset_code,
                                  body = es_dataset)
class EsBulk():
    def __init__(self,db,mb_dimension_dict):
        self.db = db
        self.es_bulk = []
        self.mb_dimension_dict = mb_dimension_dict
        
    def add_to_index(self,provider_name,dataset_code,s):
        bson = {"_op_type": 'index', 
                "_index": 'widukind',
                "_type": 'series',
                "_id": provider_name + '.' + dataset_code + '.' + s['key'],
                'provider': provider_name,
                'key': s['key'],
                'name': s['name'],
                'datasetCode': dataset_code,
                'dimensions': s['dimensions'],
                'frequency': s['frequency']}
        self.es_bulk.append(bson)
                                     
    def update_index(self,provider_name,dataset_code,s,es_s):
        update = False
        mb_dim = s['dimensions']
        new_bson = {"_op_type": 'update',
                "_index": 'widukind',
                "_type": 'series',
                "_id": provider_name + '.' + dataset_code + '.' + s['key']}

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
            self.es_bulk.append(new_bson)
            
    def update_database(self):
        res_es = helpers.bulk(self.db, self.es_bulk, index = 'widukind')
        
if __name__ == "__main__":
    e = ElasticIndex()
    e.make_index('WorldBank','GEM')
