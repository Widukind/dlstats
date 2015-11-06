from dlstats import configuration
import elasticsearch

elasticsearch_client = elasticsearch.Elasticsearch(
    host=configuration['ElasticSearch']['host']
)
