from . import configuration
from dlstats.logger import logger
import elasticsearch

elasticsearch_client = elasticsearch.Elasticsearch(host = configuration['ElasticSearch']['host'])
