from src.util.database import getES
from elasticsearch import Elasticsearch

es_conn = getES()

print(es_conn)
