from elasticsearch import Elasticsearch
from elasticsearch_dsl import A, Q, Search
from elasticsearch_dsl.aggs import Agg


DOC_TYPE = 'products'
INDEX_NAME = 'products'
HEADERS = {'content-type': 'application/json'}


def search(term: str, count: int) -> dict:
    client = Elasticsearch()
    client.transport.connection_pool.connection.headers.update(HEADERS)

    s = Search(using=client, index=INDEX_NAME, doc_type=DOC_TYPE)
    description_query = Q('match', description=dict(query=term, operator='and', fuzziness='AUTO'))
    name_query = Q('match', name=dict(query=term, operator='and', fuzziness='AUTO'))
    dismax_query = Q('dis_max', queries=[name_query, description_query])
    docs = s.query(dismax_query).execute()

    return docs
