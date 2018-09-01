from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from searchapp.constants import DOC_TYPE, INDEX_NAME
from searchapp.data import all_products, ProductData


def main():
    # Connect to localhost:9200 by default.
    es = Elasticsearch()

    es.indices.delete(index=INDEX_NAME, ignore=404)
    es.indices.create(
        index=INDEX_NAME,
        body={
            'mappings': {},
            'settings': {},
        },
    )

    # bulk helpers accept an instance of an ElasticSearch class and an iterable 'actions' ie bulk(es, actions)
    products = all_products()
    success, _ = bulk(es, products_to_index(products), index=INDEX_NAME)
    print('Indexed {} products'.format(success))


def products_to_index(products: ProductData):
    for product in products:
        yield {
            '_op_type': 'index',
            '_type': DOC_TYPE,
            '_id': product.id,
            '_source': product.__dict__
        }

def index_product(es, product: ProductData):
    """Add a single product to the ProductData index."""

    es.create(
        index=INDEX_NAME,
        doc_type=DOC_TYPE,
        id=product.id,
        body=product.__dict__
    )

    # Don't delete this! You'll need it to see if your indexing job is working,
    # or if it has stalled.
    print("Indexed {}".format("A Great Product"))


if __name__ == '__main__':
    main()
