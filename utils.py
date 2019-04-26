from pymongo import MongoClient

from constants import DOCUMENT_LAYER, STANDARD_MONGO, TRANSACTIONAL_MONGO, STRICT_MONGO


def get_client(runner):
    if runner == DOCUMENT_LAYER:
        client = MongoClient('mongodb://localhost:27016/')
    elif runner == STRICT_MONGO:
        client = MongoClient('mongodb://localhost:27017/fdb-benchmark?w=majority&journal=true')
    else:
        client = MongoClient()

    return client

def generate_data(number_of_records, collection):
    new_docs = []
    for i in range(number_of_records):
        new_docs.append(
            {
                "item" : "canvas" + str(i),
                "qty" : 100 + i,
                "tags" : ["cotton"],
                "title" : "How do I create manual workload i.e., Bulk inserts to Collection "
            }
        )
    collection.insert_many(new_docs)
    print('Dataset generated')
    print(f'{number_of_records} records inserted')
