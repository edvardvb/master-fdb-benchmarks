import argparse
import timeit
from pymongo import MongoClient


DOCUMENT_LAYER = 'fdbdl'
STANDARD_MONGO = 'mongo3'
TRANSACTIONAL_MONGO = 'mongo4'
STRICT_MONGO = 'mongowc'

parser = argparse.ArgumentParser()
parser.add_argument('-runner', choices=[DOCUMENT_LAYER, STANDARD_MONGO, TRANSACTIONAL_MONGO, STRICT_MONGO])
args = parser.parse_args()

if args.runner:
    runner = args.runner
else: runner = DOCUMENT_LAYER

if runner == DOCUMENT_LAYER:
    client = MongoClient('mongodb://localhost:27016/')
else:
    client = MongoClient()

db = client['fdb-benchmark']
collection = db[runner]

def perform_test():
    for i in range(100):
        collection.insert(
            {
                "item" : "canvas" + str(i),
                "qty" : 100 + i,
                "tags" : ["cotton"],
                "title" : "How do I create manual workload i.e., Bulk inserts to Collection ",
                " Iteration no:" : i
            }
        )

t = timeit.timeit(perform_test, number=1)

print(t)

collection.drop()

