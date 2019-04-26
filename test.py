import argparse
import timeit
from constants import DOCUMENT_LAYER, STANDARD_MONGO, TRANSACTIONAL_MONGO, STRICT_MONGO
from utils import get_client

parser = argparse.ArgumentParser()
parser.add_argument('-runner', required=True, choices=[DOCUMENT_LAYER, STANDARD_MONGO, TRANSACTIONAL_MONGO, STRICT_MONGO])
parser.add_argument('-num_runs', required=True, type=int)
args = parser.parse_args()

db = get_client(args.runner)['fdb-benchmark']
collection = db[args.runner]


def perform_test():
    for i in range(100):
        collection.insert_one(
            {
                "item" : "canvas" + str(i),
                "qty" : 100 + i,
                "tags" : ["cotton"],
                "title" : "How do I create manual workload i.e., Bulk inserts to Collection ",
                " Iteration no:" : i
            }
        )
    collection.drop()

t = timeit.timeit(perform_test, number=args.num_runs)

print(t/args.num_runs)





