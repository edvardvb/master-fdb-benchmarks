from pymongo import MongoClient

from constants import DOCUMENT_LAYER, STANDARD_MONGO, TRANSACTIONAL_MONGO, STRICT_MONGO
from workloads.workload_a import Workload_A


def get_client(runner):
    if runner == DOCUMENT_LAYER:
        client = MongoClient('mongodb://localhost:27016/')
    elif runner == STRICT_MONGO:
        client = MongoClient('mongodb://localhost:27017/fdb-benchmark?w=majority&journal=true')
    else:
        client = MongoClient()

    return client

def get_workload(workload, collection, runner):
    if workload == 'a':
        return Workload_A(collection, runner)