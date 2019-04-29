from pymongo import MongoClient, write_concern, read_concern

from constants import DOCUMENT_LAYER, STANDARD_MONGO, TRANSACTIONAL_MONGO
from workloads.workload_a import Workload_A
from workloads.workload_b import Workload_B
from workloads.workload_c import Workload_C
from workloads.workload_d import Workload_D
from workloads.workload_e import Workload_E


def get_client(runner):
    if runner == DOCUMENT_LAYER:
        client = MongoClient('mongodb://localhost:27016/')
    elif runner == TRANSACTIONAL_MONGO:
        client = MongoClient('mongodb://localhost:27017/fdb-benchmark?replicaSet=rs')
    else:
        client = MongoClient()

    return client

def get_workload(workload, db, runner):
    if workload == 'a':
        return Workload_A(db, runner)
    if workload == 'b':
        return Workload_B(db, runner)
    if workload == 'c':
        return Workload_C(db, runner)
    if workload == 'd':
        return Workload_D(db, runner)
    if workload == 'e':
        return Workload_E(db, runner)

def get_database(runner):
    client = get_client(runner)

    if runner in [TRANSACTIONAL_MONGO]:
        wc = write_concern.WriteConcern('majority')
        rc = read_concern.ReadConcern('majority')
        return client.get_database(
            'fdb-benchmark',
            write_concern=wc,
            read_concern=rc)
    else:
        return client.get_database('fdb-benchmark')