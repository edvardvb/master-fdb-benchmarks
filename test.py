import argparse

from constants import DOCUMENT_LAYER, STANDARD_MONGO, TRANSACTIONAL_MONGO, STRICT_MONGO
from setup import get_client, get_workload


parser = argparse.ArgumentParser()
parser.add_argument('-runner', required=True, choices=[DOCUMENT_LAYER, STANDARD_MONGO, TRANSACTIONAL_MONGO, STRICT_MONGO])
parser.add_argument('-workload', required=True, choices=['a'])
args = parser.parse_args()

db = get_client(args.runner)['fdb-benchmark']
collection = db[args.runner]

workload = get_workload(args.workload, collection, args.runner)

runtime, throughput, output = workload.benchmark()
print(output)
print(f'⏱  Runtime: {runtime}')
print(f'🏎  Throughput: {throughput}')
