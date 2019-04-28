import argparse

from constants import DOCUMENT_LAYER, STANDARD_MONGO, TRANSACTIONAL_MONGO
from setup import get_database, get_workload


parser = argparse.ArgumentParser()
parser.add_argument('-runner', required=True, choices=[DOCUMENT_LAYER, STANDARD_MONGO, TRANSACTIONAL_MONGO])
parser.add_argument('-workload', required=True, choices=['a', 'b', 'c'])
args = parser.parse_args()


db = get_database(args.runner)

workload = get_workload(args.workload, db, args.runner)

runtime, throughput, output = workload.benchmark()
print(output)
print(f'⏱  Runtime: {runtime}')
print(f'🏎  Throughput: {throughput}')
