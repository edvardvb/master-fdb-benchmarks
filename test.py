import argparse

from constants import DOCUMENT_LAYER, STANDARD_MONGO, TRANSACTIONAL_MONGO
from setup import get_database, get_workload


parser = argparse.ArgumentParser()
parser.add_argument('-runners', nargs='+', required=True, choices=[DOCUMENT_LAYER, STANDARD_MONGO, TRANSACTIONAL_MONGO])
parser.add_argument('-workloads', nargs='+', required=True, choices=['a', 'b', 'c', 'd'])
args = parser.parse_args()

print(args.runners)
print(args.workloads)
print()

for runner in args.runners:
    print(f'🚀 Running {len(args.workloads)} workload on runner {runner}')
    print()
    db = get_database(runner)
    for wl in args.workloads:
        print(f'👨‍🎓 Preparing workload {wl.upper()}')
        workload = get_workload(wl, db, runner)
        runtime, throughput, output = workload.benchmark()

        print(output)
        print(f'⏱  Runtime: {runtime}')
        print(f'🏎  Throughput: {throughput}')
        print()
