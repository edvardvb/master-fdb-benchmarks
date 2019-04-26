import argparse

from constants import DOCUMENT_LAYER, STANDARD_MONGO, TRANSACTIONAL_MONGO, STRICT_MONGO
from utils import get_client
from workloads.workload_a import Workload_A


parser = argparse.ArgumentParser()
parser.add_argument('-runner', required=True, choices=[DOCUMENT_LAYER, STANDARD_MONGO, TRANSACTIONAL_MONGO, STRICT_MONGO])
parser.add_argument('-num_runs', required=True, type=int)
args = parser.parse_args()

db = get_client(args.runner)['fdb-benchmark']
collection = db[args.runner]

workload = Workload_A(collection)

total_runtime, avg_runtime, throughput = workload.benchmark(args.num_runs)

print(f'⏱  Total runtime: {total_runtime}')
print(f'🧮  Average runtime: {avg_runtime}')
print(f'🏎  Throughput: {throughput}')
