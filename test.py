import argparse
from datetime import datetime

from constants import DOCUMENT_LAYER, STANDARD_MONGO, TRANSACTIONAL_MONGO
from setup import get_database, get_workload

parser = argparse.ArgumentParser()
parser.add_argument(
    "-runners",
    nargs="+",
    required=True,
    choices=[DOCUMENT_LAYER, STANDARD_MONGO, TRANSACTIONAL_MONGO],
)
parser.add_argument(
    "-workloads", nargs="+", required=True, choices=["a", "b", "c", "d", "e", "f"]
)
parser.add_argument("-num_runs", type=int)
args = parser.parse_args()

print(args.runners)
print(args.workloads)
print(f"Number of runs per workload: {args.num_runs if args.num_runs else 5}")

print()
now = datetime.now()


for runner in args.runners:
    print(
        f"🚀 Running {len(args.workloads)} workload{'s' if len(args.workloads) > 1 else ''} on runner {runner}"
    )
    print()
    db = get_database(runner)
    for wl in args.workloads:
        print(f"== 👨‍🎓 Preparing workload {wl.upper()} ==")
        print()
        workload = get_workload(wl, db, runner)
        workload.benchmark(now, args.num_runs)
