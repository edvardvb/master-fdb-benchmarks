import time
from abc import ABC, abstractmethod

from pymongo import ASCENDING

from utils import generate_data


class Workload(ABC):
    def __init__(self, db, runner, records, operations):
        self.db = db
        self.collection = db[runner]
        self.runner = runner
        self.records = records
        self.operations = operations

        self.num_read = 0
        self.num_update = 0
        self.num_insert = 0
        self.run_scan_length = 0

    def setup(self, run):
        print(f"🔌 Setting up for run {run}")
        self.num_read = 0
        self.num_update = 0
        self.num_insert = 0
        self.run_scan_length = 0
        self.collection.drop()
        print(f"🧹  Collection cleaned")
        generate_data(self.records, self.collection)
        self.collection.create_index([("item", ASCENDING)])
        print("🧮  Index built on `item`")
        print(
            f"🚄 Performing {self.operations} operations, using runner {self.runner} on {self.__repr__()}"
        )

    def benchmark(self, now, num_runs):
        if not num_runs:
            num_runs = 5

        total_reads = 0
        total_updates = 0
        total_inserts = 0
        total_runtime = 0
        total_scan_length = 0

        for i in range(num_runs):
            self.setup(i + 1)
            start = time.time()
            output = getattr(self, f"benchmark_{self.runner}")()
            end = time.time()

            runtime = end - start

            total_reads += self.num_read
            total_updates += self.num_update
            total_inserts += self.num_insert
            total_runtime += runtime
            total_scan_length += self.run_scan_length
            throughput = self.operations / runtime
            print(output)
            print(f"⏱  Runtime: {runtime}")
            print(f"🏎  Throughput: {throughput}")
            print()

        avg_runtime = total_runtime / num_runs

        print(
            f"== Summary for {num_runs} run{'s' if num_runs > 1 else ''} of {self.__repr__()} on runner {self.runner} =="
        )
        print(f"⏱  Average runtime: {avg_runtime}")
        print(f"🏎  Average throughput: {(self.operations)/avg_runtime}")
        print()

        filename = self.get_filename(now)
        outstring = self.get_outstring(
            total_reads,
            total_updates,
            total_inserts,
            total_runtime,
            total_scan_length,
            num_runs,
        )

        with open(filename, "w") as f:
            f.write(outstring)

        self.collection.drop()

    @abstractmethod
    def benchmark_mongo3(self):
        pass

    @abstractmethod
    def benchmark_mongo4(self):
        pass

    @abstractmethod
    def benchmark_fdbdl(self):
        pass

    def get_outstring(
        self,
        total_reads,
        total_updates,
        total_inserts,
        total_runtime,
        total_scan_length,
        num_runs,
    ):

        outstring = (
            f"Total runtime;{total_runtime}\n"
            + f"Number of runs;{num_runs}\n"
            + f"Average runtime;{total_runtime/num_runs}\n"
            + f"Operations per run;{self.operations}\n"
            + f"Average throughput;{(self.operations*num_runs)/total_runtime}\n"
            + f"Total number of reads;{total_reads}\n"
        )
        if total_updates:
            outstring += f"Total number of updates;{total_updates}\n"
        if total_inserts:
            outstring += f"Total number of inserts;{total_inserts}\n"
        if total_scan_length:
            outstring += f"Average scan length;{total_scan_length / self.num_read}\n"

        return outstring

    def get_filename(self, now):
        path = f"runs/{self.runner}/"
        date = f"{now.year}-{now.month}-{now.day}"
        hour = now.hour if now.hour > 9 else f"0{now.hour}"
        minute = now.minute if now.minute > 9 else f"0{now.minute}"
        time = f"{hour}:{minute}"
        return f"{path}{self.__repr__()[-1]} {date} {time}.csv"
