import time
import numpy
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
        self.num_readmod = 0
        self.run_scan_length = 0
        self.read_id = None
        self.op_set = None

    def setup(self, run):
        print(f"🔌 Setting up for run {run}")
        self.num_read = 0
        self.num_update = 0
        self.num_insert = 0
        self.num_readmod = 0
        self.run_scan_length = 0
        self.collection.drop()
        print(f"🧹  Collection cleaned")
        generate_data(self.records, self.collection)
        self.collection.create_index([("item", ASCENDING)])
        print("🧮  Index built on `item`")
        print(
            f"🚄 Performing {self.operations} operations, using runner {self.runner} on {self.__repr__()}"
        )
        self.read_id = self.collection.find_one().get('_id')

        zipf_set = numpy.random.zipf(2, self.operations)
        normalized = (zipf_set/float(max(zipf_set)))*999
        self.op_set = [int(e) for e in normalized]

    def benchmark(self, now, num_runs, write):
        if not num_runs:
            num_runs = 5

        total_reads = 0
        total_updates = 0
        total_inserts = 0
        total_readmods = 0
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
            total_readmods += self.num_readmod
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

        if write:
            filename = self.get_filename(now)
            outstring = self.get_outstring(
                total_reads,
                total_updates,
                total_inserts,
                total_readmods,
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
        total_readmods,
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
        if total_readmods:
            outstring += f"Total number of read-modify-writes;{total_readmods}\n"

        outstring += (
            f"Read percentage;{(total_reads / (self.operations*num_runs)) * 100}%\n"
        )

        if total_scan_length:
            outstring += f"Average scan length;{total_scan_length / total_reads}\n"

        return outstring

    def get_filename(self, now):
        path = f"runs/{self.runner}/"
        date = f"{now.year}-{now.month}-{now.day}"
        hour = now.hour if now.hour > 9 else f"0{now.hour}"
        minute = now.minute if now.minute > 9 else f"0{now.minute}"
        time = f"{hour}:{minute}"
        return f"{path}{self.__repr__()[-1]} {date} {time}.csv"
