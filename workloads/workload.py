import time
from abc import ABC, abstractmethod

import numpy

from constants import (
    INSERT_INDEX,
    READ_INDEX,
    READMOD_INDEX,
    RUNTIME_INDEX,
    SCAN_INDEX,
    UPDATE_INDEX,
)
from utils import generate_data


class Workload(ABC):
    def __init__(self, db, runner, records, operations):
        self.db = db
        self.collection = db[runner]
        self.runner = runner
        self.records = records
        self.operations = operations

        # COUNTERS AND INITIALS
        self.num_read = 0
        self.num_update = 0
        self.num_insert = 0
        self.num_readmod = 0
        self.run_scan_length = 0
        self.read_id = None
        self.op_set = None
        self.totals = [0] * 6

    def setup(self, run):
        print(f"🔌 Setting up for run {run}")
        self.reset()

        generate_data(self.records, self.collection)

        print(
            f"🚄 Performing {self.operations} operations, using runner {self.runner} on {self.__repr__()}"
        )

        self.read_id = self.collection.find_one().get("_id")
        self.generate_op_set()

    def benchmark(self, now, num_runs, write):
        if not num_runs:
            num_runs = 5

        for i in range(num_runs):
            self.setup(i + 1)
            start = time.time()
            output = getattr(self, f"benchmark_{self.runner}")()
            end = time.time()

            runtime = end - start

            self.increment_totals(runtime)

            throughput = self.operations / runtime
            print(output)
            print(f"⏱  Runtime: {runtime}")
            print(f"🏎  Throughput: {throughput}")
            print()

        avg_runtime = self.totals[RUNTIME_INDEX] / num_runs

        print(
            f"== Summary for {num_runs} run{'s' if num_runs > 1 else ''} of {self.__repr__()} on runner {self.runner} =="
        )
        print(f"⏱  Average runtime: {avg_runtime}")
        print(f"🏎  Average throughput: {(self.operations)/avg_runtime}")
        print()

        if write:
            filename = self.get_filename(now)
            outstring = self.get_outstring(num_runs)

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

    def increment_totals(self, runtime):
        intermediate_results = [
            self.num_read,
            self.num_update,
            self.num_insert,
            self.num_readmod,
            runtime,
            self.run_scan_length,
        ]
        for i in range(len(self.totals)):
            self.totals[i] += intermediate_results[i]

    def get_outstring(self, num_runs):

        outstring = (
            f"Total runtime;{self.totals[RUNTIME_INDEX]}\n"
            + f"Number of runs;{num_runs}\n"
            + f"Average runtime;{self.totals[RUNTIME_INDEX]/num_runs}\n"
            + f"Operations per run;{self.operations}\n"
            + f"Average throughput;{(self.operations*num_runs)/self.totals[RUNTIME_INDEX]}\n"
            + f"Total number of reads;{self.totals[READ_INDEX]}\n"
        )
        if self.totals[UPDATE_INDEX]:
            outstring += f"Total number of updates;{self.totals[UPDATE_INDEX]}\n"
        if self.totals[INSERT_INDEX]:
            outstring += f"Total number of inserts;{self.totals[INSERT_INDEX]}\n"
        if self.totals[READMOD_INDEX]:
            outstring += (
                f"Total number of read-modify-writes;{self.totals[READMOD_INDEX]}\n"
            )

        outstring += f"Read percentage;{(self.totals[READ_INDEX] / (self.operations*num_runs)) * 100}%\n"

        if self.totals[SCAN_INDEX]:
            outstring += f"Average scan length;{self.totals[SCAN_INDEX] / self.totals[READ_INDEX]}\n"

        return outstring

    def get_filename(self, now):
        path = f"runs/{self.runner}/"
        date = f"{now.year}-{now.month}-{now.day}"
        hour = now.hour if now.hour > 9 else f"0{now.hour}"
        minute = now.minute if now.minute > 9 else f"0{now.minute}"
        time = f"{hour}:{minute}"
        return f"{path}{self.__repr__()[-1]} {date} {time}.csv"

    def reset(self):
        self.num_read = 0
        self.num_update = 0
        self.num_insert = 0
        self.num_readmod = 0
        self.run_scan_length = 0
        self.collection.drop()
        print(f"🧹  Collection cleaned")

    def generate_op_set(self):
        zipf_set = numpy.random.zipf(2, self.operations)
        normalized = (zipf_set / float(max(zipf_set))) * 999
        self.op_set = [int(e) for e in normalized]
