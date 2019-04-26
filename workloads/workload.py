import timeit
from abc import ABC, abstractmethod

from utils import generate_data, timeit_patch

timeit.template = timeit_patch

class Workload(ABC):

    def __init__(self, collection, runner, records, operations):
        self.collection = collection
        self.runner = runner
        self.records = records
        self.operations = operations
        generate_data(self.records, self.collection)

    @abstractmethod
    def run_benchmark(self):
        pass

    def benchmark(self):
        runtime, output = timeit.timeit(self.run_benchmark, number=1)

        throughput = self.operations/runtime
        self.collection.drop()

        return runtime, throughput, output

    @abstractmethod
    def benchmark_mongo3(self):
        pass

    @abstractmethod
    def benchmark_mongo4(self):
        pass

    @abstractmethod
    def benchmark_fdbdl(self):
        pass

    def benchmark_mongowc(self):
        return self.benchmark_mongo3()
