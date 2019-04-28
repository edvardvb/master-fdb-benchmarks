import timeit
from abc import ABC, abstractmethod

from utils import generate_data, timeit_patch

timeit.template = timeit_patch

class Workload(ABC):

    def __init__(self, db, runner, records, operations):
        self.db = db
        self.collection = db[runner]
        self.runner = runner
        self.records = records
        self.operations = operations
        generate_data(self.records, self.collection)
        print(f'🚄 Performing {operations} operations, using runner {runner} on {self.__class__.__repr__(self)}')

    def benchmark(self):
        runtime, output = timeit.timeit(getattr(self, f'benchmark_{self.runner}'), number=1)

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