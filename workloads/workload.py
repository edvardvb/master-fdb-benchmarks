import timeit
from abc import ABC, abstractmethod

from utils import generate_data, timeit_patch

timeit.template = timeit_patch

class Workload(ABC):

    def __init__(self, collection, records, operations):
        self.collection = collection
        self.records = records
        self.operations = operations
        generate_data(self.records, self.collection)

    @abstractmethod
    def perform_workload(self):
        pass

    def benchmark(self):
        runtime, output = timeit.timeit(self.perform_workload, number=1)

        throughput = self.operations/runtime
        self.collection.drop()

        return runtime, throughput, output

