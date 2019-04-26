import timeit
from abc import ABC, abstractmethod

from utils import generate_data


class Workload(ABC):
    records = 0
    operations = 0

    def __init__(self, collection):
        generate_data(self.records, collection)
        self.collection = collection

    @abstractmethod
    def perform_workload(self):
        pass

    def benchmark(self, num_runs):
        total_runtime = timeit.timeit(self.perform_workload, number=num_runs)
        avg_runtime = total_runtime/num_runs
        throughput = self.operations/avg_runtime
        self.collection.drop()
        return total_runtime, avg_runtime, throughput

