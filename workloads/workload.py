import timeit
from abc import ABC, abstractmethod
from pymongo import ASCENDING
from datetime import datetime

from utils import generate_data, timeit_patch

timeit.template = timeit_patch

class Workload(ABC):

    def __init__(self, db, runner, records, operations):
        self.db = db
        self.collection = db[runner]
        self.collection.drop()
        print(f'🧹  Collection cleaned')

        self.runner = runner
        self.records = records
        self.operations = operations
        self.num_read = 0
        self.num_update = 0
        self.num_insert = 0
        self.total_scan_length = 0
        generate_data(self.records, self.collection)
        self.collection.create_index([('item', ASCENDING)])
        print('🧮  Index built on `item`')
        print(f'🚄 Performing {operations} operations, using runner {runner} on {self.__repr__()}')

    def benchmark(self):
        runtime, output = timeit.timeit(getattr(self, f'benchmark_{self.runner}'), number=1)

        throughput = self.operations/runtime
        date = datetime.now()
        with open(
                f'runs/{self.runner}/{self.__repr__()[-1]} {date.year}-{date.month}-{date.day} {date.hour}:{date.minute}',
                'w') \
                as f:

            f.write(f'Runtime;{runtime}\n' +
                    f'Operations;{self.operations}\n' +
                    f'Throughput;{throughput}\n' +
                    f'Number of reads;{self.num_read}\n' +
                    f'Number of updates; {self.num_update}\n' +
                    f'Number of inserts; {self.num_insert}\n' +
                    f'Average scan length; {self.total_scan_length/self.num_read}\n'
                    )

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