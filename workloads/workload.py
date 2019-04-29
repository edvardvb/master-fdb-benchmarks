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
        print(output)
        print(f'⏱  Runtime: {runtime}')
        print(f'🏎  Throughput: {throughput}')
        print()

        filename = self.get_filename()
        outstring = self.get_outstring(runtime)

        with open(filename, 'w') as f:
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

    def get_outstring(self, runtime):
        outstring = (f'Runtime;{runtime}\n' +
                     f'Operations;{self.operations}\n' +
                     f'Throughput;{self.operations/runtime}\n' +
                     f'Number of reads;{self.num_read}\n'
                     )
        if self.num_update:
            outstring += f'Number of updates; {self.num_update}\n'
        if self.num_insert:
            outstring += f'Number of inserts; {self.num_insert}\n'
        if self.total_scan_length:
           outstring += f'Average scan length; {self.total_scan_length / self.num_read}\n'

        return outstring

    def get_filename(self):
        now = datetime.now()
        path = f'runs/{self.runner}/'
        date = f'{now.year}-{now.month}-{now.day}'
        hour = now.hour if now.hour > 9 else f'0{now.hour}'
        minute = now.minute if now.minute > 9 else f'0{now.minute}'
        time = f'{hour}:{minute}'
        return f'{path}{self.__repr__()[-1]} {date} {time}.csv'