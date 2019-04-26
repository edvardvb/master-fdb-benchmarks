import random

from workloads.workload import Workload
from constants import READ, UPDATE


class Workload_A(Workload):
    """
      95/5 read/update
      1000 records
      100000 operations
      :return:
    """

    def __init__(self, collection, runners):
        records = 1000
        operations = 100000
        super().__init__(collection, runners, records, operations)

    def run_benchmark(self):
        return getattr(self, f'benchmark_{self.runner}')()

    def benchmark_mongo3(self):
        num_read = 0
        num_update = 0
        for i in range (self.operations):
            op = random.choices([READ, UPDATE], [95, 5])[0]
            if op == READ:
                num_read += 1
                self.collection.find_one({'_id': i//100})
            elif op == UPDATE:
                num_update += 1
                self.collection.update_one(
                    {'_id': i//100},
                    {'$set': {
                        'title': f"Updated at operation {i}"
                    }
                     })
        return (
            f'📖 Number of reads: {num_read}\n' +
            f'✍️  Number of updates: {num_update}\n' +
            f'{(num_read/self.operations)*100}% reads'
        )

    def benchmark_mongo4(self):
        pass

    def benchmark_fdbdl(self):
        pass
