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
    records = 1000
    operations = 100000

    def perform_workload(self):
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
        print('📖 Number of reads: ', num_read)
        print('✍️  Number of updates: ', num_update)
        print((num_read/self.operations)*100, " % reads")

