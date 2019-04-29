import random
from pymongo import write_concern, read_concern

from workloads.workload import Workload
from constants import READ, INSERT
from utils import transactional



class Workload_E(Workload):
    """
      95/5 scanning_read/insert
      1000 records
      10000 operations
      :return:
    """

    def __init__(self, db, runners):
        records = 1000
        operations = 100000

        super().__init__(db, runners, records, operations)

    def __repr__(self):
        return 'workload E'



    def benchmark_mongo3(self):
        ops = random.choices([READ, INSERT], [95, 5], k=self.operations)
        for i, op in enumerate(ops):
            if op == READ:
                self.num_read += 1
                scan_length = random.randint(0,10)
                self.total_scan_length += scan_length
                list(self.collection.find(
                    {'item':
                         {'$in': list(range(scan_length))}
                     })
                )
            elif op == INSERT:
                self.num_insert += 1
                self.collection.insert_one(
                    {
                        "item": self.records + i,
                        "qty": 100 + i,
                        "tags": ["cotton"],
                        "title": "How do I create manual workload i.e., Bulk inserts to Collection "
                    })
        return (
                f'📖 Number of reads: {self.num_read}\n' +
                f'✍️  Number of inserts: {self.num_insert}\n' +
                f'🔎 {(self.num_read / self.operations) * 100}% reads\n' +
                f'Average scan length: {self.total_scan_length/self.num_read}'
        )

    def benchmark_mongo4(self):
        rc = read_concern.ReadConcern('majority')
        wc = write_concern.WriteConcern('majority')
        batch_size = 1000
        print(f'Batch size: {batch_size}')

        with self.collection.database.client.start_session() as session:
            for i in range(int(self.operations/batch_size)):
                ops = random.choices([READ, INSERT], [95, 5], k=batch_size)
                with session.start_transaction(read_concern=rc, write_concern=wc):
                    for op in ops:
                        if op == READ:
                            self.num_read += 1
                            scan_length = random.randint(0, 10)
                            self.total_scan_length += scan_length
                            list(self.collection.find(
                                {'item':
                                     {'$in': list(range(scan_length))}
                                 }, session=session)
                            )
                        elif op == INSERT:
                            self.num_insert += 1
                            self.collection.insert_one(
                                {
                                    "item": self.records + i,
                                    "qty": 100 + i,
                                    "tags": ["cotton"],
                                    "title": "How do I create manual workload i.e., Bulk inserts to Collection "
                                }, session=session)
            return (
                    f'📖 Number of reads: {self.num_read}\n' +
                    f'✍️  Number of inserts: {self.num_insert}\n' +
                    f'🔎 {(self.num_read / self.operations) * 100}% reads\n' +
                    f'Average scan length: {self.total_scan_length / self.num_read}'
            )

    @transactional
    def perform_operations(self, db, ops, i):
        for op in ops:
            if op == READ:
                self.num_read += 1
                scan_length = random.randint(0, 10)
                self.total_scan_length += scan_length
                list(self.collection.find(
                    {'item':
                         {'$in': list(range(scan_length))}
                     })
                )
            elif op == INSERT:
                self.num_insert += 1
                self.collection.insert_one(
                    {
                        "item": self.records + i,
                        "qty": 100 + i,
                        "tags": ["cotton"],
                        "title": "How do I create manual workload i.e., Bulk inserts to Collection "
                    })

    def benchmark_fdbdl(self):
        batch_size = 1000
        print(f'Batch size: {batch_size}')
        for i in range(int(self.operations / batch_size)):
            ops = random.choices([READ, INSERT], [95, 5], k=batch_size)
            self.perform_operations(self.db, ops, i)
        return (
                f'📖 Number of reads: {self.num_read}\n' +
                f'✍️  Number of inserts: {self.num_insert}\n' +
                f'🔎 {(self.num_read / self.operations) * 100}% reads\n' +
                f'Average scan length: {self.total_scan_length / self.num_read}'
        )

