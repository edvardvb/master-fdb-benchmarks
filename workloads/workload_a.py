import random

from pymongo import read_concern, write_concern

from constants import READ, UPDATE
from utils import transactional
from workloads.workload import Workload


class Workload_A(Workload):
    """
      50/50 read/update
      1000 records
      10000 operations
      :return:
    """

    def __init__(self, db, runners):
        records = 1000
        operations = 10000

        super().__init__(db, runners, records, operations)

    def __repr__(self):
        return "workload A"

    def benchmark_mongo3(self):
        ops = random.choices([READ, UPDATE], [50, 50], k=self.operations)
        for i, op in enumerate(ops):
            random_id = random.randint(1,self.records)
            if op == READ:
                self.num_read += 1
                self.collection.find_one({"item": random_id})
            elif op == UPDATE:
                self.num_update += 1
                self.collection.update_one(
                    {"item": random_id}, {"$set": {"title": f"Updated at operation {i}"}}
                )
        return (
            f"📖 Number of reads: {self.num_read}\n"
            + f"✍️  Number of updates: {self.num_update}\n"
            + f"🔎 {(self.num_read / self.operations) * 100}% reads"
        )

    def benchmark_mongo4(self):
        rc = read_concern.ReadConcern("majority")
        wc = write_concern.WriteConcern("majority")
        batch_size = 1000
        print(f"Batch size: {batch_size}")

        with self.collection.database.client.start_session() as session:
            for i in range(int(self.operations / batch_size)):
                ops = random.choices([READ, UPDATE], [50, 50], k=batch_size)
                with session.start_transaction(read_concern=rc, write_concern=wc):
                    for op in ops:
                        random_id = random.randint(1, self.records)
                        if op == READ:
                            self.num_read += 1
                            self.collection.find_one(
                                {"item": random_id}, session=session
                            )
                        elif op == UPDATE:
                            self.num_update += 1
                            self.collection.update_one(
                                {"item": random_id},
                                {"$set": {"title": f"Updated at operation {i}"}},
                                session=session,
                            )
            return (
                f"📖 Number of reads: {self.num_read}\n"
                + f"✍️  Number of updates: {self.num_update}\n"
                + f"🔎 {(self.num_read / self.operations) * 100}% reads"
            )

    @transactional
    def perform_operations(self, db, ops, i):
        for op in ops:
            random_id = random.randint(1,self.records)
            if op == READ:
                self.num_read += 1
                self.collection.find_one({"_id": random_id})
            elif op == UPDATE:
                self.num_update += 1
                self.collection.update_one(
                    {"_id": random_id}, {"$set": {"title": f"Updated at operation {i}"}}
                )

    def benchmark_fdbdl(self):
        batch_size = 1000
        print(f"Batch size: {batch_size}")
        for i in range(int(self.operations / batch_size)):
            ops = random.choices([READ, UPDATE], [50, 50], k=batch_size)
            self.perform_operations(self.db, ops, i)
        return (
            f"📖 Number of reads: {self.num_read}\n"
            + f"✍️  Number of updates: {self.num_update}\n"
            + f"🔎 {(self.num_read / self.operations) * 100}% reads"
        )
