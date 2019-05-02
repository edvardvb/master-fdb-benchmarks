import random

from pymongo import read_concern, write_concern

from constants import INSERT, READ
from utils import transactional
from workloads.workload import Workload


class Workload_D(Workload):
    """
      95/5 read/insert
      1000 records
      10000 operations
      Read latest insert
      :return:
    """

    def __init__(self, db, runners):
        records = 1000
        operations = 10000

        super().__init__(db, runners, records, operations)

    def __repr__(self):
        return "workload D"

    def benchmark_mongo3(self):
        ops = random.choices([READ, INSERT], [95, 5], k=self.operations)
        for i, op in enumerate(ops):
            if op == READ:
                self.num_read += 1
                self.collection.find_one({"_id": self.read_id})
            elif op == INSERT:
                self.num_insert += 1
                self.read_id = self.collection.insert_one(
                    {
                        "item": self.records + i,
                        "qty": 100 + i,
                        "tags": ["tag"],
                        "title": "title",
                    }
                ).inserted_id
        return (
            f"📖 Number of reads: {self.num_read}\n"
            + f"✍️  Number of inserts: {self.num_insert}\n"
            + f"🔎 {(self.num_read / self.operations) * 100}% reads"
        )

    def benchmark_mongo4(self):
        rc = read_concern.ReadConcern("majority")
        wc = write_concern.WriteConcern("majority")
        batch_size = 1000
        print(f"Batch size: {batch_size}")

        with self.collection.database.client.start_session() as session:
            for i in range(int(self.operations / batch_size)):
                ops = random.choices([READ, INSERT], [95, 5], k=batch_size)
                with session.start_transaction(read_concern=rc, write_concern=wc):
                    for op in ops:
                        if op == READ:
                            self.num_read += 1
                            self.collection.find_one(
                                {"_id": self.read_id}, session=session
                            )
                        elif op == INSERT:
                            self.num_insert += 1
                            self.read_id = self.collection.insert_one(
                                {
                                    "item": self.records + i,
                                    "qty": 100 + i,
                                    "tags": ["tag"],
                                    "title": "title",
                                }
                            ).inserted_id

            return (
                f"📖 Number of reads: {self.num_read}\n"
                + f"✍️  Number of inserts: {self.num_insert}\n"
                + f"🔎 {(self.num_read / self.operations) * 100}% reads"
            )

    @transactional
    def perform_operations(self, db, ops, i):
        for op in ops:
            if op == READ:
                self.num_read += 1
                self.collection.find_one({"_id": self.read_id})
            elif op == INSERT:
                self.num_insert += 1
                self.read_id = self.collection.insert_one(
                    {
                        "item": self.records + i,
                        "qty": 100 + i,
                        "tags": ["tag"],
                        "title": "title",
                    }
                ).inserted_id

    def benchmark_fdbdl(self):
        batch_size = 1000
        print(f"Batch size: {batch_size}")
        for i in range(int(self.operations / batch_size)):
            ops = random.choices([READ, INSERT], [95, 5], k=batch_size)
            self.perform_operations(self.db, ops, i)
        return (
            f"📖 Number of reads: {self.num_read}\n"
            + f"✍️  Number of inserts: {self.num_insert}\n"
            + f"🔎 {(self.num_read / self.operations) * 100}% reads"
        )
