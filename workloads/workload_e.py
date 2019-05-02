import random

from pymongo import read_concern, write_concern

from constants import INSERT, READ
from utils import transactional
from workloads.workload import Workload


class Workload_E(Workload):
    """
      95/5 scanning_read/insert
      1000 records
      10000 operations
      zipfian, batched inserts
      :return:
    """

    def __init__(self, db, runners):
        records = 1000
        operations = 10000

        super().__init__(db, runners, records, operations)

    def __repr__(self):
        return "workload E"

    def benchmark_mongo3(self):
        ops = random.choices([READ, INSERT], [95, 5], k=self.operations)
        inserts = []
        for i, op in enumerate(ops):
            if op == READ:
                scan_length = random.randint(0, 10)
                start_id = self.op_set.pop()
                if start_id + scan_length > self.records:
                    start_id = start_id - ((start_id + scan_length) - self.records)
                self.num_read += 1
                self.run_scan_length += scan_length
                list(self.collection.find({"item": {"$in": list(range(start_id, start_id+scan_length))}}))
            elif op == INSERT:
                self.num_insert += 1
                inserts.append({
                    "item": self.records + i,
                    "qty": 100 + i,
                    "tags": ["tag"],
                    "title": "title",
                })
        if inserts:
            self.collection.insert_many(inserts)
        return (
            f"📖 Number of reads: {self.num_read}\n"
            + f"✍️  Number of inserts: {self.num_insert}\n"
            + f"🔎 {(self.num_read / self.operations) * 100}% reads\n"
            + f"Average scan length: {self.run_scan_length/self.num_read}"
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
                    inserts = []
                    for op in ops:
                        if op == READ:
                            scan_length = random.randint(0, 10)
                            start_id = self.op_set.pop()
                            if start_id + scan_length > self.records:
                                start_id = start_id - ((start_id + scan_length) - self.records)
                            self.num_read += 1
                            self.run_scan_length += scan_length
                            list(self.collection.find({"item": {"$in": list(range(start_id, start_id + scan_length))}}, session=session))
                        elif op == INSERT:
                            self.num_insert += 1
                            inserts.append({
                                "item": self.records + i,
                                "qty": 100 + i,
                                "tags": ["tag"],
                                "title": "title",
                            })
                    if inserts:
                        self.collection.insert_many(inserts, session=session)
            return (
                f"📖 Number of reads: {self.num_read}\n"
                + f"✍️  Number of inserts: {self.num_insert}\n"
                + f"🔎 {(self.num_read / self.operations) * 100}% reads\n"
                + f"Average scan length: {self.run_scan_length / self.num_read}"
            )

    @transactional
    def perform_operations(self, db, ops, i):
        inserts = []
        for op in ops:
            if op == READ:
                scan_length = random.randint(0, 10)
                start_id = self.op_set.pop()
                if start_id + scan_length > self.records:
                    start_id = start_id - ((start_id + scan_length) - self.records)
                self.num_read += 1
                self.run_scan_length += scan_length
                list(self.collection.find({"item": {"$in": list(range(start_id, start_id+scan_length))}}))
            elif op == INSERT:
                self.num_insert += 1
                inserts.append({
                    "item": self.records + i,
                    "qty": 100 + i,
                    "tags": ["tag"],
                    "title": "title",
                })
        if inserts:
            self.collection.insert_many(inserts)

    def benchmark_fdbdl(self):
        batch_size = 1000
        print(f"Batch size: {batch_size}")
        for i in range(int(self.operations / batch_size)):
            ops = random.choices([READ, INSERT], [95, 5], k=batch_size)
            self.perform_operations(self.db, ops, i)
        return (
            f"📖 Number of reads: {self.num_read}\n"
            + f"✍️  Number of inserts: {self.num_insert}\n"
            + f"🔎 {(self.num_read / self.operations) * 100}% reads\n"
            + f"Average scan length: {self.run_scan_length / self.num_read}"
        )
