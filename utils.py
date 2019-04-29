import inspect
import random

import pymongo as pm


def generate_data(number_of_records, collection):
    new_docs = []
    for i in range(number_of_records):
        new_docs.append(
            {"item": i, "qty": random.randint(1, 11), "tags": ["tag"], "title": "title"}
        )
    collection.insert_many(new_docs)
    print("👷‍♂️ Dataset generated")
    print(f"🚚 {number_of_records} records inserted")


def transactional(func):
    index = inspect.getargspec(func).args.index("db")

    def func_wrapper(*args, **kwargs):
        db = args[index]
        committed = False
        db.command("beginTransaction")
        while not committed:
            try:
                ret = func(*args, **kwargs)
                db.command("commitTransaction")
                committed = True
            except pm.errors.OperationFailure as e:
                print(e.details)
                db.command("beginTransaction", retry=True)
        return ret

    return func_wrapper
