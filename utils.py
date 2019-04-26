def generate_data(number_of_records, collection):
    new_docs = []
    for i in range(number_of_records):
        new_docs.append(
            {
                "item" : "canvas" + str(i),
                "qty" : 100 + i,
                "tags" : ["cotton"],
                "title" : "How do I create manual workload i.e., Bulk inserts to Collection "
            }
        )
    collection.insert_many(new_docs)
    print('Dataset generated')
    print(f'{number_of_records} records inserted')

timeit_patch = """
def inner(_it, _timer{init}):
    {setup}
    _t0 = _timer()
    for _i in _it:
        retval = {stmt}
    _t1 = _timer()
    return _t1 - _t0, retval
"""