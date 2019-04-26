# fdb-benchmarks
Benchmarking tool for comparing MongoDB and FoundationDB Document Layer, written in Python

#### Setup
- Install FoundationDB client and server
- Install FoundationDB Document Layer
- Install MongoDB client and daemon
- Clone repo
- `cd fdb-benchmarks`
- `python3 -m venv venv`
- `source venv/bin/activate`
- `pip install -r requirements.txt`

#### Usage
- Ensure FDB and FDB-DL is running
- `mongod`
- `source venv/bin/activate`
- `python test.py -runner mongo3 -workload a`
- Replace `mongo3` with either `fdbdl`,`mongo4`, or `mongowc` as necessary.
- Replace `a` with the corresponding letter for other workloads as necessary.