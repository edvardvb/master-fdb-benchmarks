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
- Start mongo daemon: `mongod -replSet rs`
- If not done before, initiate replica set in mongo client: 
  - `mongo`
  - `rs.initiate()` 
- `source venv/bin/activate`
- `python test.py -runners <runner 1> <runner 2> ... -workloads <workload 1> <workload 2> ...`
  - Insert the runners you wish to benchmark. The tool can benchmark several runners at once
    - `mongo3`, `mongo4`, or `fdbdl`
  - Insert the workloads you wish to use. The tool can benchmark several workloads at once
    - `a`, `b`, `c`, `d`, `e` or `f`
