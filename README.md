# fdb-benchmarks
Benchmarking tool for comparing MongoDB and FoundationDB Document Layer, written in Python.

Sets up an environment where a set of workloads are performed X amount of times, for each specified database runner. Workloads define different sets of operations to execute on the database, with patterns that emulate real-world scenarios (e.g. Zipf distribution, or read-latest-insert). Each workload implements a method for performing the operations that is adapted and specialized to each database. Outputs the average throughput for each workload per database runner to file, as well as the number of operations performed, grouped on operation type.

#### First time setup
- Install FoundationDB client and server, and FoundationDB Document Layer (https://www.foundationdb.org/download/)
- Install MongoDB client and daemon (https://docs.mongodb.com/manual/installation/)
- Clone repo
  - SSH: `git clone git@github.com:edvardvb/fdb-benchmarks.git`
  - HTTPS: `git clone https://github.com/edvardvb/fdb-benchmarks.git`
- Enter directory
  - `cd fdb-benchmarks`
- Create virtual environment for Python
  - `python3 -m venv venv`
- Install requirements
  - `source venv/bin/activate`
  - `pip install -r requirements.txt`
- Ensure FDB and FDB-DL is running (see FDB and FDB-DL documentation)
- Start mongo daemon with a replica set
  - `mongod -replSet rs`
- Initiate replica set in mongo client: 
  - `mongo`
  - `rs.initiate()`

#### Regular setup
- Activate virtual environment
  - `source venv/bin/activate`
- Ensure FDB and FDB-DL is running (see FDB and FDB-DL documentation)
- Start mongo daemon with a replica set
  - `mongod -replSet rs`
  
#### Usage
- `python test.py`
- **Flags and arguments**
  - `-runners`
    - Required argument, one or more
    - Options:
      - `fdbdl`, `mongo3`, `mongo4`
    - Example: `-runners fdbdl mongo3 mongo4`
      - Performs chosen workloads on FoundationDB Document Layer, standard MongoDB, and MongoDB with transactions. 
  - `-workloads`
    - Required argument, one or more
    - Options:
      - `a`, `b`, `c`, `d`, `e` or `f`
    - Example: `-workloads a e f`
      - Performs workloads A, E and F on chosen runners.
  - `-num_runs` 
    - Optional argument
    - Takes an integer, signifying the number of runs to perform per workload. Default is 5 runs.
  - `-no_write`
    - Optional flag
    - When set, outputs are not written to file, but still printed in the console.
- Example command:
  - `python test.py -runners fdbdl mongo3 mongo4 -workloads a b c d e f -num_runs 100`
  - Output: Six `.csv`-files for each runner (one for each workload), containing the average ops/sec, as well as total ops grouped on operation type. Prints status per run in console.
