## Results

1. Our optimized kv store is able to complete 22M op/s on 8 m510 machines, almost 500x above our baseline of 45234.
2. We are able to maximize use of our hardware:
    - We use up to 11 GBs of memory when running at maximum number of nodes.
    - The network usage peaked around 1 GB/s, which is close to the physical limits of the network ports.
    - Running `htop` on one of our client nodes highlights that we used roughly 68% of compute on each of the available 16 cores while running in dev mode (4 cloudlab nodes).
3. 1 node = 6458430 op/s, 2 nodes = 12289743 op/s, 4 nodes = 22531773 op/s. We can see the performance scales roughly linearly.

## Design

The most important design change we made is to get rid of the mutex, since it is not an low-level, atomic primitive. Instead, we used sync.Map, which is "thread safe for concurrent use by multiple goroutines without additional locking or coordination." This way we can greatly reduce the contention caused by multiple goroutines acquiring mutex.

Another design change we made is batching the requests and responses. This way we can greatly reduce the overhead of creating goroutines. One thing worth pointing out is that we used an atomic counter (by using `atomic.Uint64`) for fast and safe request number tracking.

Lastly, we shard the kvs to allow each server only serve parts of the kvs, which also reduces the contention.

All of these methods combined provides fast and linearizable semantics for the distributed kvs. The usage of sync.Map and sharding guarantees all the requests will be visible in some total order. And the atomic counter will ensure all requests from one client will respect real-time ordering.

## Reproduce The Results

Make sure you can write to the ./log directory.
Make sure go appears in your PATH.

1. Reserve 8 m510 machines on CloudLab using the small-lan profile with Ubuntu 24.04 as an image of choice.
2. Run setup in 1 node of choice: `ssh -A [CL-HOST-ADDR] && /proj/utah-cs6450-PG0/bin/setup-nfs && /proj/utah-cs6450-PG0/bin/install-go`
3. Enter the nfs directory `/mnt/nfs/`
4. Clone repo: `git clone https://github.com/venkateshdb/cs6450-labs.git  && cd cs6450-labs && git checkout pa1-turnin`
5. Run: `./run-cluster.sh`

## Reflections

The request batching worked surprisingly well for optimization. We were not expecting the overhead of creating goroutines are that high. And after implementing sharding, getting rid of the mutex, and batching the requests, the performance of the kvs is very good.
For future improvements, we can probably do multiple shards on one server node, which should also increase the performance.

Contributions:

- Venkatesh: Github Setup and Optimizations
- Axe: Report & performance analysis
- Parth: Report & performance analysis.
- Dibri: Report, performance analysis, and explored additional optimizations.
