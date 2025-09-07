** Results **
1. We achieved above 22,000,000 op/s
2. We're using all cores and up to 11 GBs of memory when running at maximum number of nodes. The network usage peaked around 1 GB/s, which is close to the physical limits of the network ports.
3. 1 node = 6458430 op/s, 2 nodes = 12289743 op/s, 4 nodes = 22531773 op/s. We can see the performance scales roughly linearly.

** Design **

The most important design change we made is to get rid of the mutex, since it is not an low-level, atomic primitive. Instead, we used sync.Map, which is "thread safe for concurrent use by multiple goroutines without additional locking or coordination." This way we can greatly reduce the contention caused by multiple goroutines acquiring mutex.

Another design change we made is batching the requests and responses. This way we can greatly reduce the overhead of creating goroutines. One thing worth pointing out is that we used an atomic counter (by using atomic.Uint64) for fast and safe request number tracking.

Lastly, we shard the kvs to allow each server only serve parts of the kvs, which also reduces the contention.

All of these methods combined provides fast and linearizable semantics for the distributed kvs. The usage of sync.Map and sharding guarantees all the requests will be visible in some total order. And the atomic counter will ensure all requests from one client will respect real-time ordering.

** Reproduce The Results **

Make sure you can write to the ./log directory.
Make sure go is in your PATH.

./run-cluster.sh

** Reflections **

The request batching worked surprisingly well for optimization. We were not expecting the overhead of creating goroutines are that high. And after implementing sharding, getting rid of the mutex, and batching the requests, the performance of the kvs is very good.
For future improvements, we can probably do multiple shards on one server node, which can also increase the performance.

Contributions:
    Axe: Report & performance analysis