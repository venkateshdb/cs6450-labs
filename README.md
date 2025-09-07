## Results
1. We achieved above 22,000,000 op/s
2. We're using all cores and up to 2.9 GBs of memory on one of the server node, when running at maximum number of nodes with 64 workers and batch_size of 10000. The network usage peaked around 4.35 GB/s between client and server.
3. 1 node = 6458430 op/s, 2 nodes = 12289743 op/s, 4 nodes = 22531773 op/s. We can see the performance scales roughly linearly.

*System resource usage on client when using maximum number of nodes*
![System resource usage on client when using maximum number of nodes](images/client.png)

*System resource usage on server when using maximum number of nodes*
![System resource usage on server when using maximum number of nodes](images/server.png)

*Bandwidth Consumption between Client and server*
![Bandwidth Consumption between client and server](images/network.png)

## Design

The most important design change we made is to get rid of the mutex, since it is not an low-level, atomic primitive. Instead, we used sync.Map, which is "thread safe for concurrent use by multiple goroutines without additional locking or coordination." This way we can greatly reduce the contention caused by multiple goroutines acquiring mutex.

On the client side, we also use async RPC calls to send request insted of previous blocking calls, This frees our CPU to do other tasks. Another change we made was running multiple goroutines per client to send requests to server to fully utilize availabe CPU cores.

We added 2 knobs to get better throughput :
1. Workers : Number of goroutines for run from each client node.
2. Batch Size: Number of operation to send to the server in one trip

Another design change we made is batching the requests and responses. This way we can greatly reduce the overhead of creating goroutines. One thing worth pointing out is that we used an atomic counter (by using atomic.Uint64) for fast and safe request number tracking which was used as a timestamp to maintain order the operations that are then sorted on server to maintain real time odering, hence providing linearizability.

Lastly, we shard the kvs to allow each server only serve parts of the kvs, which also reduces the contention.

All of these methods combined provides fast and linearizable semantics for the distributed kvs. The usage of sync.Map and sharding guarantees all the requests will be visible in some total order. And the atomic counter will ensure all requests from one client will respect real-time ordering.

## Reproduce The Results

Make sure you can write to the ./log directory.
Make sure go is in your PATH.

./run-cluster.sh 4 4 "" "-workers 64 -batch_size 10000" (you can vary number of workers and batch_size)

## Reflections

The request batching worked surprisingly well for optimization. We were not expecting the overhead of creating goroutines are that high. And after implementing sharding, getting rid of the mutex, and batching the requests, the performance of the kvs is very good.
For future improvements, we can probably do multiple shards on one server node, which can also increase the performance.

Contributions:
    Axe: Report & performance analysis