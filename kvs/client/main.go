package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Client struct {
	rpcClient *rpc.Client
	addr      string
}

type ClientPool struct {
	clients []*Client
	mu      sync.RWMutex
	next    int
}

func NewClientPool(hosts []string) *ClientPool {
	pool := &ClientPool{
		clients: make([]*Client, 0, len(hosts)),
	}

	for _, host := range hosts {
		client := Dial(host)
		pool.clients = append(pool.clients, client)
	}

	return pool
}

func (cp *ClientPool) GetClient() *Client {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	if len(cp.clients) == 0 {
		return nil
	}

	client := cp.clients[cp.next]
	cp.next = (cp.next + 1) % len(cp.clients)
	return client
}

func (cp *ClientPool) Close() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	for _, client := range cp.clients {
		client.Close()
	}
}

func Dial(addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{rpcClient, addr}
}

func (client *Client) Close() {
	if client.rpcClient != nil {
		client.rpcClient.Close()
		client.rpcClient = nil
	}
}

func (client *Client) Get(key string) string {
	request := kvs.GetRequest{
		Key: key,
	}
	response := kvs.GetResponse{}
	err := client.rpcClient.Call("KVService.Get", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response.Value
}

func (client *Client) Put(key string, value string) {
	request := kvs.PutRequest{
		Key:   key,
		Value: value,
	}
	response := kvs.PutResponse{}
	err := client.rpcClient.Call("KVService.Put", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
}

func (client *Client) BatchGet(keys []string) map[string]string {
	request := kvs.BatchGetRequest{
		Keys: keys,
	}
	response := kvs.BatchGetResponse{}
	err := client.rpcClient.Call("KVService.BatchGet", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
	return response.Values
}

func (client *Client) BatchPut(operations []kvs.PutRequest) {
	request := kvs.BatchPutRequest{
		Operations: operations,
	}
	response := kvs.BatchPutResponse{}
	err := client.rpcClient.Call("KVService.BatchPut", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
}

// Worker function that processes operations asynchronously
func runWorker(id int, clientPool *ClientPool, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64, wg *sync.WaitGroup) {
	defer wg.Done()

	value := strings.Repeat("x", 128)
	const batchSize = 10000

	opsCompleted := uint64(0)

	// Pre-allocate batch buffers
	getBatch := make([]string, 0, batchSize)
	putBatch := make([]kvs.PutRequest, 0, batchSize)

	for !done.Load() {
		// Collect operations into batches
		getBatch = getBatch[:0]
		putBatch = putBatch[:0]

		for j := 0; j < batchSize && !done.Load(); j++ {
			op := workload.Next()
			key := fmt.Sprintf("%d", op.Key)

			if op.IsRead {
				getBatch = append(getBatch, key)
			} else {
				putBatch = append(putBatch, kvs.PutRequest{Key: key, Value: value})
			}
		}

		// Process batches
		if len(getBatch) > 0 {
			client := clientPool.GetClient()
			client.BatchGet(getBatch)
			opsCompleted += uint64(len(getBatch))
		}

		if len(putBatch) > 0 {
			client := clientPool.GetClient()
			client.BatchPut(putBatch)
			opsCompleted += uint64(len(putBatch))
		}
	}

	fmt.Printf("Worker %d finished operations.\n", id)
	resultsCh <- opsCompleted
}

type HostList []string

func (h *HostList) String() string {
	return strings.Join(*h, ",")
}

func (h *HostList) Set(value string) error {
	*h = strings.Split(value, ",")
	return nil
}

func main() {
	hosts := HostList{}
	workerCount := flag.Int("workers", 4, "Number of worker goroutines per client")
	theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter")
	workload := flag.String("workload", "YCSB-B", "Workload type (YCSB-A, YCSB-B, YCSB-C)")
	secs := flag.Int("secs", 30, "Duration in seconds for each client to run")
	flag.Var(&hosts, "hosts", "Comma-separated list of host:ports to connect to")
	flag.Parse()

	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:8080")
	}

	fmt.Printf(
		"hosts %v\n"+
			"workers %d\n"+
			"theta %.2f\n"+
			"workload %s\n"+
			"secs %d\n",
		hosts, *workerCount, *theta, *workload, *secs,
	)

	start := time.Now()

	// Create client pool
	clientPool := NewClientPool(hosts)
	defer clientPool.Close()

	done := atomic.Bool{}
	resultsCh := make(chan uint64, *workerCount)
	var wg sync.WaitGroup

	// Start multiple workers
	for i := 0; i < *workerCount; i++ {
		wg.Add(1)
		go func(workerId int) {
			workload := kvs.NewWorkload(*workload, *theta)
			runWorker(workerId, clientPool, &done, workload, resultsCh, &wg)
		}(i)
	}

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	// Wait for all workers to finish
	wg.Wait()
	close(resultsCh)

	// Collect results
	totalOps := uint64(0)
	for ops := range resultsCh {
		totalOps += ops
	}

	elapsed := time.Since(start)
	opsPerSec := float64(totalOps) / elapsed.Seconds()
	fmt.Printf("Total throughput: %.2f ops/s\n", opsPerSec)
}
