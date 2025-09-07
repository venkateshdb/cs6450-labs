package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

var uniqueTimestamp atomic.Uint64

type Client struct {
	rpcClient *rpc.Client
}

func Dial(addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{rpcClient}
}

// can use Go to make these async
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

// func (client *Client) Get(key string) *rpc.Call {
// 	request := kvs.GetRequest{
// 		Key: key,
// 	}
// 	response := kvs.GetResponse{}
// 	client.rpcClient.Call("KVService.Get", &request, &response, nil)

// }

// func (client *Client) Put(key string, value string) *rpc.Call {
// 	request := kvs.PutRequest{
// 		Key:   key,
// 		Value: value,
// 	}

// 	response := kvs.PutResponse{}
// 	return client.rpcClient.Go("KVService.Put", &request, &response, nil)

// }

func (client *Client) BatchRequest(batchRequest []kvs.Request) *rpc.Call {
	request := kvs.BatchRequest{
		Requests: batchRequest,
	}
	response := kvs.BatchResponse{}
	return client.rpcClient.Go("KVService.BatchRequest", &request, &response, nil)
}

func (client *Client) BatchResponse(batchResponse []kvs.Response) *rpc.Call {
	request := kvs.BatchResponse{
		Responses: batchResponse,
	}
	response := kvs.BatchResponse{}
	return client.rpcClient.Go("KVService.BatchResponse", &request, &response, nil)
}

func runClient(id int, batch_size *int, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64, servers []string, wg *sync.WaitGroup) {
	defer wg.Done()

	clients := make([]*Client, len(servers))
	for i, host := range servers {
		clients[i] = Dial(host)
	}
	defer func() {
		// Clean up clients when worker finishes
		for _, client := range clients {
			client.rpcClient.Close()
		}
	}()

	value := strings.Repeat("x", 128)
	batchSize := *batch_size
	timestamp := uniqueTimestamp.Add(1)
	opsCompleted := uint64(0)

	for !done.Load() {
		ServerBatch := make(map[int][]kvs.Request)

		for j := 0; j < batchSize; j++ {
			op := workload.Next()
			key := fmt.Sprintf("%d", op.Key)
			// client := pool.GetClient(key)
			// fmt.Printf("Sending: key %s\n", key)
			server_idx := getServerIdx(key, len(clients))
			timestamp++

			request := kvs.Request{
				Key:       key,
				Value:     value,
				IsRead:    op.IsRead,
				Timestamp: timestamp,
			}
			ServerBatch[server_idx] = append(ServerBatch[server_idx], request)

		}

		var calls []*rpc.Call
		for serverIdx, requests := range ServerBatch {
			if len(requests) > 0 {
				// for Async Requests
				call := clients[serverIdx].BatchRequest(requests)
				calls = append(calls, call)
			}
		}

		for _, call := range calls {
			<-call.Done
			if call.Error != nil {
				log.Printf("RPC call failed: %v", call.Error)
			}
			if response, ok := call.Reply.(*kvs.BatchResponse); ok {
				opsCompleted += uint64(len(response.Responses))
			}
		}
	}

	fmt.Printf("Client %d finished operations.\n", id)

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

func getServerIdx(key string, N int) int {
	// strKey := strconv.Itoa(key)
	hashedKey := fnv.New64a()
	hashedKey.Write([]byte(key))
	return int(hashedKey.Sum64() % uint64(N))
}

func main() {
	hosts := HostList{}
	var wg sync.WaitGroup

	workers := flag.Int("workers", 64, "Number of goroutines to spin for each client, default is 64")
	batch_size := flag.Int("batch_size", 1024, "Batch size for each client, default is 1024")
	flag.Var(&hosts, "hosts", "Comma-separated list of host:ports to connect to")
	theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter")
	workload := flag.String("workload", "YCSB-B", "Workload type (YCSB-A, YCSB-B, YCSB-C)")
	secs := flag.Int("secs", 30, "Duration in seconds for each client to run")
	flag.Parse()

	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:8080")
	}

	fmt.Printf(
		"hosts %v\n"+
			"theta %.2f\n"+
			"workload %s\n"+
			"secs %d\n",
		hosts, *theta, *workload, *secs,
	)

	start := time.Now()

	// host := hosts[0]
	// clientId := 0
	totalWorkers := len(hosts) * *workers

	done := atomic.Bool{}
	resultsCh := make(chan uint64, totalWorkers)
	// fmt.Printf("Running on host: %s, clientId: %d\n", host, idx)
	for i := 0; i < totalWorkers; i++ {
		// clientId := i
		wg.Add(1)
		go func(clientId int) {
			// fmt.Printf("Running clien t %d\n", clientId)
			workload := kvs.NewWorkload(*workload, *theta)
			runClient(clientId, batch_size, &done, workload, resultsCh, hosts, &wg)
		}(i)
	}

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	wg.Wait()
	var totalOps uint64
	for i := 0; i < totalWorkers; i++ {
		totalOps += <-resultsCh
	}
	// opsCompleted := <-resultsCh

	elapsed := time.Since(start)

	opsPerSec := float64(totalOps) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)

}
