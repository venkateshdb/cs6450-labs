package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand/v2"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

var clientIDCounter atomic.Uint64

type Client struct {
	rpcClient    *rpc.Client
	clientID     kvs.Txid
	activeTx     kvs.Txid
	participants map[string]*rpc.Client // Map of server address to rpc.Client
	writeSet     map[string]string
	gen          *kvs.Xorshift64
	serverAddr   string // The address of this specific server
}

func Dial(addr string, clientID kvs.Txid) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{
		rpcClient:    rpcClient,
		clientID:     clientID,
		participants: make(map[string]*rpc.Client),
		writeSet:     make(map[string]string),
		gen:          kvs.NewXorshift64(rand.Uint64()),
		serverAddr:   addr,
	}
}

func (client *Client) Begin() {
	client.activeTx = kvs.Txid(client.gen.Uint64()) // Generate a new transaction ID
	client.participants = make(map[string]*rpc.Client)
	client.writeSet = make(map[string]string)
}

func (client *Client) Commit() bool {
	if client.activeTx == 0 {
		log.Fatal("Commit() called without an active transaction")
	}

	success := true
	firstParticipant := true
	for addr, rpcClient := range client.participants {
		req := kvs.CommitRequest{
			Txid: client.activeTx,
			Lead: firstParticipant,
		}
		res := kvs.CommitResponse{}
		err := rpcClient.Call("KVService.Commit", &req, &res)
		if err != nil {
			log.Printf("Commit RPC to %s failed for Txid %d: %v", addr, client.activeTx, err)
			success = false
		}
		firstParticipant = false
	}
	client.activeTx = 0 // Clear active transaction
	return success
}

func (client *Client) Abort() bool {
	if client.activeTx == 0 {
		log.Fatal("Abort() called without an active transaction")
	}

	success := true
	firstParticipant := true
	for addr, rpcClient := range client.participants {
		req := kvs.AbortRequest{
			Txid: client.activeTx,
			Lead: firstParticipant,
		}
		res := kvs.AbortResponse{}
		err := rpcClient.Call("KVService.Abort", &req, &res)
		if err != nil {
			log.Printf("Abort RPC to %s failed for Txid %d: %v", addr, client.activeTx, err)
			success = false
		}
		firstParticipant = false
	}
	client.activeTx = 0 // Clear active transaction
	return success
}

func (client *Client) Get(key string) (string, bool) {
	if client.activeTx == 0 {
		log.Fatal("Get() called without an active transaction")
	}

	// Read-your-own-writes
	if val, found := client.writeSet[key]; found {
		return val, false // Not lock failed, as it's a local read
	}

	request := kvs.GetRequest{
		Txid: client.activeTx,
		Key:  key,
	}
	response := kvs.GetResponse{}
	err := client.rpcClient.Call("KVService.Get", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	if response.LockFailed {
		client.Abort()
		return "", true // Indicate implicit abort due to lock failure
	}

	// Add this server to participants if not already there
	if _, found := client.participants[client.serverAddr]; !found {
		client.participants[client.serverAddr] = client.rpcClient
	}

	return response.Value, false
}

func (client *Client) Put(key string, value string) bool {
	if client.activeTx == 0 {
		log.Fatal("Put() called without an active transaction")
	}

	request := kvs.PutRequest{
		Txid:  client.activeTx,
		Key:   key,
		Value: value,
	}

	response := kvs.PutResponse{}
	err := client.rpcClient.Call("KVService.Put", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	if response.LockFailed {
		client.Abort()
		return true // Indicate implicit abort due to lock failure
	}

	// Add this server to participants if not already there
	if _, found := client.participants[client.serverAddr]; !found {
		client.participants[client.serverAddr] = client.rpcClient
	}

	client.writeSet[key] = value // Add to write set for read-your-own-writes

	return false
}

func runClient(id int, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64, servers []string, wg *sync.WaitGroup) {
	defer wg.Done()

	// Each worker gets its own set of clients for each server
	// This ensures each client has a unique clientID for transaction generation
	workerClients := make([]*Client, len(servers))
	for i, host := range servers {
		workerClients[i] = Dial(host, kvs.Txid(clientIDCounter.Add(1)))
	}
	defer func() {
		for _, client := range workerClients {
			client.rpcClient.Close()
		}
	}()

	value := strings.Repeat("x", 128)
	opsCompleted := uint64(0)
	numOperationsPerTx := 3 // As per requirement

	for !done.Load() {
		var currentClient *Client
		var lockFailed bool
		var txOpsCount int

		// Transaction retry loop
		for {
			// Pick a random server to initiate the transaction
			serverIdx := rand.IntN(len(workerClients))
			currentClient = workerClients[serverIdx]
			currentClient.Begin()
			lockFailed = false
			txOpsCount = 0

			for i := 0; i < numOperationsPerTx; i++ {
				op := workload.Next()
				key := fmt.Sprintf("%d", op.Key)

				// For sharding, determine the correct client for the key
				targetServerIdx := getServerIdx(key, len(workerClients))
				targetClient := workerClients[targetServerIdx]

				// If the operation is for a different server, update currentClient
				// This is crucial for tracking participants correctly
				if targetClient != currentClient {
					// The current client's rpcClient is for the server it initially dialed.
					// We need to ensure the Get/Put calls are made through the correct rpcClient
					// for the shard that owns the key.
					// To simplify, we'll just use the targetClient's rpcClient for the operation.
					// The participant tracking will ensure all involved servers are contacted for 2PC.
					currentClient.participants[targetClient.serverAddr] = targetClient.rpcClient
				}

				if op.IsRead {
					_, failed := targetClient.Get(key)
					if failed {
						lockFailed = true
						break
					}
				} else {
					failed := targetClient.Put(key, value)
					if failed {
						lockFailed = true
						break
					}
				}
				txOpsCount++
			}

			if lockFailed {
				// Abort already called by Get/Put if lock failed
				// Retry the transaction
				continue
			}

			// If all operations succeeded, attempt to commit
			if currentClient.Commit() {
				opsCompleted += uint64(txOpsCount) // Count operations only on successful commit
				break                              // Transaction committed, break retry loop
			} else {
				// Commit failed (e.g., network error, though not handled in this assignment)
				// Abort and retry
				currentClient.Abort() // Ensure abort is called if commit failed
				continue
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
			runClient(clientId, &done, workload, resultsCh, hosts, &wg)
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
