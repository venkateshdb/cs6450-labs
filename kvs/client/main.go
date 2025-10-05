package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand/v2"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

var clientIDCounter atomic.Uint64
var initDone atomic.Bool
var once sync.Once

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

func (client *Client) Get(target *Client, key string) (string, bool) {
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
	err := target.rpcClient.Call("KVService.Get", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	if response.LockFailed {
		client.Abort()
		return "", true
	}

	// Add this server to participants if not already there
	if _, found := client.participants[target.serverAddr]; !found {
		client.participants[target.serverAddr] = target.rpcClient
	}

	return response.Value, false
}

func (client *Client) Put(target *Client, key string, value string) bool {
	if client.activeTx == 0 {
		log.Fatal("Put() called without an active transaction")
	}

	request := kvs.PutRequest{
		Txid:  client.activeTx,
		Key:   key,
		Value: value,
	}

	response := kvs.PutResponse{}
	err := target.rpcClient.Call("KVService.Put", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	if response.LockFailed {
		client.Abort()
		return true // Indicate implicit abort due to lock failure
	}

	// Add this server to participants if not already there
	if _, found := client.participants[target.serverAddr]; !found {
		client.participants[target.serverAddr] = target.rpcClient
	}

	// Add to write set for read-your-own-writes
	client.writeSet[key] = value

	return false
}

func runClient(id int, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64, servers []string, wg *sync.WaitGroup) {
	defer wg.Done()

	rpc_clients := make([]*Client, len(servers))
	for i, host := range servers {
		rpc_clients[i] = Dial(host, kvs.Txid(clientIDCounter.Add(1)))
	}
	defer func() {
		for _, client := range rpc_clients {
			client.rpcClient.Close()
		}
	}()

	value := strings.Repeat("x", 128)
	opsCompleted := uint64(0)
	numOperationsPerTx := 3 // Could go crazy??? 20???

	for !done.Load() {
		var currentClient *Client
		var lockFailed bool
		var txOpsCount int

		// Retry loop
		for {
			serverIdx := rand.IntN(len(rpc_clients))
			currentClient = rpc_clients[serverIdx]
			currentClient.Begin()
			lockFailed = false
			txOpsCount = 0

			for i := 0; i < numOperationsPerTx; i++ {
				op := workload.Next()
				key := fmt.Sprintf("%d", op.Key)

				// Determine the correct client for the key
				targetServerIdx := getServerIdx(key, len(rpc_clients))
				targetServer := rpc_clients[targetServerIdx]

				if op.IsRead {
					_, lockFailed = currentClient.Get(targetServer, key)
				} else { // Write operation
					lockFailed = currentClient.Put(targetServer, key, value)
				}

				if lockFailed {
					break
				}
				txOpsCount++
			}

			if lockFailed {
				continue
			}

			// Abort/retry
			if currentClient.Commit() {
				opsCompleted += uint64(txOpsCount)
				break
			} else {
				currentClient.Abort()
				continue
			}
		}
	}

	fmt.Printf("Client %d finished operations.\n", id)

	resultsCh <- opsCompleted
}

func runXferClient(id int, done *atomic.Bool, resultsCh chan<- uint64, servers []string, wg *sync.WaitGroup) {
	defer wg.Done()

	rpc_clients := make([]*Client, len(servers))
	for i, host := range servers {
		rpc_clients[i] = Dial(host, kvs.Txid(clientIDCounter.Add(1)))
	}
	defer func() {
		for _, client := range rpc_clients {
			client.rpcClient.Close()
		}
	}()

	// Initialization of accounts by client 0
	once.Do(func() {
		client := rpc_clients[0] // Use the first client for initialization
		for {
			fmt.Println("Before begin()")
			client.Begin()
			for i := 0; i < 10; i++ {
				key := strconv.Itoa(i)
				targetServerIdx := getServerIdx(key, len(rpc_clients))
				// fmt.Println("targetServerIdx: ", targetServerIdx)
				// fmt.Println("i:", i)
				// If put or get return true, it means failed to aquire lock
				client.Put(rpc_clients[targetServerIdx], key, "1000")
			}
			if client.Commit() {
				break // Initialization successful
			}
		}
		// check for initialization
		balances := make([]int, 10)
		client.Begin()
		for i := 0; i < 10; i++ {
			key := strconv.Itoa(i)
			targetServerIdx := getServerIdx(key, len(rpc_clients))
			valStr, lockFailed := client.Get(rpc_clients[targetServerIdx], key)
			if lockFailed {
				break
			}
			bal, _ := strconv.Atoi(valStr)
			balances[i] = bal
		}
		if client.Commit() {
			fmt.Printf("After initialization, balances: %v\n", balances)
		}
		initDone.Store(true)
	})

	// Wait for initialization to complete
	for !initDone.Load() {
		time.Sleep(10 * time.Millisecond)
	}

	opsCompleted := uint64(0)
	clientId := rand.IntN(10) // Randomize id

	for !done.Load() {
		// Transfer transaction
		for {
			serverIdx := rand.IntN(len(rpc_clients))
			client := rpc_clients[serverIdx]
			client.Begin()

			src := clientId
			// always the next client
			dst := (clientId + 1) % 10
			// dst := rand.IntN(10)

			// Skip self transfer if necessary
			if src == dst {
				continue
			}

			srcKey := strconv.Itoa(src)
			dstKey := strconv.Itoa(dst)

			srcTarget := rpc_clients[getServerIdx(srcKey, len(rpc_clients))]
			dstTarget := rpc_clients[getServerIdx(dstKey, len(rpc_clients))]

			srcValStr, lockFailed := client.Get(srcTarget, srcKey)
			if lockFailed {
				continue
			}
			srcBal, _ := strconv.Atoi(srcValStr)

			if srcBal < 100 {
				client.Abort()
				break
			}

			dstValStr, lockFailed := client.Get(dstTarget, dstKey)
			if lockFailed {
				continue
			}
			dstBal, _ := strconv.Atoi(dstValStr)

			if client.Put(srcTarget, srcKey, strconv.Itoa(srcBal-100)) {
				continue
			}
			if client.Put(dstTarget, dstKey, strconv.Itoa(dstBal+100)) {
				continue
			}

			if client.Commit() {
				opsCompleted++
				break // Success
			}
		}

		// Minimal check interval is 6
		periodicity := uint64(rand.IntN(20) + 5)

		if opsCompleted > 0 && opsCompleted%periodicity == 0 {
			for {
				serverIdx := rand.IntN(len(rpc_clients))
				client := rpc_clients[serverIdx]
				client.Begin()

				total := 0
				balances := make([]int, 10)
				aborted := false

				// GET ALL ACCOUNTS
				for i := 0; i < 10; i++ {
					key := strconv.Itoa(i)
					target := rpc_clients[getServerIdx(key, len(rpc_clients))]
					valStr, lockFailed := client.Get(target, key)
					if lockFailed {
						aborted = true
						break
					}
					bal, _ := strconv.Atoi(valStr)
					balances[i] = bal
					total += bal
				}

				if aborted {
					continue
				}

				if client.Commit() {
					if total != 10000 {
						log.Fatalf("Assertion failed: total balance is %d, expected 10000. Balances: %v", total, balances)
					}
					fmt.Printf("Client %d verified total balance: %d. Balances: %v\n", id, total, balances)
					break // Success
				}
			}
		}
	}
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
	workload := flag.String("workload", "YCSB-B", "Workload type (YCSB-A, YCSB-B, YCSB-C, xfer)")
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
			if *workload == "xfer" {
				runXferClient(clientId, &done, resultsCh, hosts, &wg)
			} else {
				// fmt.Printf("Running clien t %d\n", clientId)
				workload := kvs.NewWorkload(*workload, *theta)
				runClient(clientId, &done, workload, resultsCh, hosts, &wg)
			}
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
	if *workload != "xfer" {
		fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
	}

}
