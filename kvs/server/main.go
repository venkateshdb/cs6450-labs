package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Stats struct {
	puts uint64
	gets uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.puts = s.puts - prev.puts
	r.gets = s.gets - prev.gets
	return r
}

type KVService struct {
	sync.RWMutex
	// mp        map[string]string
	mp        sync.Map
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.mp = sync.Map{}
	kvs.lastPrint = time.Now()
	return kvs
}

// func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
// 	// kv.Lock()
// 	// defer kv.Unlock()

// 	kv.stats.gets++

// 	if value, found := kv.mp.Load(request.Key); found {
// 		// fmt.Printf("Get key: %s, value : %s, found: %t\n", request.Key, value.(string), found)
// 		response.Value = value.(string)
// 	}

// 	return nil
// }

// func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
// 	// kv.Lock()
// 	// defer kv.Unlock()

// 	kv.stats.puts++
// 	// fmt.Printf("Put key: %s, found: %s\n", request.Key, request.Value)
// 	kv.mp.Store(request.Key, request.Value)

// 	return nil
// }

func (kv *KVService) BatchRequest(request *kvs.BatchRequest, response *kvs.BatchResponse) error {
	sort.Slice(request.Requests, func(i, j int) bool {
		return request.Requests[i].Timestamp < request.Requests[j].Timestamp
	})
	results := make([]kvs.Response, len(request.Requests))

	for i, req := range request.Requests {
		if req.IsRead {
			// kv.stats.gets++
			atomic.AddUint64(&kv.stats.gets, 1)
			if value, found := kv.mp.Load(req.Key); found {
				results[i] = kvs.Response{
					Value: value.(string),
				}
			} else {
				results[i] = kvs.Response{Value: ""}
			}
		} else {
			// kv.stats.puts++
			atomic.AddUint64(&kv.stats.puts, 1)
			kv.mp.Store(req.Key, req.Value)
			results[i] = kvs.Response{
				Value: "",
			}
		}
	}

	response.Responses = results
	return nil

}

func (kv *KVService) printStats() {
	kv.RLock()
	stats := kv.stats
	prevStats := kv.prevStats
	kv.prevStats = stats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	kv.RUnlock()

	diff := stats.Sub(&prevStats)
	deltaS := now.Sub(lastPrint).Seconds()

	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\n\n",
		float64(diff.gets)/deltaS,
		float64(diff.puts)/deltaS,
		float64(diff.gets+diff.puts)/deltaS)
}

func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	flag.Parse()

	kvs := NewKVService()
	rpc.Register(kvs)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Printf("Starting KVS server on :%s\n", *port)

	go func() {
		for {
			kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	http.Serve(l, nil)
}
