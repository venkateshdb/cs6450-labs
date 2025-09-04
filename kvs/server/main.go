package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
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

// LockFreeMap uses sync.Map for lock-free concurrent access
type LockFreeMap struct {
	data sync.Map
}

func NewLockFreeMap() *LockFreeMap {
	return &LockFreeMap{}
}

func (lfm *LockFreeMap) Get(key string) (string, bool) {
	if value, found := lfm.data.Load(key); found {
		return value.(string), true
	}
	return "", false
}

func (lfm *LockFreeMap) Put(key, value string) {
	lfm.data.Store(key, value)
}

func (lfm *LockFreeMap) BatchGet(keys []string) map[string]string {
	result := make(map[string]string)

	for _, key := range keys {
		if value, found := lfm.data.Load(key); found {
			result[key] = value.(string)
		}
	}

	return result
}

func (lfm *LockFreeMap) BatchPut(operations []kvs.PutRequest) {
	for _, op := range operations {
		lfm.data.Store(op.Key, op.Value)
	}
}

type KVService struct {
	mp         *LockFreeMap
	stats      Stats
	prevStats  Stats
	lastPrint  time.Time
	statsMutex sync.RWMutex
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.mp = NewLockFreeMap()
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.statsMutex.Lock()
	kv.stats.gets++
	kv.statsMutex.Unlock()

	if value, found := kv.mp.Get(request.Key); found {
		response.Value = value
	}

	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.statsMutex.Lock()
	kv.stats.puts++
	kv.statsMutex.Unlock()

	kv.mp.Put(request.Key, request.Value)

	return nil
}

func (kv *KVService) BatchGet(request *kvs.BatchGetRequest, response *kvs.BatchGetResponse) error {
	kv.statsMutex.Lock()
	kv.stats.gets += uint64(len(request.Keys))
	kv.statsMutex.Unlock()

	response.Values = kv.mp.BatchGet(request.Keys)
	return nil
}

func (kv *KVService) BatchPut(request *kvs.BatchPutRequest, response *kvs.BatchPutResponse) error {
	kv.statsMutex.Lock()
	kv.stats.puts += uint64(len(request.Operations))
	kv.statsMutex.Unlock()

	kv.mp.BatchPut(request.Operations)
	return nil
}

func (kv *KVService) printStats() {
	kv.statsMutex.RLock()
	stats := kv.stats
	prevStats := kv.prevStats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.statsMutex.RUnlock()

	kv.statsMutex.Lock()
	kv.prevStats = stats
	kv.lastPrint = now
	kv.statsMutex.Unlock()

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

	// Optimize HTTP server for high concurrency
	server := &http.Server{
		Handler:      nil, // Use default mux
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	fmt.Printf("Starting KVS server on :%s\n", *port)

	go func() {
		for {
			kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	server.Serve(l)
}
