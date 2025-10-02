package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type TxStatus int

const (
	Pending TxStatus = iota
	Committed
	Aborted
)

type TxInfo struct {
	Txid     kvs.Txid
	Status   TxStatus
	ReadSet  map[string]struct{}
	WriteSet map[string]string
}

type KeyLock struct {
	sync.Mutex
	readers map[kvs.Txid]struct{}
	writer  kvs.Txid
}

type Stats struct {
	committedTxns uint64
	abortedTxns   uint64
	gets          uint64
	puts          uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.committedTxns = s.committedTxns - prev.committedTxns
	r.abortedTxns = s.abortedTxns - prev.abortedTxns
	r.gets = s.gets - prev.gets
	r.puts = s.puts - prev.puts
	return r
}

type KVService struct {
	sync.Mutex
	mp           sync.Map // Stores committed key-value pairs
	transactions sync.Map // map[kvs.Txid]*TxInfo
	keyLocks     sync.Map // map[string]*KeyLock
	stats        Stats
	prevStats    Stats
	lastPrint    time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.mp = sync.Map{}
	kvs.transactions = sync.Map{}
	kvs.keyLocks = sync.Map{}
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) getTxInfo(txid kvs.Txid) *TxInfo {
	if val, ok := kv.transactions.Load(txid); ok {
		return val.(*TxInfo)
	}
	txInfo := &TxInfo{
		Txid:     txid,
		Status:   Pending,
		ReadSet:  make(map[string]struct{}),
		WriteSet: make(map[string]string),
	}
	kv.transactions.Store(txid, txInfo)
	return txInfo
}

func (kv *KVService) getKeyLock(key string) *KeyLock {
	if val, ok := kv.keyLocks.Load(key); ok {
		return val.(*KeyLock)
	}
	keyLock := &KeyLock{
		readers: make(map[kvs.Txid]struct{}),
		writer:  0,
	}
	kv.keyLocks.Store(key, keyLock)
	return keyLock
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	atomic.AddUint64(&kv.stats.gets, 1)

	txInfo := kv.getTxInfo(request.Txid)
	keyLock := kv.getKeyLock(request.Key)

	keyLock.Lock()
	defer keyLock.Unlock()

	// Check for exclusive lock by another transaction
	if keyLock.writer != 0 && keyLock.writer != request.Txid {
		response.LockFailed = true
		return nil
	}

	// Grant shared lock
	keyLock.readers[request.Txid] = struct{}{}
	txInfo.ReadSet[request.Key] = struct{}{}

	if value, found := kv.mp.Load(request.Key); found {
		response.Value = value.(string)
	} else {
		response.Value = ""
	}

	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	atomic.AddUint64(&kv.stats.puts, 1)

	txInfo := kv.getTxInfo(request.Txid)
	keyLock := kv.getKeyLock(request.Key)

	keyLock.Lock()
	defer keyLock.Unlock()

	// Check for any lock by another transaction
	if keyLock.writer != 0 && keyLock.writer != request.Txid {
		response.LockFailed = true
		return nil
	}
	if len(keyLock.readers) > 0 {
		// If there are readers, check if any are from other transactions
		for readerTxid := range keyLock.readers {
			if readerTxid != request.Txid {
				response.LockFailed = true
				return nil
			}
		}
	}

	// Grant exclusive lock
	keyLock.writer = request.Txid
	txInfo.WriteSet[request.Key] = request.Value

	return nil
}

func (kv *KVService) Commit(request *kvs.CommitRequest, response *kvs.CommitResponse) error {
	txInfo := kv.getTxInfo(request.Txid)

	// Apply writes and release locks
	for key, value := range txInfo.WriteSet {
		kv.mp.Store(key, value)
		keyLock := kv.getKeyLock(key)
		keyLock.Lock()
		keyLock.writer = 0
		delete(keyLock.readers, request.Txid)
		keyLock.Unlock()
	}

	for key := range txInfo.ReadSet {
		keyLock := kv.getKeyLock(key)
		keyLock.Lock()
		delete(keyLock.readers, request.Txid)
		keyLock.Unlock()
	}

	txInfo.Status = Committed
	if request.Lead {
		atomic.AddUint64(&kv.stats.committedTxns, 1)
	}
	response.Success = true
	return nil
}

func (kv *KVService) Abort(request *kvs.AbortRequest, response *kvs.AbortResponse) error {
	txInfo := kv.getTxInfo(request.Txid)

	// Release all locks
	for key := range txInfo.WriteSet {
		keyLock := kv.getKeyLock(key)
		keyLock.Lock()
		keyLock.writer = 0
		delete(keyLock.readers, request.Txid)
		keyLock.Unlock()
	}

	for key := range txInfo.ReadSet {
		keyLock := kv.getKeyLock(key)
		keyLock.Lock()
		delete(keyLock.readers, request.Txid)
		keyLock.Unlock()
	}

	txInfo.Status = Aborted
	if request.Lead {
		atomic.AddUint64(&kv.stats.abortedTxns, 1)
	}
	response.Success = true
	return nil
}

func (kv *KVService) printStats() {
	kv.Lock()
	stats := kv.stats
	prevStats := kv.prevStats
	kv.prevStats = stats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	kv.Unlock()

	diff := stats.Sub(&prevStats)
	deltaS := now.Sub(lastPrint).Seconds()

	fmt.Printf("commit/s %0.2f\nabort/s %0.2f\n\n",
		float64(diff.committedTxns)/deltaS,
		float64(diff.abortedTxns)/deltaS)
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
