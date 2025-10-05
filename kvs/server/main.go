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
	Pending TxStatus = 0
	Committed
	Aborted
)

type TxInfo struct {
	sync.Mutex
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
	txInfo := &TxInfo{
		Txid:     txid,
		Status:   Pending,
		ReadSet:  make(map[string]struct{}),
		WriteSet: make(map[string]string),
	}
	val, _ := kv.transactions.LoadOrStore(txid, txInfo)
	return val.(*TxInfo)
}

func (kv *KVService) getKeyLock(key string) *KeyLock {
	keyLock := &KeyLock{
		readers: make(map[kvs.Txid]struct{}),
		writer:  0,
	}
	val, _ := kv.keyLocks.LoadOrStore(key, keyLock)
	return val.(*KeyLock)
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	atomic.AddUint64(&kv.stats.gets, 1)

	// Anti simu-access locks
	txInfo := kv.getTxInfo(request.Txid)
	keyLock := kv.getKeyLock(request.Key)

	txInfo.Lock()
	defer txInfo.Unlock()
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

	// Anti simu-access locks
	txInfo := kv.getTxInfo(request.Txid)
	keyLock := kv.getKeyLock(request.Key)

	txInfo.Lock()
	defer txInfo.Unlock()
	keyLock.Lock()
	defer keyLock.Unlock()

	// check for write-write and write-read conflicts.
	if (keyLock.writer != 0 && keyLock.writer != request.Txid) ||
		(len(keyLock.readers) > 0 && !isOnlyReader(keyLock.readers, request.Txid)) {
		response.LockFailed = true
		return nil
	}

	// Grant exclusive lock
	keyLock.writer = request.Txid
	txInfo.WriteSet[request.Key] = request.Value

	return nil
}

func isOnlyReader(readers map[kvs.Txid]struct{}, txid kvs.Txid) bool {
	if len(readers) == 1 {
		if _, ok := readers[txid]; ok {
			return true
		}
	}
	return false
}

func (kv *KVService) Commit(request *kvs.CommitRequest, response *kvs.CommitResponse) error {
	// Anti simu-access locks
	txInfo := kv.getTxInfo(request.Txid)
	txInfo.Lock()
	defer txInfo.Unlock()

	if txInfo.Status != Pending {
		response.Success = (txInfo.Status == Committed)
		return nil
	}

	// Writes
	for key, value := range txInfo.WriteSet {
		kv.mp.Store(key, value)
	}

	// Release all locks
	allKeys := make(map[string]struct{})
	for key := range txInfo.WriteSet {
		allKeys[key] = struct{}{}
	}
	for key := range txInfo.ReadSet {
		allKeys[key] = struct{}{}
	}

	for key := range allKeys {
		keyLock := kv.getKeyLock(key)
		keyLock.Lock()
		if _, isWrite := txInfo.WriteSet[key]; isWrite {
			if keyLock.writer == request.Txid {
				keyLock.writer = 0
			}
		}
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
	// Anti simu-access locks
	txInfo := kv.getTxInfo(request.Txid)
	txInfo.Lock()
	defer txInfo.Unlock()

	if txInfo.Status != Pending {
		response.Success = true // Idempotent abort
		return nil
	}

	// Release all locks held by the transaction
	allKeys := make(map[string]struct{})
	for key := range txInfo.WriteSet {
		allKeys[key] = struct{}{}
	}
	for key := range txInfo.ReadSet {
		allKeys[key] = struct{}{}
	}

	for key := range allKeys {
		keyLock := kv.getKeyLock(key)
		keyLock.Lock()
		if _, isWrite := txInfo.WriteSet[key]; isWrite {
			if keyLock.writer == request.Txid {
				keyLock.writer = 0
			}
		}
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
