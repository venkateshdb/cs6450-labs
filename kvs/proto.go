package kvs

// Txid is a unique identifier for a transaction.
type Txid uint64

type PutRequest struct {
	Txid      Txid
	Key       string
	Value     string
	Timestamp uint64
}

type PutResponse struct {
	LockFailed bool
}

type GetRequest struct {
	Txid      Txid
	Key       string
	Timestamp uint64
}

type GetResponse struct {
	Value      string
	LockFailed bool
}

type CommitRequest struct {
	Txid Txid
	Lead bool // True if this is the leading participant for stats counting
}

type CommitResponse struct {
	Success bool
}

type AbortRequest struct {
	Txid Txid
	Lead bool // True if this is the leading participant for stats counting
}

type AbortResponse struct {
	Success bool
}
