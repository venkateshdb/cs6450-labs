package kvs

type PutRequest struct {
	Key       string
	Value     string
	Timestamp uint64
}

type PutResponse struct {
}

type GetRequest struct {
	Key       string
	Timestamp uint64
}

type GetResponse struct {
	Value string
}

type Request struct {
	Key       string
	Value     string
	IsRead    bool
	Timestamp uint64
}

type Response struct {
	Value string
}

type BatchRequest struct {
	Requests []Request
}

type BatchResponse struct {
	Responses []Response
}