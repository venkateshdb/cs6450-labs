package kvs

type PutRequest struct {
	Key   string
	Value string
}

type PutResponse struct {
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value string
}

// Batch operations for better performance
type BatchGetRequest struct {
	Keys []string
}

type BatchGetResponse struct {
	Values map[string]string
}

type BatchPutRequest struct {
	Operations []PutRequest
}

type BatchPutResponse struct {
}
