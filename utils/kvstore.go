package utils

type KVStore struct {
	store map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		store: make(map[string]string),
	}
}

func (kv *KVStore) Set(key, value string) {
	kv.store[key] = value
}

func (kv *KVStore) Get(key string) (string, bool) {
	val, exists := kv.store[key]
	return val, exists
}
