package utils

import "strings"

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

// SyncData đồng bộ dữ liệu giữa các nút (cập nhật từ các bản ghi log).
func (kv *KVStore) SyncData(entries []string) {
	for _, entry := range entries {
		// Giả sử mỗi entry là một cặp key-value dưới dạng "key=value"
		parts := strings.Split(entry, "=")
		if len(parts) == 2 {
			kv.Set(parts[0], parts[1])
		}
	}
}