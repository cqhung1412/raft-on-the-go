package utils

import (
	pb "raft-on-the-go/proto"
	"strings"
)

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

// GetStore trả về bản sao của map store
func (kv *KVStore) GetStore() map[string]string {
	// Nếu cần đảm bảo thread-safe, bạn có thể sử dụng mutex ở đây (nếu KVStore có mutex)
	storeCopy := make(map[string]string)
	for k, v := range kv.store {
		storeCopy[k] = v
	}
	return storeCopy
}


// SyncData đồng bộ dữ liệu giữa các nút (cập nhật từ các bản ghi log).
func (kv *KVStore) SyncData(entries []*pb.LogEntry) {	
	for _, entry := range entries {
		// Giả sử mỗi entry là một cặp key-value dưới dạng "key=value"
		parts := strings.Split(entry.Command, "=")
		if len(parts) == 2 {
			kv.Set(parts[0], parts[1])
		}
	}
}