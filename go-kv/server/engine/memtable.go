package engine

import (
	"encoding/json"
	"os"
	"sort"
)

type LogEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type MemTable struct {
	store map[string]string
}

func newMemTable() *MemTable {
	return &MemTable{
		store: make(map[string]string),
	}
}

func (store *MemTable) size() int {
	return len(store.store)
}

func (store *MemTable) get(key string) (string, bool) {
	val, ok := store.store[key]
	return val, ok
}

func (store *MemTable) put(key string, value string) {
	store.store[key] = value
}

func (store *MemTable) persist(file *os.File) error {
	keys := make([]string, 0, len(store.store))
	for k := range store.store {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	encoder := json.NewEncoder(file)
	for _, k := range keys {
		item := LogEntry{Key: k, Value: store.store[k]}
		err := encoder.Encode(&item) // adds a new line
		if err != nil {
			return err
		}
	}

	if err := file.Sync(); err != nil {
		return err
	}

	// reset memtable
	store.store = make(map[string]string)
	return nil
}
