package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-chi/chi/v5"
)

type ValueBody struct {
	Value string `json:"value"`
}

type StorageEngine struct {
	store                *MemTable
	lock                 sync.Mutex
	next_sst_filenum     int
	manifest_file_handle *os.File
}

func NewStorageEngine(next_sst_filenum int, manifest_file_handle *os.File) *StorageEngine {
	return &StorageEngine{
		store:                newMemTable(),
		next_sst_filenum:     next_sst_filenum,
		manifest_file_handle: manifest_file_handle,
	}
}

func (se *StorageEngine) GetHandler(w http.ResponseWriter, r *http.Request) {
	se.lock.Lock()
	defer se.lock.Unlock()

	key := chi.URLParam(r, "key")

	key_found := true
	value, ok := se.store.get(key)
	if !ok {
		if value = se.getKeyFromSST(key); value == "" {
			key_found = false
		}
	}

	if key_found {
		fmt.Printf("value: %s\n", value)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp := ValueBody{Value: value}
		json.NewEncoder(w).Encode(resp)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (se *StorageEngine) PutHandler(w http.ResponseWriter, r *http.Request) {
	se.lock.Lock()
	defer se.lock.Unlock()

	key := chi.URLParam(r, "key")
	var req ValueBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	se.store.put(key, req.Value) // shallow copy of fat pointer of string
	w.WriteHeader(http.StatusOK)

	if se.store.size() >= 2000 {
		se.persistMemTable()
	}
}

func (se *StorageEngine) getKeyFromSST(key string) string {
	var sst_files []string

	data_dir := filepath.Dir(se.manifest_file_handle.Name())
	se.manifest_file_handle.Seek(0, io.SeekStart)
	scanner := bufio.NewScanner(se.manifest_file_handle)
	for scanner.Scan() {
		sst_files = append(sst_files, scanner.Text())
	}

	for i := len(sst_files) - 2; i >= 0; i-- {
		sst_file_path := filepath.Join(data_dir, sst_files[i])
		sst_file, err := os.Open(sst_file_path)
		if err != nil {
			fmt.Printf("Can't open file %s\n", sst_file_path)
		}

		file_reader := bufio.NewScanner(sst_file)
		for file_reader.Scan() {
			line := file_reader.Bytes()
			var entry LogEntry
			if err := json.Unmarshal(line, &entry); err != nil {
				fmt.Printf("failed decoding line in %s file", sst_file_path)
				panic(err)
			}

			if entry.Key == key {
				sst_file.Close()
				return entry.Value
			}
		}

		sst_file.Close()
	}
	return ""
}

func (se *StorageEngine) persistMemTable() {
	data_dir := filepath.Dir(se.manifest_file_handle.Name())
	sst_file_name := filepath.Join(data_dir, fmt.Sprintf("sst-%d.json", se.next_sst_filenum))

	f, err := os.Create(sst_file_name)
	if err != nil {
		fmt.Printf("Could not create sst file num: %d", se.next_sst_filenum)
		panic(err)
	}
	defer f.Close()

	if err := se.store.persist(f); err != nil {
		fmt.Printf("Persistence failed with err %v\n", err)
		panic(err)
	}

	// Add next sst file name at end of manifest
	se.manifest_file_handle.Seek(0, io.SeekEnd)
	fmt.Fprintf(se.manifest_file_handle, "sst-%d.json\n", se.next_sst_filenum+1)

	if err := se.manifest_file_handle.Sync(); err != nil {
		fmt.Println("Manifest file sync failed")
		panic(err)
	}

	se.next_sst_filenum += 1
}
