package kv

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
)

type ValueBody struct {
	Value string `json:"value"`
}

type StorageEngine struct {
	Store map[string]string
}

func NewStorageEngine() *StorageEngine {
	return &StorageEngine{
		Store: make(map[string]string),
	}
}

func (se *StorageEngine) GetHandler(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	value, ok := se.Store[key]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp := ValueBody{Value: value}
		json.NewEncoder(w).Encode(resp)
	}
}

func (se *StorageEngine) PutHandler(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	var req ValueBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	se.Store[key] = req.Value // shallow copy of fat pointer of string
	w.WriteHeader(http.StatusOK)
}
