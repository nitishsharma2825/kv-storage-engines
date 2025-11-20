package main

import (
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/nitishsharma2825/go-kv/server/kv"
)

func main() {
	r := chi.NewRouter()
	engine := kv.NewStorageEngine()

	r.Use(middleware.Logger)

	r.Get("/{key}", engine.GetHandler)
	r.Put("/{key}", engine.PutHandler)

	log.Println("listening on :8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("server fail: %v", err)
	}
}
