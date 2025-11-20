package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	engine "github.com/nitishsharma2825/go-kv/server/engine"
)

const DATA_DIR = "./data"

func main() {
	var next_sst_filenum int
	var manifest_file_handle *os.File

	_, err := os.Stat(DATA_DIR)
	if os.IsNotExist(err) {
		err = os.Mkdir(DATA_DIR, 0755)
		if err != nil {
			fmt.Println("Error creating data dir")
			panic(err)
		}

		f, err := os.Create(fmt.Sprintf("%s/%s", DATA_DIR, "manifest.txt"))
		if err != nil {
			fmt.Println("Error creating manifest file")
			panic(err)
		}
		f.WriteString("sst-1.json\n")
		next_sst_filenum = 1
		manifest_file_handle = f
	} else {
		f, err := os.OpenFile(fmt.Sprintf("%s/%s", DATA_DIR, "manifest.txt"), os.O_RDWR, 0644)
		if err != nil {
			fmt.Println("manifest file should be present, not found")
			panic(err)
		}

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			next_sst_filename := scanner.Text()
			_, _ = fmt.Sscanf(next_sst_filename, "sst-%d.json", &next_sst_filenum)
		}
		manifest_file_handle = f
	}

	r := chi.NewRouter()
	fmt.Printf("Current SSt file num: %d\n", next_sst_filenum)
	engine := engine.NewStorageEngine(next_sst_filenum, manifest_file_handle)
	defer manifest_file_handle.Close()

	r.Use(middleware.Logger)

	r.Get("/{key}", engine.GetHandler)
	r.Put("/{key}", engine.PutHandler)

	log.Println("listening on :8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("server fail: %v", err)
	}
}
