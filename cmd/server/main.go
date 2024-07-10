package main

import (
	"log"
	"os"

	"github.com/michimani/proglog/internal/server"
)

func main() {
	addr, exists := os.LookupEnv("PROGLOG_ADDR")
	if !exists {
		addr = ":8080"
	}

	srv := server.NewHTTPServer(addr)
	log.Fatal(srv.ListenAndServe())
}
