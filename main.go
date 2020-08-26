package main

import (
	"log"

	"github.com/valyala/fasthttp"
)

func main() {
	as, _ := NewAntriServer(1_000_000, 10, 10)

	server := fasthttp.Server{
		Handler:     NewAntriServerRouter(as).Handler,
		Concurrency: 128,
	}
	log.Fatal(server.ListenAndServe("127.0.0.1:8080"))
}
