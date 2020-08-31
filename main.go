package main

import (
	"log"

	"github.com/valyala/fasthttp"
)

func main() {
	as, _ := NewAntriServer(1_000_000, 30, 120)

	server := fasthttp.Server{
		Handler:     NewAntriServerRouter(as).Handler,
		Concurrency: 256,
	}
	log.Fatal(server.ListenAndServe("0.0.0.0:8080"))
}
