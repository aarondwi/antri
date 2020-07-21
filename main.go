package main

import (
	"log"

	"github.com/valyala/fasthttp"
)

func main() {
	as, _ := NewAntriServer(50_000)

	server := fasthttp.Server{
		Handler:     as.Router().Handler,
		Concurrency: 50,
	}
	log.Fatal(server.ListenAndServe("127.0.0.1:8080"))
}
