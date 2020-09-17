package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/valyala/fasthttp"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	as, _ := NewAntriServer(1_000_000, 30, 300)
	server := fasthttp.Server{
		Handler:     NewAntriServerRouter(as).Handler,
		Concurrency: 256,
	}

	go func() {
		<-sigs
		log.Println("Shutting Down")
		as.Close()
		log.Println("Done")
		os.Exit(0)
	}()

	log.Fatal(server.ListenAndServe("0.0.0.0:8080"))
}
