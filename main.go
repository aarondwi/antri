package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"google.golang.org/grpc"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	as, _ := New(30, 300)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		<-sigs
		log.Println("Shutting Down...")
		as.Close()
		log.Println("Done")
		wg.Done()
	}()

	address := "127.0.0.1:15026"
	lis, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}
	gs := grpc.NewServer()
	go as.Run(gs, lis)

	wg.Wait()
}
