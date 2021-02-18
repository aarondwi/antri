package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	as, _ := New(1_000_000, 30, 300)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		<-sigs
		log.Println("Shutting Down...")
		as.Close()
		log.Println("Done")
		wg.Done()
	}()

	as.Run("127.0.0.1:15026")
	wg.Wait()
}
