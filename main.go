package main

import (
	"log"
	"math/rand"
	"time"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	s, _ := NewAntriServer(50_000)

	r := router.New()
	r.POST("/add", s.AddTask)
	r.GET("/retrieve", s.RetrieveTask)
	r.POST("/commit/{taskKey}", s.CommitTask)
	r.POST("/reject/{taskKey}", s.RejectTask)

	server := fasthttp.Server{
		Handler:     r.Handler,
		Concurrency: 50,
	}
	log.Fatal(server.ListenAndServe("127.0.0.1:8080"))
}
