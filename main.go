package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/valyala/fasthttp"
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	s, _ := NewAntriServer(50_000)
	requestHandler := func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/add":
			s.AddTask(ctx)
			return
		case "/retrieve":
			s.RetrieveTask(ctx)
			return
		case "/commit":
			s.CommitTask(ctx)
			return
		case "/reject":
			s.RejectTask(ctx)
			return
		default:
			ctx.SetStatusCode(404)
			fmt.Fprint(ctx, "path not found")
		}
	}

	server := fasthttp.Server{
		Handler:     requestHandler,
		Concurrency: 50,
	}
	if err := server.ListenAndServe("127.0.0.1:8080"); err != nil {
		log.Fatalf("error in ListenAndServe: %s", err)
	}
}
