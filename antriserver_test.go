package main

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/aarondwi/antri/priorityqueue"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/reuseport"
)

var (
	client   = &fasthttp.Client{}
	addr     = "127.0.0.1:3000"
	httpaddr = "http://127.0.0.1:3000"
	as, _    = NewAntriServer(10)
)

func TestAddRetrieveCommit(t *testing.T) {
	server := fasthttp.Server{
		Handler:     as.Router().Handler,
		Concurrency: 2,
	}

	ln, _ := reuseport.Listen("tcp4", addr)
	defer ln.Close()
	go func() { _ = server.Serve(ln) }()

	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(res)

	req.SetRequestURI(httpaddr + "/add")
	req.Header.SetMethod("POST")
	req.PostArgs().AddBytesKV([]byte("value"), []byte("helloworld"))

	client.Do(req, res)
	if res.StatusCode() != 200 {
		t.Logf(string(res.Body()))
		t.Fatalf("Expected Status 200 OK, got %d", res.StatusCode())
	}
	if len(res.Body()) != 16 {
		t.Fatalf("Expected OK, got %v", res.Body())
	}
	keyToCheck := string(res.Body())

	req.Reset()
	res.Reset()
	req.SetRequestURI(httpaddr + "/retrieve")
	client.Do(req, res)
	if res.StatusCode() != 200 {
		t.Fatalf("Expected Status 200 OK, got %d", res.StatusCode())
	}
	var jsonRes priorityqueue.PqItem
	err := json.Unmarshal(res.Body(), &jsonRes)
	if err != nil {
		t.Logf(string(res.Body()))
		t.Fatalf("Should be a json of PqItem, but it is not")
	}
	if jsonRes.Key != keyToCheck {
		t.Fatalf("Expected key %s, but got %s", keyToCheck, jsonRes.Key)
	}

	req.Reset()
	res.Reset()
	req.SetRequestURI(httpaddr + "/commit/" + keyToCheck)
	req.Header.SetMethod("POST")
	client.Do(req, res)

	if res.StatusCode() != 200 {
		t.Fatalf("Expected Status 200 OK, got %d", res.StatusCode())
	}
}

func TestAddRetrieveRejectThenReretrieve(t *testing.T) {
	server := fasthttp.Server{
		Handler:     as.Router().Handler,
		Concurrency: 2,
	}

	ln, _ := reuseport.Listen("tcp4", addr)
	defer ln.Close()
	go func() { _ = server.Serve(ln) }()

	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(res)

	req.SetRequestURI(httpaddr + "/add")
	req.Header.SetMethod("POST")
	req.PostArgs().AddBytesKV([]byte("value"), []byte("helloworld"))

	client.Do(req, res)
	if res.StatusCode() != 200 {
		t.Logf(string(res.Body()))
		t.Fatalf("Expected Status 200 OK, got %d", res.StatusCode())
	}
	if len(res.Body()) != 16 {
		t.Fatalf("Expected OK, got %v", res.Body())
	}
	keyToCheck := string(res.Body())

	req.Reset()
	res.Reset()
	req.SetRequestURI(httpaddr + "/retrieve")
	client.Do(req, res)
	if res.StatusCode() != 200 {
		t.Fatalf("Expected Status 200 OK, got %d", res.StatusCode())
	}
	var jsonRes priorityqueue.PqItem
	err := json.Unmarshal(res.Body(), &jsonRes)
	if err != nil {
		t.Logf(string(res.Body()))
		t.Fatalf("Should be a json of PqItem, but it is not")
	}
	if jsonRes.Key != keyToCheck {
		t.Fatalf("Expected key %s, but got %s", keyToCheck, jsonRes.Key)
	}

	req.Reset()
	res.Reset()
	req.SetRequestURI(httpaddr + "/reject/" + keyToCheck)
	req.Header.SetMethod("POST")
	client.Do(req, res)

	if res.StatusCode() != 200 {
		t.Fatalf("Expected Status 200 OK, got %d", res.StatusCode())
	}

	req.Reset()
	res.Reset()
	time.Sleep(6 * time.Second)
	req.SetRequestURI(httpaddr + "/retrieve")
	client.Do(req, res)
	if res.StatusCode() != 200 {
		t.Fatalf("Expected Status 200 OK, got %d", res.StatusCode())
	}
	err = json.Unmarshal(res.Body(), &jsonRes)
	if err != nil {
		t.Logf(string(res.Body()))
		t.Fatalf("Should be a json of PqItem, but it is not")
	}
	if jsonRes.Key != keyToCheck {
		t.Fatalf("Expected key %s, but got %s", keyToCheck, jsonRes.Key)
	}
}

func TestValueNotProvided(t *testing.T) {
	server := fasthttp.Server{
		Handler:     as.Router().Handler,
		Concurrency: 2,
	}

	ln, _ := reuseport.Listen("tcp4", addr)
	defer ln.Close()
	go func() { _ = server.Serve(ln) }()

	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()

	req.SetRequestURI(httpaddr + "/add")
	req.Header.SetMethod("POST")
	client.Do(req, res)
	if res.StatusCode() != 400 {
		t.Fatalf("Expected Status 400, got %d", res.StatusCode())
	}
}

func TestCommitNotFound(t *testing.T) {
	server := fasthttp.Server{
		Handler:     as.Router().Handler,
		Concurrency: 2,
	}

	ln, _ := reuseport.Listen("tcp4", addr)
	defer ln.Close()
	go func() { _ = server.Serve(ln) }()

	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()

	req.SetRequestURI(httpaddr + "/commit/notfound")
	req.Header.SetMethod("POST")
	client.Do(req, res)
	if res.StatusCode() != 404 {
		t.Fatalf("Expected Status 404, got %d", res.StatusCode())
	}
}

func TestRejectNotFound(t *testing.T) {
	server := fasthttp.Server{
		Handler:     as.Router().Handler,
		Concurrency: 2,
	}

	ln, _ := reuseport.Listen("tcp4", addr)
	defer ln.Close()
	go func() { _ = server.Serve(ln) }()

	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()

	req.SetRequestURI(httpaddr + "/reject/notfound")
	req.Header.SetMethod("POST")
	client.Do(req, res)
	if res.StatusCode() != 404 {
		t.Fatalf("Expected Status 404, got %d", res.StatusCode())
	}
}

var (
	as2, _    = NewAntriServer(1)
	addr2     = "127.0.0.1:3001"
	httpaddr2 = "http://127.0.0.1:3001"
)

func TestLockWaitFlow(t *testing.T) {
	server := fasthttp.Server{
		Handler:     as2.Router().Handler,
		Concurrency: 5,
	}

	ln, _ := reuseport.Listen("tcp4", addr2)
	defer ln.Close()
	go func() { _ = server.Serve(ln) }()

	var keyRetrieved string
	var wg sync.WaitGroup
	go func() {
		wg.Add(1)
		req := fasthttp.AcquireRequest()
		res := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(res)

		req.SetRequestURI(httpaddr2 + "/retrieve")
		client.Do(req, res)
		if res.StatusCode() != 200 {
			t.Fatalf("Expected Status 200 OK, got %d", res.StatusCode())
		}

		time.Sleep(1 * time.Second) // give time for 2 writes to come and get blocked
		var jsonRes priorityqueue.PqItem
		err := json.Unmarshal(res.Body(), &jsonRes)
		if err != nil {
			t.Logf(string(res.Body()))
			t.Fatalf("Should be a json of PqItem, but it is not")
		}
		keyRetrieved = jsonRes.Key
		wg.Done()
	}()

	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(res)

	req.SetRequestURI(httpaddr2 + "/add")
	req.Header.SetMethod("POST")
	req.PostArgs().AddBytesKV([]byte("value"), []byte("helloworld"))

	client.Do(req, res)
	if res.StatusCode() != 200 {
		t.Logf(string(res.Body()))
		t.Fatalf("Expected Status 200 OK, got %d", res.StatusCode())
	}
	if len(res.Body()) != 16 {
		t.Fatalf("Expected OK, got %v", res.Body())
	}
	keyToCheck := string(res.Body())
	client.Do(req, res)
	if res.StatusCode() != 200 {
		t.Logf(string(res.Body()))
		t.Fatalf("Expected Status 200 OK, got %d", res.StatusCode())
	}
	if len(res.Body()) != 16 {
		t.Fatalf("Expected OK, got %v", res.Body())
	}

	wg.Wait()
	if keyToCheck != keyRetrieved {
		t.Fatalf("Expected %s, got %s", keyToCheck, keyRetrieved)
	}
}
