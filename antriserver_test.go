package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aarondwi/antri/ds"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/reuseport"
)

var (
	client   = &fasthttp.Client{}
	addr     = "127.0.0.1:3000"
	httpaddr = "http://127.0.0.1:3000"
	as, _    = NewAntriServer(10, 1, 1)
)

func TestAntriServerParameter(t *testing.T) {
	_, err := NewAntriServer(0, 1, 1)
	if err == nil {
		log.Fatalf("maxsize should be positive, but it is not returning an error")
	}

	_, err = NewAntriServer(1, -1, 1)
	if err == nil {
		log.Fatalf("taskTimeout negative value should be error, but it is not")
	}

	_, err = NewAntriServer(1, 1, -1)
	if err == nil {
		log.Fatalf("checkpointDuration negative value should be error, but it is not")
	}
}

func TestAddRetrieveCommit(t *testing.T) {
	server := fasthttp.Server{
		Handler:     NewAntriServerRouter(as).Handler,
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
	var jsonRes ds.PqItem
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
	req.SetRequestURI(fmt.Sprintf("%s/%s/commit", httpaddr, keyToCheck))
	req.Header.SetMethod("POST")
	client.Do(req, res)

	if res.StatusCode() != 200 {
		t.Fatalf("Expected Status 200 OK, got %d", res.StatusCode())
	}
}

func TestAddRetrieveRejectThenReretrieve(t *testing.T) {
	server := fasthttp.Server{
		Handler:     NewAntriServerRouter(as).Handler,
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
	var jsonRes ds.PqItem
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
	req.SetRequestURI(fmt.Sprintf("%s/%s/reject", httpaddr, keyToCheck))
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

	req.Reset()
	res.Reset()
	req.SetRequestURI(fmt.Sprintf("%s/%s/commit", httpaddr, keyToCheck))
	req.Header.SetMethod("POST")
	client.Do(req, res)

	if res.StatusCode() != 200 {
		t.Fatalf("Expected Status 200 OK, got %d", res.StatusCode())
	}
}

func TestRejectError(t *testing.T) {
	server := fasthttp.Server{
		Handler:     NewAntriServerRouter(as).Handler,
		Concurrency: 2,
	}

	ln, _ := reuseport.Listen("tcp4", addr)
	defer ln.Close()
	go func() { _ = server.Serve(ln) }()

	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(res)

	req.SetRequestURI(httpaddr + "/nosuchkey/reject")
	req.Header.SetMethod("POST")
	req.PostArgs().AddBytesKV([]byte("secondsfromnow"), []byte("helloworld"))
	client.Do(req, res)
	if res.StatusCode() != 400 {
		t.Fatalf(
			"Expected Status 400 Bad Request because `secondsfromnow` is not an integer, but got %d",
			res.StatusCode())
	}

	req.Reset()
	res.Reset()

	req.SetRequestURI(httpaddr + "/nosuchkey/reject")
	req.Header.SetMethod("POST")
	req.PostArgs().AddBytesKV([]byte("secondsfromnow"), []byte(strconv.Itoa(-10)))
	client.Do(req, res)
	if res.StatusCode() != 400 {
		t.Fatalf(
			"Expected Status 400 Bad Request because `secondsfromnow` is negative, but got %d",
			res.StatusCode())
	}
}

func TestAddRetrieveTimeoutThenReretrieve(t *testing.T) {
	server := fasthttp.Server{
		Handler:     NewAntriServerRouter(as).Handler,
		Concurrency: 1,
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
	var jsonRes ds.PqItem
	err := json.Unmarshal(res.Body(), &jsonRes)
	if err != nil {
		t.Logf(string(res.Body()))
		t.Fatalf("Should be a json of PqItem, but it is not")
	}
	if jsonRes.Key != keyToCheck {
		t.Fatalf("Expected key %s, but got %s", keyToCheck, jsonRes.Key)
	}

	time.Sleep(2 * time.Second)

	req.Reset()
	res.Reset()
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

	req.Reset()
	res.Reset()
	req.SetRequestURI(fmt.Sprintf("%s/%s/commit", httpaddr, keyToCheck))
	req.Header.SetMethod("POST")
	client.Do(req, res)

	if res.StatusCode() != 200 {
		t.Fatalf("Expected Status 200 OK, got %d", res.StatusCode())
	}
}

func TestValueNotProvided(t *testing.T) {
	server := fasthttp.Server{
		Handler:     NewAntriServerRouter(as).Handler,
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
		Handler:     NewAntriServerRouter(as).Handler,
		Concurrency: 2,
	}

	ln, _ := reuseport.Listen("tcp4", addr)
	defer ln.Close()
	go func() { _ = server.Serve(ln) }()

	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()

	req.SetRequestURI(httpaddr + "/notfound/commit")
	req.Header.SetMethod("POST")
	client.Do(req, res)
	if res.StatusCode() != 404 {
		t.Fatalf("Expected Status 404, got %d", res.StatusCode())
	}
}

func TestRejectNotFound(t *testing.T) {
	server := fasthttp.Server{
		Handler:     NewAntriServerRouter(as).Handler,
		Concurrency: 2,
	}

	ln, _ := reuseport.Listen("tcp4", addr)
	defer ln.Close()
	go func() { _ = server.Serve(ln) }()

	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()

	req.SetRequestURI(httpaddr + "/notfound/reject")
	req.Header.SetMethod("POST")
	client.Do(req, res)
	if res.StatusCode() != 404 {
		t.Fatalf("Expected Status 404, got %d", res.StatusCode())
	}
}

var (
	as2, _    = NewAntriServer(1, 1, 10)
	addr2     = "127.0.0.1:3001"
	httpaddr2 = "http://127.0.0.1:3001"
)

func TestLockWaitFlow(t *testing.T) {
	server := fasthttp.Server{
		Handler:     NewAntriServerRouter(as2).Handler,
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
			keyRetrieved = fmt.Sprintf("`Expected Status 200 OK, got %d`", res.StatusCode())
			wg.Done()
			return
		}

		time.Sleep(1 * time.Second) // give time for 2 writes to come and get blocked
		var jsonRes ds.PqItem
		err := json.Unmarshal(res.Body(), &jsonRes)
		if err != nil {
			keyRetrieved = fmt.Sprint("`Should be a json of PqItem, but it is not`")
			wg.Done()
			return
		}
		keyRetrieved = jsonRes.Key
		wg.Done()
	}()

	time.Sleep(1 * time.Second)
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
