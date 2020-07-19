package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/aarondwi/antri/priorityqueue"
	"github.com/aarondwi/antri/util"
	"github.com/valyala/fasthttp"
)

var newlineByte = byte('\n')
var newlineByteSlice = []byte("\n")
var letterBytes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randStringBytes(n int) []byte {
	b := make([]byte, n)
	l := len(letterBytes)
	for i := range b {
		b[i] = letterBytes[rand.Intn(l)]
	}
	return b
}

type AntriServer struct {
	// internal queue + stats
	mutex           *sync.Mutex
	notEmpty        *sync.Cond
	notFull         *sync.Cond
	pq              *priorityqueue.Pq
	maxsize         int
	unfinishedTasks int // tracking

	// inflight records + stats
	// not using sync.Map or equivalent
	// cause we also want to update the stats
	inflightMutex   *sync.Mutex
	inflightRecords map[string]*priorityqueue.PqItem
	inflightTasks   int

	// durability option
	added *util.MutexedFile
	taken *util.MutexedFile
}

func NewAntriServer(maxsize int) (*AntriServer, error) {
	if maxsize <= 0 {
		return nil, fmt.Errorf("maxsize should be positive, received %d", maxsize)
	}

	// addTask + retrieveTask path
	mutex := sync.Mutex{}
	notEmpty := sync.NewCond(&mutex)
	notFull := sync.NewCond(&mutex)

	addedMutex := sync.Mutex{}
	addedFile, err := os.OpenFile("added.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	// inflight path
	inflightMutex := sync.Mutex{}
	inflightRecords := make(map[string]*priorityqueue.PqItem)

	// commitTask path
	takenMutex := sync.Mutex{}
	takenFile, err := os.OpenFile("taken.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	return &AntriServer{
		mutex:           &mutex,
		notEmpty:        notEmpty,
		notFull:         notFull,
		unfinishedTasks: 0,
		pq:              priorityqueue.NewPq(maxsize),
		maxsize:         maxsize,
		inflightMutex:   &inflightMutex,
		inflightRecords: inflightRecords,
		added: &util.MutexedFile{
			M: &addedMutex,
			F: addedFile},
		taken: &util.MutexedFile{
			M: &takenMutex,
			F: takenFile},
	}, nil
}

// AddTask save the task message to wal
// and add it to in-memory PriorityQueue
// Available via POST method, at /add
func (as *AntriServer) AddTask(ctx *fasthttp.RequestCtx) {
	if string(ctx.Method()) != "POST" {
		ctx.SetStatusCode(400)
		fmt.Fprint(ctx, "Only Support POST Method.")
		return
	}

	// by default (for now), we gonna craete the key using 16-byte base62
	taskKey := randStringBytes(16)
	taskKeyStr := string(taskKey)

	// separate commit point
	// dont wanna block read because of fsync
	// after this point, it is considered committed
	// for now, we use text files for wal
	as.added.M.Lock()
	as.added.F.Write(taskKey)
	as.added.F.Write(newlineByteSlice)
	as.added.F.Write(append(ctx.FormValue("value"), newlineByte))
	as.added.F.Sync()
	as.added.M.Unlock()

	// lock/unlock manually
	// we dont want unlock to wait for fmt
	as.mutex.Lock()
	for as.unfinishedTasks == as.maxsize {
		as.notFull.Wait()
	}
	as.pq.Insert(&priorityqueue.PqItem{
		Key:         taskKeyStr,
		Value:       string(ctx.FormValue("value")),
		ScheduledAt: time.Now().Unix() + int64(ctx.PostArgs().GetUintOrZero("secondsfromnow")),
		Retries:     0})
	as.unfinishedTasks++
	as.notEmpty.Signal()
	as.mutex.Unlock()

	fmt.Fprintf(ctx, taskKeyStr)
}

// RetrieveTask takes a task from in-memory priorityqueue
// and move it to in-memory map
// Available via GET method, at /retrieve
func (as *AntriServer) RetrieveTask(ctx *fasthttp.RequestCtx) {
	if string(ctx.Method()) != "GET" {
		ctx.SetStatusCode(400)
		fmt.Fprint(ctx, "Only Support GET Method.")
		return
	}

	var res *priorityqueue.PqItem
	var placeholder *priorityqueue.PqItem
	var timediff int64

	// lock/unlock manually
	// we dont want unlock to wait for fmt
	as.mutex.Lock()
	for {
		for as.unfinishedTasks == 0 {
			as.notEmpty.Wait()
		}
		// won't be nil, as we wait for unfinishedTasks to be > 0
		// so no need to check
		placeholder = as.pq.Peek()
		timediff = placeholder.ScheduledAt - int64(time.Now().Unix())
		if timediff > 0 {
			as.mutex.Unlock()
			// can't sleep for timediff
			// cause some later message may be scheduled for earlier time
			time.Sleep(10 * time.Millisecond)
			as.mutex.Lock()
			continue
		}
		res = as.pq.Pop()
		as.unfinishedTasks--
		as.notFull.Signal()
		as.mutex.Unlock()
		break
	}

	// only hold in-memory
	// the at-least-once guarantee is via log
	as.inflightMutex.Lock()
	as.inflightRecords[res.Key] = res
	as.inflightTasks++
	as.inflightMutex.Unlock()

	// this should NOT error
	// because we totally manage this ourselves
	byteArray, _ := json.Marshal(res)
	fmt.Fprintf(ctx, string(byteArray))
}

// CommitTask checks if the given key is currently inflight
// if found, it removes the key from the inflightRecords
// if not found, returns error
func (as *AntriServer) CommitTask(ctx *fasthttp.RequestCtx) {
	if string(ctx.Method()) != "POST" {
		ctx.SetStatusCode(400)
		fmt.Fprint(ctx, "Only Support POST Method.")
		return
	}

	// no need to check it here
	taskKey := string(ctx.FormValue("key"))
	as.inflightMutex.Lock()
	_, ok := as.inflightRecords[taskKey]
	if ok {
		delete(as.inflightRecords, taskKey)
	}
	as.inflightMutex.Unlock()

	// no need to do this inside the lock
	if !ok {
		ctx.SetStatusCode(404)
		fmt.Fprint(ctx, "Task Key not found")
		return
	}

	// meaning found, commit to wal
	as.taken.M.Lock()
	as.taken.F.Write(ctx.FormValue("key"))
	as.taken.F.Write(newlineByteSlice)
	as.taken.F.Sync()
	as.taken.M.Unlock()

	fmt.Fprint(ctx, "OK")
}

// RejectTask checks if the given key is currently inflight
// if found, it removes the key from the inflightRecords, and put it back to pq
// if not found, returns error
func (as *AntriServer) RejectTask(ctx *fasthttp.RequestCtx) {
	if string(ctx.Method()) != "POST" {
		ctx.SetStatusCode(400)
		fmt.Fprint(ctx, "Only Support POST Method.")
		return
	}

	// no need to check it here
	taskKey := string(ctx.FormValue("key"))
	as.inflightMutex.Lock()
	val, ok := as.inflightRecords[taskKey]
	if ok {
		delete(as.inflightRecords, taskKey)
	}
	as.inflightMutex.Unlock()

	// no need to do this inside the lock
	if !ok {
		ctx.SetStatusCode(404)
		fmt.Fprint(ctx, "Task Key not found")
		return
	}

	// for now, add 10s for retries delay
	val.Retries++
	val.ScheduledAt = time.Now().Unix() + 10
	as.mutex.Lock()
	for as.unfinishedTasks == as.maxsize {
		as.notFull.Wait()
	}
	as.pq.Insert(val)
	as.unfinishedTasks++
	as.notEmpty.Signal()
	as.mutex.Unlock()

	fmt.Fprint(ctx, "OK")
}
