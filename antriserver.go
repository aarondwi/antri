package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/aarondwi/antri/ds"
	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
)

var newlineByte = byte('\n')
var newlineByteSlice = []byte("\n")
var letterBytes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

type MutexedFile struct {
	M *sync.Mutex
	F *os.File
}

func randStringBytes(n int) []byte {
	b := make([]byte, n)
	l := len(letterBytes)
	for i := range b {
		b[i] = letterBytes[rand.Intn(l)]
	}
	return b
}

type AntriServer struct {
	// internal queue
	mutex    *sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	pq       *ds.Pq
	maxsize  int

	// not using sync.Map or equivalent
	// cause we also want to update the stats
	inflightMutex   *sync.Mutex
	inflightRecords *ds.OrderedMap
	taskTimeout     int

	// durability option
	added *MutexedFile
	taken *MutexedFile
}

func NewAntriServer(maxsize, taskTimeout int) (*AntriServer, error) {
	if maxsize <= 0 {
		return nil, fmt.Errorf("maxsize should be positive, received %d", maxsize)
	}
	if taskTimeout <= 0 {
		return nil, fmt.Errorf("taskTimeout should be positive, received %d", maxsize)
	}

	// addTask + retrieveTask path
	mutex := sync.Mutex{}
	notEmpty := sync.NewCond(&mutex)
	notFull := sync.NewCond(&mutex)

	addedMutex := sync.Mutex{}
	addedFile, err := os.OpenFile("added.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY|os.O_SYNC, 0644)
	if err != nil {
		log.Fatal(err)
	}

	// inflight path
	inflightMutex := sync.Mutex{}
	inflightRecords := ds.NewOrderedMap(int64(taskTimeout))

	// commitTask path
	takenMutex := sync.Mutex{}
	takenFile, err := os.OpenFile("taken.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY|os.O_SYNC, 0644)
	if err != nil {
		log.Fatal(err)
	}

	as := &AntriServer{
		mutex:           &mutex,
		notEmpty:        notEmpty,
		notFull:         notFull,
		pq:              ds.NewPq(maxsize),
		maxsize:         maxsize,
		inflightMutex:   &inflightMutex,
		inflightRecords: inflightRecords,
		added: &MutexedFile{
			M: &addedMutex,
			F: addedFile},
		taken: &MutexedFile{
			M: &takenMutex,
			F: takenFile},
	}
	go as.taskTimeoutWatchdog()
	return as, nil
}

// AddTask save the task message to wal
// and add it to in-memory ds
// Available via POST method, at /add
func (as *AntriServer) AddTask(ctx *fasthttp.RequestCtx) {
	// by default (for now), we gonna craete the key using 16-byte base62
	if len(ctx.FormValue("value")) <= 0 {
		ctx.SetStatusCode(400)
		ctx.WriteString("content of the task should be provided")
		return
	}

	taskKeyStr := string(randStringBytes(16))

	item := &ds.PqItem{
		Key:         taskKeyStr,
		Value:       string(ctx.FormValue("value")),
		ScheduledAt: time.Now().Unix() + int64(ctx.PostArgs().GetUintOrZero("secondsfromnow")),
		Retries:     0}

	// separate commit point
	// dont wanna block read because of fsync
	// after this point, it is considered committed
	// for now, we use text files for wal
	as.added.M.Lock()
	ok := WritePqItemToLog(as.added.F, item)
	if !ok {
		panic("failed writing to log!")
	}
	as.added.M.Unlock()

	// lock/unlock manually
	// we dont want unlock to wait for fmt
	as.mutex.Lock()
	for as.pq.HeapSize() == as.maxsize {
		as.notFull.Wait()
	}
	as.pq.Insert(item)
	as.notEmpty.Signal()
	as.mutex.Unlock()

	ctx.WriteString(taskKeyStr)
}

func (as *AntriServer) taskTimeoutWatchdog() {
	// cause we always append to back
	// we can be sure that this array is sorted on expireOn
	ticker := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			itemArr := make([]*ds.PqItem, 0)

			// remove from inflight
			currentTime := time.Now().Unix()
			as.inflightMutex.Lock()
			for as.inflightRecords.Length() > 0 &&
				as.inflightRecords.PeekExpire() < currentTime {

				// no need to check the err
				// undoubtedly gonna get one
				item, _ := as.inflightRecords.Pop()
				itemArr = append(itemArr, item)
			}
			as.inflightMutex.Unlock()

			as.mutex.Lock()
			for _, item := range itemArr {
				for as.pq.HeapSize() == as.maxsize {
					as.notFull.Wait()
				}
				as.pq.Insert(item)

				// have to be put inside
				// so workers can know that items may be taken
				as.notEmpty.Signal()
			}
			as.mutex.Unlock()
		}
	}
}

// RetrieveTask takes a task from in-memory ds
// and move it to in-memory map
// Available via GET method, at /retrieve
func (as *AntriServer) RetrieveTask(ctx *fasthttp.RequestCtx) {
	var res *ds.PqItem
	var placeholder *ds.PqItem
	var timediff int64

	// lock/unlock manually
	// we dont want unlock to wait for fmt
	as.mutex.Lock()
	for {
		for as.pq.HeapSize() == 0 {
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
		as.notFull.Signal()
		as.mutex.Unlock()
		break
	}

	// only hold in-memory
	// the at-least-once guarantee is via log
	as.inflightMutex.Lock()
	as.inflightRecords.Insert(res.Key, res)
	as.inflightMutex.Unlock()

	// this should NOT error
	// because we totally manage this ourselves
	byteArray, _ := json.Marshal(res)
	ctx.Write(byteArray)
}

// CommitTask checks if the given key is currently inflight
// if found, it removes the key from the inflightRecords
// if not found, returns error
func (as *AntriServer) CommitTask(ctx *fasthttp.RequestCtx) {
	taskKey := ctx.UserValue("taskKey").(string)
	as.inflightMutex.Lock()
	_, ok := as.inflightRecords.Get(taskKey)
	if ok {
		as.inflightRecords.Delete(taskKey)
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
	ok = WriteCommittedKeyToLog(as.taken.F, ctx.FormValue("key"))
	if !ok {
		panic("failed to commit key!")
	}
	as.taken.M.Unlock()

	ctx.WriteString("OK")
}

// RejectTask checks if the given key is currently inflight
// if found, it removes the key from the inflightRecords, and put it back to pq
// if not found, returns error
func (as *AntriServer) RejectTask(ctx *fasthttp.RequestCtx) {
	taskKey := ctx.UserValue("taskKey").(string)
	as.inflightMutex.Lock()
	val, ok := as.inflightRecords.Get(taskKey)
	if ok {
		as.inflightRecords.Delete(taskKey)
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
	val.ScheduledAt = time.Now().Unix() + 5
	as.mutex.Lock()
	for as.pq.HeapSize() == as.maxsize {
		as.notFull.Wait()
	}
	as.pq.Insert(val)
	as.notEmpty.Signal()
	as.mutex.Unlock()

	ctx.WriteString("OK")
}

// NewAntriServerRouter returns fasthttp/router that already set with AntriServer handler
// Also seed the rng
func NewAntriServerRouter(as *AntriServer) *router.Router {
	rand.Seed(time.Now().UTC().UnixNano())
	r := router.New()
	r.POST("/add", as.AddTask)
	r.GET("/retrieve", as.RetrieveTask)
	r.POST("/{taskKey}/commit", as.CommitTask)
	r.POST("/{taskKey}/reject", as.RejectTask)

	return r
}
