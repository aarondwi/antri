package main

import (
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
var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randStringRunes(n int) string {
	b := make([]rune, n)
	l := len(letterRunes)
	for i := range b {
		b[i] = letterRunes[rand.Intn(l)]
	}
	return string(b)
}

type AntriServer struct {
	// synchronization primitive
	mutex    *sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond

	// internal objects
	pq      *priorityqueue.Pq
	maxsize int

	// tracking
	unfinishedTasks int

	// durability option
	added *util.MutexedFile
	taken *util.MutexedFile
}

func NewAntriServer(maxsize int) (*AntriServer, error) {
	if maxsize <= 0 {
		return nil, fmt.Errorf("maxsize should be positive, received %d", maxsize)
	}
	mutex := sync.Mutex{}
	notEmpty := sync.NewCond(&mutex)
	notFull := sync.NewCond(&mutex)

	addedMutex := sync.Mutex{}
	addedFile, err := os.OpenFile("addedfile", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	takenMutex := sync.Mutex{}
	takenFile, err := os.OpenFile("getfile", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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

	// separate commit point
	// dont wanna block read because of fsync
	// after this point, it is considered committed
	// and this can be done
	// because the next part shouldn't fail
	as.added.M.Lock()
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
		Value:       ctx.FormValue("value"),
		ScheduledAt: time.Now().Unix() + int64(ctx.PostArgs().GetUintOrZero("secondsfromnow"))})
	as.unfinishedTasks++
	as.notEmpty.Signal()
	as.mutex.Unlock()

	fmt.Fprintf(ctx, "OK!")
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

	var res []byte
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
		res = as.pq.Pop().Value
		as.unfinishedTasks--
		as.notFull.Signal()
		as.mutex.Unlock()
		break
	}

	fmt.Fprintf(ctx, string(res))
}
