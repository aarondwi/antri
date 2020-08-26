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

var fileFlag = os.O_APPEND | os.O_CREATE | os.O_WRONLY | os.O_SYNC
var fileMode = os.FileMode(0644)
var addedFileFormat = "added-%031d.log"
var takenFileFormat = "taken-%031d.log"

type mutexedFile struct {
	M *sync.Mutex
	F *os.File
	C int
	N int
}

func randStringBytes(n int) []byte {
	b := make([]byte, n)
	l := len(letterBytes)
	for i := range b {
		b[i] = letterBytes[rand.Intn(l)]
	}
	return b
}

var pqItemPool = &sync.Pool{
	New: func() interface{} {
		return &ds.PqItem{}
	},
}

// AntriServer is our main class implementation
//
// it sets up the internal priority queue (for task), orderedmap (for tracking),
// and logging system
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
	added *mutexedFile
	taken *mutexedFile

	// asynchronous snapshot
	checkpointFile     *os.File
	checkpointDuration int
}

// NewAntriServer initiate the AntriServer with all needed params.
// So far:
//
// 1. maxsize to prevent OOM error (you can count the number of bytes needed for your data)
//
// 2. taskTimeout is how long before a retrieved task by a worker considered failed, and should be resent
//
// 3. checkpointDuration is how often antri does its asynchronous snapshotting
func NewAntriServer(maxsize, taskTimeout, checkpointDuration int) (*AntriServer, error) {
	if maxsize <= 0 {
		return nil, fmt.Errorf("maxsize should be positive, received %d", maxsize)
	}
	if taskTimeout <= 0 {
		return nil, fmt.Errorf("taskTimeout should be positive, received %d", taskTimeout)
	}
	if checkpointDuration <= 0 {
		return nil, fmt.Errorf("checkpointDuration should be positive, received %d", checkpointDuration)
	}

	// addTask + retrieveTask path
	mutex := sync.Mutex{}
	notEmpty := sync.NewCond(&mutex)
	notFull := sync.NewCond(&mutex)

	addedMutex := sync.Mutex{}
	addedFile, err := os.OpenFile(
		fmt.Sprintf(addedFileFormat, 1), fileFlag, fileMode)
	if err != nil {
		log.Fatal(err)
	}

	// inflight path
	inflightMutex := sync.Mutex{}
	inflightRecords := ds.NewOrderedMap(int64(taskTimeout))

	// commitTask path
	takenMutex := sync.Mutex{}
	takenFile, err := os.OpenFile(
		fmt.Sprintf(takenFileFormat, 1), fileFlag, fileMode)
	if err != nil {
		log.Fatal(err)
	}

	checkpointFile, err := os.OpenFile("snapshot", fileFlag, fileMode)
	if err != nil {
		log.Fatal(err)
	}

	as := &AntriServer{
		mutex:           &mutex,
		notEmpty:        notEmpty,
		notFull:         notFull,
		pq:              ds.NewPq(),
		maxsize:         maxsize,
		inflightMutex:   &inflightMutex,
		inflightRecords: inflightRecords,
		added: &mutexedFile{
			M: &addedMutex,
			F: addedFile,
			C: 1,
			N: 0},
		taken: &mutexedFile{
			M: &takenMutex,
			F: takenFile,
			C: 1,
			N: 0},
		checkpointDuration: checkpointDuration,
		checkpointFile:     checkpointFile,
	}
	go as.taskTimeoutWatchdog()
	// go as.snapshotter()
	return as, nil
}

// separate commit point from adding to in-memory data structure
// dont wanna block read because of fsync
// after this function returns, the message is considered committed
func (as *AntriServer) writeNewMessageToWal(item *ds.PqItem) {
	as.added.M.Lock()
	ok := WritePqItemToLog(as.added.F, item)
	if !ok {
		panic("failed writing to log!")
	}
	as.added.N++
	if as.added.N%1000 == 0 {
		err := as.added.F.Close()
		if err != nil {
			log.Fatalf("Fail closing log file with error -> %v", err)
		}

		as.added.C++
		as.added.F, err = os.OpenFile(fmt.Sprintf(addedFileFormat, as.added.C), fileFlag, fileMode)
		if err != nil {
			log.Fatalf("Opening new log file failed with error -> %v", err)
		}
		as.added.N = 1
	}
	as.added.M.Unlock()
}
func (as *AntriServer) addNewMessageToInMemoryDS(item *ds.PqItem) {
	as.mutex.Lock()
	for as.pq.HeapSize() == as.maxsize {
		as.notFull.Wait()
	}
	as.pq.Insert(item)
	as.mutex.Unlock()
	as.notEmpty.Signal()
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

	item := pqItemPool.Get().(*ds.PqItem)
	item.Key = taskKeyStr
	item.Value = string(ctx.FormValue("value"))
	item.ScheduledAt = time.Now().Unix() + int64(ctx.PostArgs().GetUintOrZero("secondsfromnow"))
	item.Retries = 0

	as.writeNewMessageToWal(item)
	as.addNewMessageToInMemoryDS(item)

	ctx.WriteString(taskKeyStr)
}

// only hold in-memory
// the at-least-once guarantee is via log
func (as *AntriServer) addToInflightStorer(key string, item *ds.PqItem) {
	as.inflightMutex.Lock()
	as.inflightRecords.Insert(key, item)
	as.inflightMutex.Unlock()
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
			time.Sleep(100 * time.Millisecond)
			as.mutex.Lock()
			continue
		}
		res = as.pq.Pop()
		as.mutex.Unlock()
		as.notFull.Signal()
		break
	}

	as.addToInflightStorer(res.Key, res)

	// this should NOT error
	// because we totally manage this ourselves
	byteArray, _ := json.Marshal(res)
	ctx.Write(byteArray)
}

func (as *AntriServer) writeCommitKeyToWal(key []byte) {
	as.taken.M.Lock()
	ok := WriteCommittedKeyToLog(as.taken.F, key)
	if !ok {
		panic("failed to commit key!")
	}
	as.taken.N++
	if as.taken.N%1000 == 0 {
		err := as.taken.F.Close()
		if err != nil {
			log.Fatalf("Fail closing log file with error -> %v", err)
		}

		as.taken.C++
		as.taken.F, err = os.OpenFile(fmt.Sprintf(takenFileFormat, as.taken.C), fileFlag, fileMode)
		if err != nil {
			log.Fatalf("Opening new log file failed with error -> %v", err)
		}
		as.taken.N = 1
	}
	as.taken.M.Unlock()
}

// CommitTask checks if the given key is currently inflight
// if found, it removes the key from the inflightRecords
// if not found, returns error
func (as *AntriServer) CommitTask(ctx *fasthttp.RequestCtx) {
	taskKey := ctx.UserValue("taskKey").(string)
	as.inflightMutex.Lock()
	item, ok := as.inflightRecords.Get(taskKey)
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

	// meaning found
	// put first, so can be reused directly
	pqItemPool.Put(item)

	as.writeCommitKeyToWal([]byte(taskKey))

	ctx.WriteString("OK")
}

// RejectTask checks if the given key is currently inflight
// if found, it removes the key from the inflightRecords, and put it back to pq
// if not found, returns error
func (as *AntriServer) RejectTask(ctx *fasthttp.RequestCtx) {
	taskKey := ctx.UserValue("taskKey").(string)
	as.inflightMutex.Lock()
	item, ok := as.inflightRecords.Get(taskKey)
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
	item.Retries++
	item.ScheduledAt = time.Now().Unix() + 5
	as.addNewMessageToInMemoryDS(item)

	ctx.WriteString("OK")
}

// 2 internal asynchronous functions
// and both are run on other goroutines
func (as *AntriServer) snapshotter() {
	// based on
	// https://gist.github.com/wrfly/1a3239cd2f78ee68157ebc9b987f565b
	// pwd, err := os.Getwd()
	// if err != nil {
	// 	log.Fatalf("getwd err: %s", err)
	// }
	// procAttr := syscall.ProcAttr{
	// 	Dir:   pwd,
	// 	Env:   []string{},
	// 	Files: []uintptr{as.checkpointFile.Fd()},
	// 	Sys:   nil,
	// }
	ticker := time.NewTicker(time.Duration(as.checkpointDuration) * time.Second)
	for {
		select {
		case <-ticker.C:
		default:
		}
	}
}
func (as *AntriServer) taskTimeoutWatchdog() {
	// cause we always append to back
	// we can be sure that this array is sorted on expireOn
	//
	// does this function also need to track the retries to file?
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
