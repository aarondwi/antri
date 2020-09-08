package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aarondwi/antri/ds"
	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
)

var letterBytes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
var fileFlag = os.O_APPEND | os.O_CREATE | os.O_RDWR | os.O_SYNC
var fileMode = os.FileMode(0644)
var walFilenameFormat = "data/wal-%16d"

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
	walFile *wal

	// asynchronous snapshot
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

	mutex := sync.Mutex{}
	notEmpty := sync.NewCond(&mutex)
	notFull := sync.NewCond(&mutex)

	if _, err := os.Stat("data/"); os.IsNotExist(err) {
		os.Mkdir("data/", fileMode)
	}

	// access to file
	addedMutex := sync.Mutex{}
	addedFile, err := os.OpenFile(fmt.Sprintf(walFilenameFormat, 1), fileFlag, fileMode)
	if err != nil {
		log.Fatal(err)
	}

	// inflight path
	inflightMutex := sync.Mutex{}
	inflightRecords := ds.NewOrderedMap(int64(taskTimeout))

	as := &AntriServer{
		mutex:           &mutex,
		notEmpty:        notEmpty,
		notFull:         notFull,
		pq:              ds.NewPq(),
		maxsize:         maxsize,
		inflightMutex:   &inflightMutex,
		inflightRecords: inflightRecords,
		walFile: &wal{
			M: &addedMutex,
			F: addedFile,
			C: 1,
			N: 0},
		checkpointDuration: checkpointDuration,
	}
	go as.taskTimeoutWatchdog()
	// go as.snapshotter()
	return as, nil
}

func (as *AntriServer) rollWal() {
	as.walFile.N++
	if as.walFile.N%1000 == 0 {
		err := as.walFile.F.Close()
		if err != nil {
			log.Fatalf("Fail closing log file with error -> %v", err)
		}

		as.walFile.C++
		as.walFile.F, err = os.OpenFile(fmt.Sprintf(walFilenameFormat, as.walFile.C), fileFlag, fileMode)
		if err != nil {
			log.Fatalf("Opening new log file failed with error -> %v", err)
		}
		as.walFile.N = 0
	}
}

// separate commit point from adding to in-memory data structure
// dont wanna block read because of fsync
// after this function returns, the message is considered committed
func (as *AntriServer) writeNewMessageToWal(item *ds.PqItem) {
	as.walFile.M.Lock()
	ok := WriteNewMessageToLog(as.walFile.F, item)
	if !ok {
		panic("failed writing to log!")
	}
	as.rollWal()
	as.walFile.M.Unlock()
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

// get next message that has passed its scheduled time
func (as *AntriServer) getReadyToBeRetrievedMessage() *ds.PqItem {
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

	return res
}

// RetrieveTask takes a task from in-memory ds
// and move it to in-memory map
// Available via GET method, at /retrieve
func (as *AntriServer) RetrieveTask(ctx *fasthttp.RequestCtx) {
	res := as.getReadyToBeRetrievedMessage()
	as.addToInflightStorer(res.Key, res)

	// this should NOT error
	// because we totally manage this ourselves
	byteArray, _ := json.Marshal(res)
	ctx.Write(byteArray)
}

func (as *AntriServer) writeCommitKeyToWal(key []byte) {
	as.walFile.M.Lock()
	ok := WriteCommitMessageToLog(as.walFile.F, key)
	if !ok {
		panic("failed to commit key!")
	}
	as.rollWal()
	as.walFile.M.Unlock()
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
		fmt.Fprint(ctx, "Task Key not found\n")
		return
	}

	// meaning found
	// put first, so can be reused directly
	pqItemPool.Put(item)

	as.writeCommitKeyToWal([]byte(taskKey))

	ctx.WriteString("OK\n")
}

func (as *AntriServer) writeRetryOccurenceToWal(item *ds.PqItem) {
	as.walFile.M.Lock()
	ok := WriteRetriesOccurenceToLog(as.walFile.F, item)
	if !ok {
		panic("failed writing to log!")
	}
	as.rollWal()
	as.walFile.M.Unlock()
}

// RejectTask checks if the given key is currently inflight
// if found, it removes the key from the inflightRecords, and put it back to pq
// if not found, returns error
func (as *AntriServer) RejectTask(ctx *fasthttp.RequestCtx) {
	secondsfromnow := 5 // default to 5s
	if len(ctx.FormValue("secondsfromnow")) > 0 {
		secondsfromnow, err := strconv.Atoi(string(ctx.FormValue("secondsfromnow")))
		if err != nil {
			ctx.SetStatusCode(400)
			fmt.Fprint(ctx, "Failed reading the value of `secondsfromnow`. Did you pass a proper integer value?\n")
			return
		}
		if secondsfromnow < 0 {
			ctx.SetStatusCode(400)
			fmt.Fprint(ctx, "If provided, the value of `secondsfromnow` should be zero or positive\n")
			return
		}
	}

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
		fmt.Fprint(ctx, "Task Key not found\n")
		return
	}

	item.Retries++
	item.ScheduledAt = time.Now().Unix() + int64(secondsfromnow)
	as.writeRetryOccurenceToWal(item)
	as.addNewMessageToInMemoryDS(item)

	ctx.WriteString("OK\n")
}

// get all files matching the `word`, until the `limit`
func listItemInDirMatchingARegex(files []os.FileInfo, wordToMatch, limit string) []string {
	result := []string{}
	for _, f := range files {
		if strings.Contains(f.Name(), wordToMatch) &&
			strings.Compare(f.Name(), limit) == -1 {
			result = append(result, f.Name())
		}
	}

	sort.Strings(result)
	return result
}

// snapshotter create snapshot file of current state asynchronously
// to speed-up the recovery process
// the commit point is when we fsync the new checkpoint file
// after that, all previous checkpoint and logs are considered obsolete
// and may be deleted
func (as *AntriServer) snapshotter() {
	ticker := time.NewTicker(time.Duration(as.checkpointDuration) * time.Second)
	for {
		select {
		case <-ticker.C:
			// can safely read these 2 files without locks
			// as if it changed, it only goes forward, not backwards
			// currentAddedFilename := as.added.F.Name()
			// currentTakenFilename := as.taken.F.Name()

			files, err := ioutil.ReadDir("./")
			if err != nil {
				log.Fatalf("Failed reading directory for snapshot: %v", err)
			}

			log.Printf("Snapshotting started at %d", time.Now().Unix())
			// create new names first, so can be used to match and limit files read
			// also create placeholder files
			// which will hold the curretn snapshot data
			// we don't directly writes into snapshot file
			// cause it may crashes in the middle (not atomic for writing more than 1 page)
			// instead, we will atomically rename the file into snapshot after all done
			newSnapshotFilename := fmt.Sprintf("snapshot-%d", time.Now().Unix())
			placeholderFilename := fmt.Sprintf("placeholder-%d", time.Now().Unix())
			checkpointFiles := listItemInDirMatchingARegex(files, "snapshot", newSnapshotFilename)

			// read the snapshot, check where to start begin reading added and taken
			// generate intermediary state from the snapshot first
			// read the necessary taken files, craete internal map for fast matching
			// read the necessary added files, update the intermediary state as we read the files
			// re-put the state into new checkpount file
			// delete all added, taken, checkpoint until current snapshot

			// itemPlaceholder := []ds.PqItem{}
			if len(checkpointFiles) > 0 {
				// we can open the old snapshot
				// but only read the last one
				// all other files are just undeleted files
				// lastSnapshotFile := os.OpenFile(checkpointFiles[len(checkpointFiles)-1], fileFlag, fileMode)
			}

			err = os.Rename(placeholderFilename, newSnapshotFilename)
			if err != nil {
				log.Fatalf("Failed creating new snapshot files with error: %v", err)
			}
			log.Printf("Snapshotting finished at %d", time.Now().Unix())
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

			// either way, we need to put it individually
			// while batching may seems faster
			// but we also may surpassed the task number limit
			// unless it is allowed
			for _, item := range itemArr {
				as.writeRetryOccurenceToWal(item)
				as.addNewMessageToInMemoryDS(item)
			}
		}
	}
}

// Close all the underlying system
func (as *AntriServer) Close() {
	log.Println("Closing wal file...")
	as.walFile.F.Close()
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
