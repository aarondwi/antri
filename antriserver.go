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
var walFilenameFormat = "data/wal-%016d"

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
	// as.recovery()
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
	err := WriteNewMessageToLog(as.walFile.F, item)
	if err != nil {
		log.Fatal(err)
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

// only hold in-memory, the at-least-once guarantee is via log
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
// and move it to in-memory map.
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
	err := WriteCommitMessageToLog(as.walFile.F, key)
	if err != nil {
		log.Fatal(err)
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
	err := WriteRetriesOccurenceToLog(as.walFile.F, item)
	if err != nil {
		log.Fatal(err)
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

// recovers the current state of the MQ from the data directory
//
// also updates the counter to match the most recent one
func (as *AntriServer) recovery() {

}

// snapshotter create snapshot file of current state asynchronously,
// to speed-up the recovery process.
//
// the commit point is when we fsync the new checkpoint file.
// After that, all previous checkpoint and logs are considered obsolete
// and may be deleted
//
// the naming for the snapshot file includes the counter for wal files.
// for example, snapshot-0000000000000003 means the snapshot of wal files until counter 3
func (as *AntriServer) snapshotter() {
	ticker := time.NewTicker(time.Duration(as.checkpointDuration) * time.Second)
	for {
		select {
		case <-ticker.C:
			as.walFile.M.Lock()
			currentWalFilename := as.walFile.F.Name()
			currentWalCounter := as.walFile.C
			as.walFile.M.Unlock()

			files, err := ioutil.ReadDir("data/")
			if err != nil {
				log.Fatalf("Failed reading directory for snapshotting: %v", err)
			}

			log.Printf("Snapshotting started at %d", time.Now().Unix())

			// create new names first, so can be used to match and limit files read
			// also create placeholder files, which will hold the curretn snapshot data
			// we don't directly writes into snapshot file
			// cause it may crashes in the middle (not atomic for writing more than 1 page)
			// instead, we will atomically rename the file into snapshot after all done
			lastCheckpointedWalFilename := ""
			newSnapshotFilename := fmt.Sprintf("data/snapshot-%016d", currentWalCounter-1)
			placeholderFilename := fmt.Sprintf("data/placeholder-%016d", currentWalCounter-1)

			// for now, use a slice
			// this is NOT efficient, and gonna be changed later
			// because we also need to search them by key (for RETRY AND COMMIT)
			itemPlaceholder := []*ds.PqItem{}
			previousCheckpointFiles := sortedListOfFilesInDirMatchingARegexUntilALimit(files, "snapshot", newSnapshotFilename)
			if len(previousCheckpointFiles) > 0 {
				// we can open the old snapshot file
				// but only read the last one
				// all other files are just undeleted files
				lastSnapshotFiles, err := os.OpenFile(
					previousCheckpointFiles[len(previousCheckpointFiles)-1], os.O_RDONLY, fileMode)
				if err != nil {
					log.Fatalf("Failed reading last snapshot file, which is `%s` with error %v",
						previousCheckpointFiles[len(previousCheckpointFiles)-1], err)
				}
				defer lastSnapshotFiles.Close()

				// record lastCheckpointedFilename
				lastCheckpointedWalFilename = previousCheckpointFiles[len(previousCheckpointFiles)-1]

				r, err := ReadSnapshotContents(lastSnapshotFiles)
				if err != nil {
					log.Fatal(err)
				}
				itemPlaceholder = append(itemPlaceholder, r...)
			}

			walFilesToBeCompacted := sortedListOfFilesInDirMatchingARegexUntilALimit(files, "wal", currentWalFilename)
			sort.Strings(walFilesToBeCompacted)
			if len(walFilesToBeCompacted) == 0 || (lastCheckpointedWalFilename != "" &&
				strings.Compare( // need to check if no new files from name too
					fileSequenceNumberAsString(walFilesToBeCompacted[len(walFilesToBeCompacted)-1]),
					fileSequenceNumberAsString(lastCheckpointedWalFilename)) <= 0) {
				log.Printf("No new files, skipping ...")
				continue
			}

			snapshotPlaceholderFile, err := os.OpenFile(
				placeholderFilename,
				os.O_APPEND|os.O_CREATE|os.O_WRONLY, fileMode)
			if err != nil {
				log.Fatalf("Failed creating placeholder file for snapshotting, with error: %v", err)
			}
			defer snapshotPlaceholderFile.Close()

			for _, f := range walFilesToBeCompacted {
				// only process those that has not yet been included to the previous snapshot
				// to guarantee exactly once internally
				if lastCheckpointedWalFilename != "" && strings.Compare(
					fileSequenceNumberAsString(walFilesToBeCompacted[len(walFilesToBeCompacted)-1]),
					fileSequenceNumberAsString(lastCheckpointedWalFilename)) <= 0 {
					continue
				}

				walFile, err := os.OpenFile(f, os.O_RDONLY, fileMode)
				if err != nil {
					log.Fatalf("Failed reading wal file, which is `%s` with error %v", f, err)
				}
				defer walFile.Close()

				itemsFromThisWalfile, err := ReadLogMultiple(walFile)
				if err != nil {
					log.Fatal(err)
				}
				itemPlaceholder = append(itemPlaceholder, itemsFromThisWalfile...)
			}

			for _, item := range itemPlaceholder {
				WriteNewMessageToLog(snapshotPlaceholderFile, item)
			}
			err = snapshotPlaceholderFile.Sync()
			if err != nil {
				log.Fatalf("Failed fsync snapshot, with error: %v", err)
			}

			// after this rename, snapshotting is considered done (COMMITTED)
			err = os.Rename(placeholderFilename, newSnapshotFilename)
			if err != nil {
				log.Fatalf("Failed renaming into snapshot file, with error: %v", err)
			}
			log.Printf("Snapshotting finished at %d", time.Now().Unix())

			// delete all wals, snapshots, and placeholders until current snapshot
			// this process is safe, as it only removes until just before the just created checkpoint
			previousPlaceholderFiles := sortedListOfFilesInDirMatchingARegexUntilALimit(files, "placeholder", placeholderFilename)
			for _, f := range previousPlaceholderFiles {
				os.Remove(f)
			}
			for _, f := range previousCheckpointFiles {
				os.Remove(f)
			}
			for _, f := range walFilesToBeCompacted {
				os.Remove(f)
			}
		default:
		}
	}
}

// taskTimeoutWatchdog watches all the in-flight task
// to guarantee liveness of the data
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

			// either way, we need to put it individually
			// while batching may seems faster
			// but we also may surpassed the task number limit
			// unless it is allowed (at least not yet)
			for _, item := range itemArr {
				item.Retries++
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
