package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aarondwi/antri/ds"
	"github.com/aarondwi/antri/proto"
	"google.golang.org/grpc"
)

var letterBytes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
var l = len(letterBytes)
var fileFlag = os.O_APPEND | os.O_CREATE | os.O_RDWR
var fileMode = os.FileMode(0644)
var dataDir = "data/"
var filenameFormat = dataDir + "wal-%016d"

func randStringBytes(n int) []byte {
	b := make([]byte, n)
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
	pq       *ds.PriorityQueue

	// not using sync.Map or equivalent
	// cause we also want to update the stats
	inflightMutex   *sync.Mutex
	inflightRecords *ds.OrderedMap
	taskTimeout     int

	// durability option + asynchronous snapshot
	walFile            *wal
	checkpointDuration int

	// utility to stop background worker
	runningCtx context.Context
	cancelFunc context.CancelFunc

	// grpc's
	gs *grpc.Server
	proto.UnimplementedAntriServer
}

// New initiate the AntriServer with all needed params.
// So far:
//
// 1. taskTimeout is how long before a retrieved task by a worker considered failed, and should be resent
//
// 2. checkpointDuration is how often antri does its asynchronous snapshotting
func New(taskTimeout, checkpointDuration int) (*AntriServer, error) {
	if taskTimeout <= 0 {
		return nil, fmt.Errorf("taskTimeout should be positive, received %d", taskTimeout)
	}
	if checkpointDuration <= 0 {
		return nil, fmt.Errorf("checkpointDuration should be positive, received %d", checkpointDuration)
	}

	mutex := sync.Mutex{}
	notEmpty := sync.NewCond(&mutex)

	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		os.Mkdir(dataDir, fileMode)
	}

	// inflight path
	inflightMutex := sync.Mutex{}
	inflightRecords := ds.NewOrderedMap(int64(taskTimeout))

	runningCtx, cancelFunc := context.WithCancel(context.Background())

	as := &AntriServer{
		mutex:              &mutex,
		notEmpty:           notEmpty,
		pq:                 ds.NewPriorityQueue(),
		inflightMutex:      &inflightMutex,
		inflightRecords:    inflightRecords,
		checkpointDuration: checkpointDuration,
		runningCtx:         runningCtx,
		cancelFunc:         cancelFunc,
	}

	// start a recovery, to get all the data,
	// then compute the next counter to be used
	//
	// as recovery() returns the last known value,
	// we inc it by 1 so the file doesnt get corrupted
	// (basically start clean)
	startCounter := as.recovery() + 1

	// access to file
	walMutex := sync.Mutex{}
	walFile, err := os.OpenFile(fmt.Sprintf(filenameFormat, startCounter), fileFlag, fileMode)
	if err != nil {
		log.Fatal(err)
	}
	as.walFile = &wal{
		M: &walMutex,
		F: walFile,
		C: startCounter,
		N: 0,
	}

	go as.snapshotter()
	go as.taskTimeoutWatchdog()
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
		as.walFile.F, err = os.OpenFile(fmt.Sprintf(filenameFormat, as.walFile.C), fileFlag, fileMode)
		if err != nil {
			log.Fatalf("Opening new log file failed with error -> %v", err)
		}
		as.walFile.N = 0
	}
}

// separate commit point from adding to in-memory data structure
// dont wanna block read because of fsync
// after this function returns, the message is considered committed
func (as *AntriServer) writeNewMessageToWal(items []*ds.PqItem) {
	as.walFile.M.Lock()
	for _, item := range items {
		err := WriteNewMessageToLog(as.walFile.F, item)
		if err != nil {
			log.Fatal(err)
		}
	}
	err := as.walFile.F.Sync()
	if err != nil {
		log.Fatal(err)
	}
	as.rollWal()
	as.walFile.M.Unlock()
}

func (as *AntriServer) addNewMessageToInMemoryDS(items []*ds.PqItem) {
	as.mutex.Lock()
	for _, item := range items {
		as.pq.Insert(item)
	}
	as.mutex.Unlock()
	as.notEmpty.Signal()
}

var addTasksSuccess = &proto.AddTasksResponse{Result: true}

// ErrContentShouldNotBeEmpty is returned when
// at least one of proto.AddTasksRequest contents is nil
var ErrContentShouldNotBeEmpty = errors.New("Content of tasks should be provided")

// ErrRequestObjectIsNil is returned when
// the *main* request object is nil
var ErrRequestObjectIsNil = errors.New("Request object is nil")

// ErrRequestObjectArrayIsEmptyOrNil is returned when
// the *main* request object's arrray is nil
var ErrRequestObjectArrayIsEmptyOrNil = errors.New("Request object's array data is empty or nil")

// ErrTaskIsNil is returned when
// at least one of proto.AddTasksRequest.Tasks is nil
var ErrTaskIsNil = errors.New("Content of tasks should be provided")

// AddTasks save multiple tasks to wal
// and add it to in-memory ds
func (as *AntriServer) AddTasks(
	ctx context.Context,
	in *proto.AddTasksRequest) (*proto.AddTasksResponse, error) {

	if in == nil {
		return nil, ErrRequestObjectIsNil
	}
	if in.Tasks == nil || len(in.Tasks) == 0 {
		return nil, ErrRequestObjectArrayIsEmptyOrNil
	}
	for _, t := range in.Tasks {
		if t == nil {
			return nil, ErrTaskIsNil
		}
		if len(t.Content) == 0 {
			return nil, ErrContentShouldNotBeEmpty
		}
	}

	newTasks := make([]*ds.PqItem, 0, len(in.Tasks))
	for _, t := range in.Tasks {
		item := pqItemPool.Get().(*ds.PqItem)
		item.Key = string(randStringBytes(16))
		item.Value = t.Content
		item.ScheduledAt = time.Now().Unix() + int64(t.SecondsFromNow)
		item.Retries = 0
		newTasks = append(newTasks, item)
	}

	as.writeNewMessageToWal(newTasks)
	as.addNewMessageToInMemoryDS(newTasks)

	return addTasksSuccess, nil
}

// only hold in-memory, the at-least-once guarantee is via log
func (as *AntriServer) addToInflightStorer(items []*ds.PqItem) {
	as.inflightMutex.Lock()
	for _, item := range items {
		as.inflightRecords.Insert(string(item.Key), item)
	}
	as.inflightMutex.Unlock()
}

// get next message that has passed its scheduled time
func (as *AntriServer) getReadyToBeRetrievedMessage(N uint32) []*ds.PqItem {
	// lock/unlock manually
	// we dont want unlock to wait for fmt
	res := make([]*ds.PqItem, 0, N)
	as.mutex.Lock()
	for {
		for as.pq.HeapSize() == 0 {
			as.notEmpty.Wait()
		}

		// won't be nil, as we wait for unfinishedTasks to be > 0
		// so no need to check
		if timediff := as.pq.Peek().ScheduledAt - int64(time.Now().Unix()); timediff > 0 {
			as.mutex.Unlock()
			// can't sleep for time difference
			// cause some later message may be scheduled for earlier time
			time.Sleep(50 * time.Millisecond)
			as.mutex.Lock()
			continue
		}

		// only returns whichever is lower
		valueToGet := as.pq.HeapSize()
		if int(N) < valueToGet {
			valueToGet = int(N)
		}
		now := int64(time.Now().Unix())
		for i := 0; i < valueToGet; i++ {
			// do not want to get too many
			// as we may break this system's guarantee
			if as.pq.Peek().ScheduledAt-now > 0 {
				break
			}
			res = append(res, as.pq.Pop())
		}
		as.mutex.Unlock()
		break
	}

	return res
}

var getTasksEmpty = &proto.GetTasksResponse{Tasks: make([]*proto.RetrievedTask, 0)}

// GetTasks returns multiple (1..n) tasks
// from in-memory ds, and for the duration, put it to in-memory map
func (as *AntriServer) GetTasks(
	ctx context.Context,
	in *proto.GetTasksRequest) (*proto.GetTasksResponse, error) {

	if in == nil {
		return nil, ErrRequestObjectIsNil
	}
	if in.MaxN == 0 { // uint32 may return 0
		return getTasksEmpty, nil
	}

	res := as.getReadyToBeRetrievedMessage(in.MaxN)
	as.addToInflightStorer(res)
	tasks := make([]*proto.RetrievedTask, 0, len(res))
	for _, r := range res {
		tasks = append(tasks, &proto.RetrievedTask{
			Key:         r.Key,
			Content:     r.Value,
			RetryNumber: int32(r.Retries),
		})
	}
	return &proto.GetTasksResponse{Tasks: tasks}, nil
}

func (as *AntriServer) writeCommitKeyToWal(keys []string) {
	as.walFile.M.Lock()
	for _, key := range keys {
		err := WriteCommitMessageToLog(as.walFile.F, []byte(key))
		if err != nil {
			log.Fatal(err)
		}
	}
	err := as.walFile.F.Sync()
	if err != nil {
		log.Fatal(err)
	}
	as.rollWal()
	as.walFile.M.Unlock()
}

// ErrCommitKeyShouldNotBeEmpty is returned when
// at least one of proto.AddTasksRequest contents is nil
var ErrCommitKeyShouldNotBeEmpty = errors.New("Content of key should not eb empty")

// CommitTasks checks if the given keys are inflight.
// If found, it removes the key from the inflightRecords
//
// As it is batched, we don't want to fail all keys when only one fail.
// As tradeoff, this will not return error, but will happily continue,
// only for those keys that are found
func (as *AntriServer) CommitTasks(
	ctx context.Context,
	in *proto.CommitTasksRequest) (*proto.CommitTasksResponse, error) {

	if in == nil {
		return nil, ErrRequestObjectIsNil
	}
	if in.Keys == nil || len(in.Keys) == 0 {
		return nil, ErrRequestObjectArrayIsEmptyOrNil
	}

	for _, t := range in.Keys {
		if t == "" {
			return nil, ErrCommitKeyShouldNotBeEmpty
		}
	}

	committedKeys := make([]string, 0, len(in.Keys))
	as.inflightMutex.Lock()
	for _, key := range in.Keys {
		item, ok := as.inflightRecords.Get(key)
		if ok {
			as.inflightRecords.Delete(key)
			committedKeys = append(committedKeys, key)
			// put first, so can be reused directly
			pqItemPool.Put(item)
		}
	}
	as.inflightMutex.Unlock()

	as.writeCommitKeyToWal(committedKeys)

	return &proto.CommitTasksResponse{
		Keys: committedKeys,
	}, nil
}

func (as *AntriServer) writeRetryOccurenceToWal(items []*ds.PqItem) {
	as.walFile.M.Lock()
	for _, item := range items {
		err := WriteRetriesOccurenceToLog(as.walFile.F, item)
		if err != nil {
			log.Fatal(err)
		}
	}
	err := as.walFile.F.Sync()
	if err != nil {
		log.Fatal(err)
	}
	as.rollWal()
	as.walFile.M.Unlock()
}

type antriServerStats struct {
	WaitingTask   int `json:"waiting_tasks"`
	InflightTasks int `json:"inflight_tasks"`
}

func (as *AntriServer) stats() []byte {
	waitingTask := as.pq.HeapSize()
	inflightTasks := as.inflightRecords.Length()
	byteArray, _ := json.Marshal(antriServerStats{
		WaitingTask:   waitingTask,
		InflightTasks: inflightTasks,
	})
	return byteArray
}

func (as *AntriServer) batchInsertIntoInMemoryDS(items []*ds.PqItem) {
	as.mutex.Lock()
	for _, item := range items {
		as.pq.Insert(item)
	}
	as.mutex.Unlock()
}

// recovers the current state of the MQ from the data directory
//
// also returns the last known counter value
func (as *AntriServer) recovery() int {
	files, err := ioutil.ReadDir(dataDir)
	if err != nil {
		log.Fatalf("Failed reading directory for snapshotting: %v", err)
	}

	lastKnownCounter := 0
	lastSnapshotFilename := ""
	itemPlaceholder := []*ds.PqItem{}
	checkpointFiles := sortedListOfFilesInDirMatchingARegex(files, "snapshot")
	if len(checkpointFiles) > 0 {
		lastSnapshotFilename = checkpointFiles[len(checkpointFiles)-1]
		lastKnownCounter, err = fileSequenceNumber(lastSnapshotFilename)
		if err != nil {
			log.Fatal(err)
		}

		// we can open the old snapshot file
		// but only read the last one
		// all other files are just undeleted files
		lastSnapshotFile, err := os.OpenFile(
			dataDir+lastSnapshotFilename, os.O_RDONLY, fileMode)
		if err != nil {
			log.Fatalf("Failed reading last snapshot file, which is `%s` with error %v",
				lastSnapshotFilename, err)
		}
		defer lastSnapshotFile.Close()

		r, err := ReadSnapshotContents(lastSnapshotFile)
		if err != nil {
			log.Fatal(err)
		}
		itemPlaceholder = append(itemPlaceholder, r...)
	}

	walFilesToRecovered := sortedListOfFilesInDirMatchingARegex(files, "wal")
	sort.Strings(walFilesToRecovered)
	if len(walFilesToRecovered) == 0 || (lastSnapshotFilename != "" &&
		strings.Compare( // need to check if no new files from name too
			fileSequenceNumberAsString(walFilesToRecovered[len(walFilesToRecovered)-1]),
			fileSequenceNumberAsString(lastSnapshotFilename)) <= 0) {
		as.batchInsertIntoInMemoryDS(itemPlaceholder)
		log.Printf("No wal files to be recovered, skipping ...")
		return lastKnownCounter
	}

	for _, f := range walFilesToRecovered {
		// only process those that has not yet been included to the previous snapshot
		// to guarantee exactly once internally
		if lastSnapshotFilename != "" && strings.Compare(
			fileSequenceNumberAsString(f),
			fileSequenceNumberAsString(lastSnapshotFilename)) <= 0 {
			continue
		}

		walFileCounter, err := fileSequenceNumber(f)
		if err != nil {
			log.Fatal(err)
		}
		lastKnownCounter = walFileCounter

		walFile, err := os.OpenFile(dataDir+f, os.O_RDONLY, fileMode)
		if err != nil {
			log.Fatalf("Failed reading wal file, which is `%s` with error %v", f, err)
		}
		defer walFile.Close()

		log.Printf("Reading %s...", walFile.Name())
		itemPlaceholder, err = ReadLogMultiple(walFile, itemPlaceholder)
		if err != nil {
			log.Fatal(err)
		}
	}

	as.batchInsertIntoInMemoryDS(itemPlaceholder)

	// put all to pool
	// to reduce the need to allocate memory later
	for _, item := range itemPlaceholder {
		pqItemPool.Put(item)
	}

	return lastKnownCounter
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
		case <-as.runningCtx.Done():
			return
		case <-ticker.C:
			as.walFile.M.Lock()
			currentWalFilename := filepath.Base(as.walFile.F.Name())
			currentWalCounter := as.walFile.C
			as.walFile.M.Unlock()

			files, err := ioutil.ReadDir(dataDir)
			if err != nil {
				log.Fatalf("Failed reading directory for snapshotting: %v", err)
			}

			log.Printf("Snapshotting started at %d", time.Now().Unix())

			// create new names first, so can be used to match and limit files read
			// also create placeholder files, which will hold the curretn snapshot data
			// we don't directly writes into snapshot file
			// cause it may crashes in the middle (not atomic for writing more than 1 page)
			// instead, we will atomically rename the file into snapshot after all done
			lastSnapshotFilename := ""

			// do snapshot until just the last file before current number
			newSnapshotFilename := fmt.Sprintf("snapshot-%016d", currentWalCounter-1)
			placeholderFilename := fmt.Sprintf("placeholder-%016d", currentWalCounter-1)

			// for now, use a slice
			// this is NOT efficient, and gonna be changed later
			// because we also need to search them by key (for RETRY AND COMMIT)
			itemPlaceholder := []*ds.PqItem{}
			snapshotFiles := sortedListOfFilesInDirMatchingARegexUntilALimit(files, "snapshot", newSnapshotFilename)
			if len(snapshotFiles) > 0 {
				// we can open the old snapshot file
				// but only read the last one
				// all other files are just undeleted files
				lastSnapshotFile, err := os.OpenFile(
					dataDir+snapshotFiles[len(snapshotFiles)-1], os.O_RDONLY, fileMode)
				if err != nil {
					log.Fatalf("Failed reading last snapshot file, which is `%s` with error %v",
						snapshotFiles[len(snapshotFiles)-1], err)
				}

				// record lastCheckpointedFilename
				lastSnapshotFilename = snapshotFiles[len(snapshotFiles)-1]

				r, err := ReadSnapshotContents(lastSnapshotFile)
				if err != nil {
					log.Fatal(err)
				}
				itemPlaceholder = append(itemPlaceholder, r...)
				lastSnapshotFile.Close()
			}

			walFilesToBeCompacted := sortedListOfFilesInDirMatchingARegexUntilALimit(files, "wal", currentWalFilename)
			sort.Strings(walFilesToBeCompacted)
			if len(walFilesToBeCompacted) == 0 || (lastSnapshotFilename != "" &&
				strings.Compare( // need to check if no new files from name too
					fileSequenceNumberAsString(walFilesToBeCompacted[len(walFilesToBeCompacted)-1]),
					fileSequenceNumberAsString(lastSnapshotFilename)) <= 0) {
				log.Printf("No new files, skipping ...")
				continue
			}

			snapshotPlaceholderFile, err := os.OpenFile(
				dataDir+placeholderFilename,
				os.O_APPEND|os.O_CREATE|os.O_WRONLY, fileMode)
			if err != nil {
				log.Fatalf("Failed creating placeholder file for snapshotting, with error: %v", err)
			}
			defer snapshotPlaceholderFile.Close()

			for _, f := range walFilesToBeCompacted {
				// only process those that has not yet been included to the previous snapshot
				// to guarantee exactly once internally
				if lastSnapshotFilename != "" && strings.Compare(
					fileSequenceNumberAsString(f),
					fileSequenceNumberAsString(lastSnapshotFilename)) <= 0 {
					continue
				}

				walFile, err := os.OpenFile(dataDir+f, os.O_RDONLY, fileMode)
				if err != nil {
					log.Fatalf("Failed reading wal file, which is `%s` with error %v", f, err)
				}

				log.Printf("Reading %s...", walFile.Name())
				itemPlaceholder, err = ReadLogMultiple(walFile, itemPlaceholder)
				if err != nil {
					log.Fatal(err)
				}
				walFile.Close()
			}

			for _, item := range itemPlaceholder {
				WriteNewMessageToLog(snapshotPlaceholderFile, item)
			}
			err = snapshotPlaceholderFile.Sync()
			if err != nil {
				log.Fatalf("Failed fsync snapshot, with error: %v", err)
			}
			snapshotPlaceholderFile.Close()

			// after this rename, snapshotting is considered done (COMMITTED)
			err = os.Rename(
				dataDir+placeholderFilename,
				dataDir+newSnapshotFilename)
			if err != nil {
				log.Fatalf("Failed renaming into snapshot file, with error: %v", err)
			}
			log.Printf("Snapshotting finished at %d", time.Now().Unix())

			// put all to pool, to reduce the need to allocate memory
			for _, item := range itemPlaceholder {
				pqItemPool.Put(item)
			}

			// delete all wals, snapshots, and placeholders until current snapshot
			// this process is safe, as it only removes until just before the just created checkpoint
			previousPlaceholderFiles := sortedListOfFilesInDirMatchingARegex(files, "placeholder")
			for _, f := range previousPlaceholderFiles {
				os.Remove(dataDir + f)
			}
			for _, f := range snapshotFiles {
				os.Remove(dataDir + f)
			}
			for _, f := range walFilesToBeCompacted {
				os.Remove(dataDir + f)
			}
		}
	}
}

// taskTimeoutWatchdog watches all the in-flight task
// to guarantee liveness of the data
func (as *AntriServer) taskTimeoutWatchdog() {
	// allocate once
	itemArr := make([]*ds.PqItem, 0, 10)

	// cause we always append to back
	// we can be sure that this array is sorted on expireOn
	ticker := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-as.runningCtx.Done():
			return
		case <-ticker.C:

			// remove from inflight
			currentTime := time.Now().Unix()
			as.inflightMutex.Lock()
			for as.inflightRecords.Length() > 0 &&
				as.inflightRecords.PeekExpire() < currentTime {

				item, ok := as.inflightRecords.Pop()
				if ok {
					itemArr = append(itemArr, item)
				}
			}
			as.inflightMutex.Unlock()

			for _, item := range itemArr {
				item.Retries++
			}
			as.writeRetryOccurenceToWal(itemArr)
			as.addNewMessageToInMemoryDS(itemArr)

			// reset, to reduce allocation
			itemArr = itemArr[:0]
		}
	}
}

// Close all the underlying system
func (as *AntriServer) Close() {
	as.gs.Stop()

	log.Println("Stopping all background workers...")
	as.cancelFunc()

	log.Println("Closing wal file...")
	as.walFile.F.Close()
}

// Run AntriServer
//
// Returns when grpcServer returns
func (as *AntriServer) Run(
	gs *grpc.Server,
	lis net.Listener) error {
	as.gs = gs
	proto.RegisterAntriServer(as.gs, as)
	log.Println("Server started")
	return as.gs.Serve(lis)
}
