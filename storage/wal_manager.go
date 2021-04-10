package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

// AntriWalManager handles how antri interacts with filesystem, and implements `walManagerInterface`.
// The goal is to allow antri amortizes the cost of fsync across multiple request.
//
// This implementation allows `record()` call while fsync is on progress.
// To do that, that means fsync should be outside the lock, but still need reliable
// way to notify the fsync goroutine. This is done via fsync chan, buffered (hardcoded to 4, for now).
// If chan buffer is full, it is okay to still hold main lock (and block others), as
// it means the storage layer is not keeping up. (My decision, as of the time of writing)
//
// This implementation only uses `fsync`, and not `O_[D]SYNC`.
// While O_SYNC is usually faster, it doesn't work on windows, so
// writes on windows may not be synced at all.
// With fsync, both linux variant and windows behave the same,
// resulting in easy portability :)
//
// 2 important behavior not yet implemented (or should it?)
//
// 1. fsync on directory after creating file, because filesystem implementation may miss it.
//
// 2. Aligned write. Current implementation only write from 1 goroutine, and always wait after that.
// Currently no case for writing concurrently from many goroutines, or managing the block manually (O_DIRECT)
type AntriWalManager struct {
	mu         *sync.Mutex
	newBatch   *sync.Cond
	ctx        context.Context
	cancelFunc context.CancelFunc

	// track all background goroutine and coordinate on shutdown
	wg *sync.WaitGroup

	fsyncChan chan *batchHandle

	dataDir            string
	currentFileCounter uint64
	f                  *os.File
	timeLimit          time.Duration

	lastID               uint64
	fourByteHolder       []byte
	bufferPool           sync.Pool
	currentBatch         *batchHandle
	currentFileBufferPos int
	itemSizeLimit        int
	bufferSoftLimit      int

	fsyncOnWrite bool
}

// NewWalManager creates our AntriWalManager object
func NewWalManager(
	dataDir string,
	itemSizeLimit int,
	batchSize int,
	timeLimit time.Duration,
	fsyncOnWrite bool) (*AntriWalManager, error) {

	if (itemSizeLimit < 0) || (itemSizeLimit > WAL_ITEM_SIZE_LIMIT) {
		return nil, ErrSizeLimitOutOfRange
	}

	if batchSize < WAL_BATCH_LOWER_LIMIT || batchSize > WAL_BATCH_UPPER_LIMIT {
		return nil, ErrSizeLimitOutOfRange
	}

	_, err := os.Stat(dataDir)
	if err != nil {
		err = os.Mkdir(dataDir, WAL_FILE_MODE)
		if err != nil {
			// no directory, and we can't create new one
			// abort
			log.Fatalf("Can't create directory for data, with error %v", err)
		}
	}

	mu := &sync.Mutex{}
	newBatch := sync.NewCond(mu)

	ctx, cancelFunc := context.WithCancel(context.Background())

	return &AntriWalManager{
		mu:         mu,
		newBatch:   newBatch,
		ctx:        ctx,
		cancelFunc: cancelFunc,
		wg:         &sync.WaitGroup{},

		fsyncChan: make(chan *batchHandle, 4),

		dataDir:   dataDir,
		timeLimit: timeLimit,

		fourByteHolder: make([]byte, 4),
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, batchSize+itemSizeLimit)
			},
		},
		itemSizeLimit:   itemSizeLimit,
		bufferSoftLimit: batchSize,

		fsyncOnWrite: fsyncOnWrite,
	}, nil
}

func (w *AntriWalManager) generateFilename(counter uint64) string {
	return fmt.Sprintf("%s/wal-%016d", w.dataDir, counter)
}

func (w *AntriWalManager) createNewFile(counter uint64) *os.File {
	// Any error here means we can't safely continue
	f, err := os.OpenFile(
		w.generateFilename(counter),
		WAL_FILE_FLAG,
		WAL_FILE_MODE)
	if err != nil {
		panic(err)
	}
	// pre-allocate the file
	err = f.Truncate(int64(WAL_FILE_SIZE))
	if err != nil {
		panic(err)
	}

	return f
}

func (w *AntriWalManager) incrementCounterAndCreateNewFile() {
	w.f.Close()
	w.currentFileCounter++
	w.f = w.createNewFile(w.currentFileCounter)
}

// Run our WalManager, also all corresponding background goroutines
func (w *AntriWalManager) Run(initialCounter uint64) {
	select {
	case <-w.ctx.Done():
		return
	default:
	}
	w.currentFileCounter = initialCounter
	w.f = w.createNewFile(w.currentFileCounter)
	w.wg.Add(2) // committer and timeLimitCommitter
	go w.committer()
	go w.timeLimitCommitter()
}

func (w *AntriWalManager) readyToFsync() {
	// there may be case where batch is currently nil
	// but the new data coming is bigger than filesize
	//
	// meaning we just need to create a new file
	if w.currentBatch == nil {
		w.fsyncChan <- emptyBatchHandle
		return
	}

	// normal flow
	// check whether we need to close it now
	// cause at most, it can only get w.itemSizeLimit
	//
	// the downside is that the file may has unused space (at most, is w.itemSizeLimit)
	needToCreateNewFile := w.currentFileBufferPos+w.itemSizeLimit > WAL_FILE_SIZE
	if needToCreateNewFile {
		w.currentFileBufferPos = 0
	}

	w.currentBatch.needToCreateNewFile = needToCreateNewFile
	w.fsyncChan <- w.currentBatch
	w.currentBatch = nil
}

// Record the given data to the batch, to be handled by committer goroutine
//
// The format is:
//
// 1. 1 byte `1`, indicating a new data
//
// 2. 4 bytes data length
//
// 3. data itself
//
// 4. a crc32 checksum. This also acts as commit record (as it is under page size, which usually is 4 KB)
func (w *AntriWalManager) Record(data []byte) (RecordHandle, error) {
	dataLength := len(data)
	totalLength := dataLength + RECORD_METADATA_SIZE

	// static, no need to check inside lock
	if totalLength > w.itemSizeLimit {
		return emptyRecordHandle, ErrDataTooBig
	}

	// create RecordHandle first
	// gonna be putting more data into this later on
	rh := RecordHandle{}
	checksum := crc32.ChecksumIEEE(data)

	w.mu.Lock()

	select {
	// if already closed, just return
	case <-w.ctx.Done():
		w.mu.Unlock()
		return emptyRecordHandle, ErrWalManagerClosed
	default:
	}

	if w.currentFileBufferPos+totalLength > WAL_FILE_SIZE {
		w.readyToFsync()
	}

	// no running batch, create one
	if w.currentBatch == nil {
		w.lastID++
		w.currentBatch = &batchHandle{
			id:         w.lastID,
			wg:         &sync.WaitGroup{},
			currentPos: 0,
			// can wg be pooled too? it said it shouldn't be copied after use.
			// So to be safe, pool the buffer only
			//
			// this also means, in the current implementation
			// for each batch, they will be 1 allocation, at least.
			buffer: w.bufferPool.Get().([]byte),
		}
		w.currentBatch.wg.Add(1)
		w.newBatch.Signal()
	}

	// reaching here means the data will not overflow the file,
	// so we can safely continue
	rh.bh = w.currentBatch

	// below comments may be useless, as it is clear already.
	// But putting comment allow segmenting them, so clearer which does which

	// ------- write newRecordCode -------
	copy(w.currentBatch.buffer[w.currentBatch.currentPos:], NEW_RECORD_CODE)

	// ------- write data length -------
	binary.LittleEndian.PutUint32(w.fourByteHolder, uint32(dataLength))
	copy(w.currentBatch.buffer[w.currentBatch.currentPos+1:], w.fourByteHolder)

	// ------- write our new data -------
	copy(w.currentBatch.buffer[w.currentBatch.currentPos+5:], data)

	// ------- write the checksum -------
	binary.LittleEndian.PutUint32(w.fourByteHolder, checksum)
	copy(w.currentBatch.buffer[w.currentBatch.currentPos+dataLength+5:], w.fourByteHolder)

	w.currentBatch.currentPos += totalLength
	w.currentFileBufferPos += totalLength

	shouldCommitAfterCopy := false
	if w.currentBatch.currentPos > w.bufferSoftLimit {
		// already reaching limit, need to fsync soon
		// to prevent fsync-ing too much data once
		shouldCommitAfterCopy = true
	}
	if shouldCommitAfterCopy {
		w.readyToFsync()
	}

	w.mu.Unlock()

	return rh, nil
}

// committer batch write + (optional) fsync to the storage
//
// For both write and sync call, we panic.
// Because it means for some reason, the filesystem can't be writen to,
// and continuing means violating our durability guarantee.
func (w *AntriWalManager) committer() {
	var bh *batchHandle
	for {
		select {
		// wait on either of these
		case <-w.ctx.Done():
			w.wg.Done()
			return
		case bh = <-w.fsyncChan:
		}

		if bh == emptyBatchHandle {
			// new item waiting is bigger than the file size limit, but no new batch
			//
			// just need to close, and open new one
			w.incrementCounterAndCreateNewFile()
		} else {
			// normal flow
			_, err := w.f.Write(bh.buffer[:bh.currentPos])
			if err != nil {
				panic(err)
			}

			if w.fsyncOnWrite {
				err := w.f.Sync()
				if err != nil {
					panic(err)
				}
			}

			bh.writtenFileNumber = w.currentFileCounter
			w.bufferPool.Put(bh.buffer)
			bh.buffer = nil

			if bh.needToCreateNewFile {
				// for not fsync on normal write, fsync now
				if !w.fsyncOnWrite {
					err := w.f.Sync()
					if err != nil {
						panic(err)
					}
				}
				w.incrementCounterAndCreateNewFile()
			}

			bh.wg.Done()
		}
	}
}

// timeLimitCommitter waits a new batch creation, then wait until specified timeout,
// with at-most once semantic. Possible flows:
//
// 1. Each batch creation will trigger the event,
// and then this goroutine will notify committer after specified timeout,
// while also atomically changing the currentBatch.
// So, no multiple notification for the same batch.
//
// 2. If it happened that our batches coming much faster than fsync,
// and they are queuing on fsync chan, it means this goroutine
// has no need to track those batches
func (w *AntriWalManager) timeLimitCommitter() {
	for {
		w.mu.Lock()
		for w.currentBatch == nil {
			select {
			case <-w.ctx.Done():
				w.mu.Unlock()
				w.wg.Done()
				return
			default:
			}
			w.newBatch.Wait()
		}
		batchIDToTrack := w.currentBatch.id
		w.mu.Unlock()

		time.Sleep(w.timeLimit)

		select {
		case <-w.ctx.Done():
			w.wg.Done()
			return
		default:
		}

		// meaning haven't changed since before,
		// need to commit now to maintain latency for aleady-waiting requests
		w.mu.Lock()
		if w.currentBatch != nil &&
			w.currentBatch.id == batchIDToTrack {
			w.readyToFsync()
		}
		w.mu.Unlock()
	}
}

// Load file of the given counter
//
// This function is not latency critical, and
// should be easy to parallelize (with bit of change)
//
// For now, it uses defer for readability.
//
// Any other errors happening here, probably the file is corrupted so bad
func (w *AntriWalManager) Load(fileCounter uint64) ([][]byte, error) {
	f, err := os.OpenFile(w.generateFilename(fileCounter), os.O_RDONLY, WAL_FILE_MODE)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := walBufferPool.Get().([]byte)
	// defer PutWalBuffer(buf)

	totalDataLength, err := f.Read(buf)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if totalDataLength == 0 {
		// sanity check
		// empty file, probably after creating new file, it stopped before writing new records
		return make([][]byte, 0), nil
	}

	// this is just an approx
	numOfItem := WAL_FILE_SIZE / WAL_ITEM_SIZE_LIMIT
	res := make([][]byte, 0, numOfItem)

	bufPos := 0
	for {
		// ------------ CHECK CODE --------------
		if bufPos+1 > totalDataLength {
			// corrupted? but so far so good
			// can return some (or not?)
			break
		}
		if bytes.Equal(buf[bufPos:bufPos+1], FILE_TERMINAL_CODE) {
			break
		}
		bufPos++

		// ------------ CHECK LENGTH --------------
		if bufPos+4 > totalDataLength {
			// corrupted? but so far so good
			// can return some (or not?)
			break
		}
		l := int(binary.LittleEndian.Uint32(buf[bufPos : bufPos+4]))
		bufPos += 4

		// ------------ CHECK DATA --------------
		if bufPos+l+4 > totalDataLength {
			// corrupted? but so far so good
			// can return some (or not?)
			break
		}
		checksum := binary.LittleEndian.Uint32(buf[bufPos+l : bufPos+l+4])

		bufHolder := buf[bufPos : bufPos+l]
		if crc32.ChecksumIEEE(bufHolder) != checksum {
			return nil, ErrWalFileCorrupted
		}

		res = append(res, bufHolder)
		bufPos += l + 4
	}
	return res, nil
}

// Close this instance, rejecting subsequent request
func (w *AntriWalManager) Close() {
	w.cancelFunc()
	w.newBatch.Broadcast()
	w.wg.Wait()

	// probably close after fsync is done
	// so need to do it here
	if !w.fsyncOnWrite {
		err := w.f.Sync()
		if err != nil {
			panic(err)
		}
	}
	w.f.Close()
}
