package storage

import "sync"

// batchHandle is a struct that holds a few internal metadata
// used by background goroutine to maintain state.
type batchHandle struct {
	wg                  *sync.WaitGroup
	id                  uint64
	currentPos          int
	writtenFileNumber   uint64
	needToCreateNewFile bool
	buffer              []byte
}

var emptyBatchHandle = &batchHandle{}

// RecordHandle is our `future` implementation
// which will notify its caller the result of the batch.
//
// we separate this implementation from batchHandle
// so we can easily add more attribute to it later, if needed
type RecordHandle struct {
	bh *batchHandle
}

var emptyRecordHandle = RecordHandle{}

// GetBatchId allows the caller to track which batch
// the returned value corresponds to
func (rh *RecordHandle) GetBatchId() uint64 {
	return rh.bh.id
}

// GetFileNumber waits until the batch is finished, then return in which wal file the batch is
func (rh *RecordHandle) GetFileNumber() uint64 {
	rh.bh.wg.Wait()
	return rh.bh.writtenFileNumber
}
