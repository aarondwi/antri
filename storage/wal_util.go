package storage

import (
	"os"
	"sync"
)

var WAL_FILE_FLAG = os.O_CREATE | os.O_RDWR
var WAL_FILE_MODE = os.FileMode(0644)
var WAL_FILE_SIZE = 32 * 1024 * 1024

// a WAL file is 32MB, and we allow 4 pending in the fsync chan.
// to guarantee only 1 new file not yet created, we need to bind
// the hard limit to also be 32/4 = 8 MB.
//
// Just to allow more into a file, divide by 2, so 4 MB
var WAL_BATCH_UPPER_LIMIT = 4 * 1024 * 1024

// too small data fsync'ed waste the call
//
// 128KB seems like a good balance
var WAL_BATCH_LOWER_LIMIT = 128 * 1024

// It is supposed to be binary data, and because aimed for business case,
// 256KB should be much more than enough
//
// For comparison:
//
// 1. AWS SQS allows 256KB, but priced for each 256KB
//
// 2. Facebook's FOQS allows only 10KB message size
var WAL_ITEM_SIZE_LIMIT = 256 * 1024

// WRITE_CHAN_SIZE limits the number of waiting to be written and/or fsync'ed
// As doing more, we gonna just put in the backlog, while the OS/disk is having problem keeping-up
var WRITE_CHAN_SIZE = 4

// RECORD_METADATA_SIZE is other data recorded together with data
//
// consists of:
//
// 1. 1 byte code (1 for new record)
//
// 2. 4 byte data length
//
// 3. 4 byte crc32 checksum
var RECORD_METADATA_SIZE = 9

// finding this means it is a new record
var NEW_RECORD_CODE = []byte{1}

// finding this means this file has no more records
var FILE_TERMINAL_CODE = []byte{0}

var walBufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, WAL_FILE_SIZE)
	},
}

// PutWalBuffer returns wal buffer to pool.
// Can be used to maintain memory usage during snapshot/recovery.
//
// No published `GetWalBuffer` method,
// because mostly the buffer is used by `Load()` walManager calls
func PutWalBuffer(buf []byte) {
	buf = buf[0:]
	walBufferPool.Put(buf)
}
