package main

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"log"

	"github.com/aarondwi/antri/ds"
)

// WriteNewMessageToLog format PqItem into binary data to write into storage.
// we separate it from Insert so we can use it on concurrently with main lock.
//
// our log format => 17 bytes metadata + key length + value length
//
// NEW message indicator (the value 0) -> 1 byte
//
// scheduledAt int64 -> 8 bytes
//
// length of key int16 -> 2 bytes (so later we can receive key from user)
//
// length of value int32 -> 4 bytes
//
// number of retries -> 2 bytes
//
// key string/[]byte
//
// value string/[]byte
func WriteNewMessageToLog(w io.Writer, pi *ds.PqItem) bool {
	buf := make([]byte, 17)

	buf[0] = 0 // NEW indicator
	binary.LittleEndian.PutUint64(buf[1:], uint64(pi.ScheduledAt))
	binary.LittleEndian.PutUint16(buf[9:], uint16(len(pi.Key)))
	binary.LittleEndian.PutUint32(buf[11:], uint32(len(pi.Value)))
	binary.LittleEndian.PutUint16(buf[15:], uint16(pi.Retries))
	buf = append(buf, pi.Key...)
	buf = append(buf, pi.Value...)

	_, err := w.Write(buf)
	if err != nil {
		log.Printf("failed writing log: %v", err)
		return false
	}
	return true
}

// WriteRetriesOccurenceToLog writes the key and the number of retries
//
// so we can still track the number of retries in spite of failure
//
// every time meets this item in the log, increase the number of retries by 1
//
// Retries is considered committed after they are written here
//
// the retry format -> 11 bytes + key
//
// RETRY indicator (the value 1) -> 1 byte
//
// scheduledAt int64 -> 8 bytes
//
// length of key int16 -> 2 bytes
//
// key string/[]byte
func WriteRetriesOccurenceToLog(w io.Writer, pi *ds.PqItem) bool {
	buf := make([]byte, 11)

	buf[0] = 1 // RETRY indicator
	binary.LittleEndian.PutUint64(buf[1:], uint64(pi.ScheduledAt))
	binary.LittleEndian.PutUint16(buf[9:], uint16(len(pi.Key)))
	buf = append(buf, pi.Key...)

	_, err := w.Write(buf)
	if err != nil {
		log.Printf("failed writing log: %v", err)
		return false
	}

	return true
}

// WriteCommitMessageToLog writes key that is taken already
//
// after written here, the task is considered committed
//
// format -> 3 bytes + key
//
// COMMIT indicator (the value 2) -> 1 byte
//
// length of key int16 -> 2 bytes
//
// key string/[]byte
func WriteCommitMessageToLog(w io.Writer, key []byte) bool {
	buf := make([]byte, 3)

	buf[0] = 2 // COMMIT indicator
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(key)))

	buf = append(buf, key...)

	_, err := w.Write(buf)
	if err != nil {
		log.Printf("failed writing log: %v", err)
		return false
	}
	return true
}

// MsgWrapper wraps msgType with an object of ds.PqItemMsgWrapper
// the MsgType:
//
// 0. NEW message
//
// 1. RETRY message
//
// 2. COMMIT message
type MsgWrapper struct {
	msgType uint8
	item    ds.PqItem
}

// ReadLog reads log into a *ds.PqItem.
// Designed as a method because gonna be using sync.Pool later.
func ReadLog(r io.Reader) (MsgWrapper, bool) {
	indicatorHolder := make([]byte, 1)
	int16holder := make([]byte, 2)
	int32holder := make([]byte, 4)
	int64holder := make([]byte, 8)

	n, err := r.Read(indicatorHolder)
	if n == 0 || err != nil {
		log.Printf("failed parsing indicator: %d", n)
		return MsgWrapper{}, false
	}

	indicator := indicatorHolder[0]
	if indicator == 0 { // new message
		n, err = r.Read(int64holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing scheduledAt: %d", n)
			return MsgWrapper{}, false
		}
		scheduledAtRead := binary.LittleEndian.Uint64(int64holder)

		n, err = r.Read(int16holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing lengthOfKey: %d", n)
			return MsgWrapper{}, false
		}
		lengthOfKeyRead := binary.LittleEndian.Uint16(int16holder)

		n, err = r.Read(int32holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing lengthOfValue: %d", n)
			return MsgWrapper{}, false
		}
		lengthOfValueRead := binary.LittleEndian.Uint32(int32holder)

		n, err = r.Read(int16holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing numOfRetries: %d", n)
			return MsgWrapper{}, false
		}
		numOfRetriesRead := binary.LittleEndian.Uint16(int16holder)

		keyBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfKeyRead)))
		if err != nil {
			log.Printf("failed reading key: %v", err)
			return MsgWrapper{}, false
		}

		valueBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfValueRead)))
		if err != nil {
			log.Printf("failed reading value: %v", err)
			return MsgWrapper{}, false
		}

		return MsgWrapper{
			msgType: 0,
			item: ds.PqItem{
				ScheduledAt: int64(scheduledAtRead),
				Key:         string(keyBytes),
				Value:       string(valueBytes),
				Retries:     int16(numOfRetriesRead)},
		}, true
	} else if indicator == 1 {
		n, err = r.Read(int64holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing scheduledAt: %d", n)
			return MsgWrapper{}, false
		}
		scheduledAtRead := binary.LittleEndian.Uint64(int64holder)

		n, err = r.Read(int16holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing lengthOfKey: %d", n)
			return MsgWrapper{}, false
		}
		lengthOfKeyRead := binary.LittleEndian.Uint16(int16holder)

		keyBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfKeyRead)))
		if err != nil {
			log.Printf("failed reading key: %v", err)
			return MsgWrapper{}, false
		}

		return MsgWrapper{
			msgType: 1,
			item: ds.PqItem{
				ScheduledAt: int64(scheduledAtRead),
				Key:         string(keyBytes)},
		}, true
	} else if indicator == 2 {
		n, err := r.Read(int16holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing lengthOfKey: %d -> %v", n, err)
			return MsgWrapper{}, false
		}
		lengthOfKeyRead := binary.LittleEndian.Uint16(int16holder)

		keyBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfKeyRead)))
		if err != nil {
			log.Printf("failed reading key: %v", err)
			return MsgWrapper{}, false
		}

		return MsgWrapper{
			msgType: 2,
			item: ds.PqItem{
				Key: string(keyBytes)},
		}, true
	} else {
		log.Fatalf("Unknown indicator number: %d", indicator)
		return MsgWrapper{}, false
	}
}
