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
// retry indicator (the value 0) -> 1 byte
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

	buf[0] = 0 // new message indicator
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
// retry indicator (the value 1) -> 1 byte
//
// scheduledAt int64 -> 8 bytes
//
// length of key int16 -> 2 bytes
//
// key string/[]byte
func WriteRetriesOccurenceToLog(w io.Writer, pi *ds.PqItem) bool {
	buf := make([]byte, 11)

	buf[0] = 1 // retry indicator
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

type retryWrapper struct {
	retryKey    string
	scheduledAt int64
}

// MsgWrapper wraps 2 type of values,
// either a new message, or a retry key
type MsgWrapper struct {
	item  *ds.PqItem
	retry retryWrapper
}

// ReadMessageFromLog reads log into a *ds.PqItem.
// Designed as a method because gonna be using sync.Pool later.
func ReadMessageFromLog(r io.Reader) (*MsgWrapper, bool) {
	indicatorHolder := make([]byte, 1)
	int16holder := make([]byte, 2)
	int32holder := make([]byte, 4)
	int64holder := make([]byte, 8)

	n, err := r.Read(indicatorHolder)
	if n == 0 || err != nil {
		log.Printf("failed parsing indicator: %d", n)
		return nil, false
	}

	indicator := indicatorHolder[0]
	if indicator == 0 { // new message
		n, err = r.Read(int64holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing scheduledAt: %d", n)
			return nil, false
		}
		scheduledAtRead := binary.LittleEndian.Uint64(int64holder)

		n, err = r.Read(int16holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing lengthOfKey: %d", n)
			return nil, false
		}
		lengthOfKeyRead := binary.LittleEndian.Uint16(int16holder)

		n, err = r.Read(int32holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing lengthOfValue: %d", n)
			return nil, false
		}
		lengthOfValueRead := binary.LittleEndian.Uint32(int32holder)

		n, err = r.Read(int16holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing numOfRetries: %d", n)
			return nil, false
		}
		numOfRetriesRead := binary.LittleEndian.Uint16(int16holder)

		keyBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfKeyRead)))
		if err != nil {
			log.Printf("failed reading key: %v", err)
			return nil, false
		}

		valueBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfValueRead)))
		if err != nil {
			log.Printf("failed reading value: %v", err)
			return nil, false
		}

		return &MsgWrapper{
			item: &ds.PqItem{
				ScheduledAt: int64(scheduledAtRead),
				Key:         string(keyBytes),
				Value:       string(valueBytes),
				Retries:     int16(numOfRetriesRead)},
		}, true
	} else if indicator == 1 {
		n, err = r.Read(int64holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing scheduledAt: %d", n)
			return nil, false
		}
		scheduledAtRead := binary.LittleEndian.Uint64(int64holder)

		n, err = r.Read(int16holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing lengthOfKey: %d", n)
			return nil, false
		}
		lengthOfKeyRead := binary.LittleEndian.Uint16(int16holder)

		keyBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfKeyRead)))
		if err != nil {
			log.Printf("failed reading key: %v", err)
			return nil, false
		}

		return &MsgWrapper{
			retry: retryWrapper{
				retryKey:    string(keyBytes),
				scheduledAt: int64(scheduledAtRead),
			}}, true
	} else {
		log.Fatalf("Unknown indicator number: %d", indicator)
		return nil, false
	}
}

// WriteCommittedKeyToLog writes key that is taken already
//
// after written here, the task is considered committed
//
// format -> 2 bytes length of key + key
func WriteCommittedKeyToLog(w io.Writer, key []byte) bool {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(len(key)))

	buf = append(buf, key...)

	_, err := w.Write(buf)
	if err != nil {
		log.Printf("failed writing log: %v", err)
		return false
	}
	return true
}

// ReadCommittedKeyFromLog reads the given reader
// to get 1 committed key
func ReadCommittedKeyFromLog(r io.Reader) (string, bool) {
	int16holder := make([]byte, 2)
	n, err := r.Read(int16holder)
	if n == 0 || err != nil {
		log.Printf("failed parsing lengthOfKey: %d -> %v", n, err)
		return "", false
	}
	lengthOfKeyRead := binary.LittleEndian.Uint16(int16holder)

	keyBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfKeyRead)))
	if err != nil {
		log.Printf("failed reading key: %v", err)
		return "", false
	}

	return string(keyBytes), true
}
