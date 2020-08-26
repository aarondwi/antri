package main

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"log"

	"github.com/aarondwi/antri/ds"
)

// WritePqItemToLog format PqItem into binary data to write into storage.
// we separate it from Insert so we can use it on concurrently with main lock.
//
// our log format => 16 bytes metadata + key length + value length
//
// scheduledAt int64 -> 8 bytes
//
// length of key int16 -> 2 bytes
//
// length of value int32 -> 4 bytes
//
// number of retries int16 -> 2 bytes
//
// key string/[]byte
//
// value string/[]byte
func WritePqItemToLog(w io.Writer, pi *ds.PqItem) bool {
	buf := make([]byte, 16)

	binary.LittleEndian.PutUint64(buf, uint64(pi.ScheduledAt))
	binary.LittleEndian.PutUint16(buf[8:], uint16(len(pi.Key)))
	binary.LittleEndian.PutUint32(buf[10:], uint32(len(pi.Value)))
	binary.LittleEndian.PutUint16(buf[14:], uint16(pi.Retries))
	buf = append(buf, pi.Key...)
	buf = append(buf, pi.Value...)

	_, err := w.Write(buf)
	if err != nil {
		log.Printf("failed writing log: %v", err)
		return false
	}
	return true
}

// ReadPqItemFromLog reads log into a *ds.PqItem.
// Designed as a method because gonna be using sync.Pool later.
func ReadPqItemFromLog(r io.Reader) *ds.PqItem {
	int16holder := make([]byte, 2)
	int32holder := make([]byte, 4)
	int64holder := make([]byte, 8)

	n, err := r.Read(int64holder)
	if n == 0 || err != nil {
		log.Printf("failed parsing scheduledAt: %d", n)
		return nil
	}
	scheduledAtRead := binary.LittleEndian.Uint64(int64holder)

	n, err = r.Read(int16holder)
	if n == 0 || err != nil {
		log.Printf("failed parsing lengthOfKey: %d", n)
		return nil
	}
	lengthOfKeyRead := binary.LittleEndian.Uint16(int16holder)

	n, err = r.Read(int32holder)
	if n == 0 || err != nil {
		log.Printf("failed parsing lengthOfValue: %d", n)
		return nil
	}
	lengthOfValueRead := binary.LittleEndian.Uint32(int32holder)

	n, err = r.Read(int16holder)
	if n == 0 || err != nil {
		log.Printf("failed parsing numOfRetries: %d", n)
		return nil
	}
	numOfRetriesRead := binary.LittleEndian.Uint16(int16holder)

	keyBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfKeyRead)))
	if err != nil {
		log.Printf("failed reading key: %v", err)
		return nil
	}

	valueBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfValueRead)))
	if err != nil {
		log.Printf("failed reading value: %v", err)
		return nil
	}

	return &ds.PqItem{
		ScheduledAt: int64(scheduledAtRead),
		Key:         string(keyBytes),
		Value:       string(valueBytes),
		Retries:     int16(numOfRetriesRead),
	}
}

// WriteCommittedKeyToLog writes key that is taken already
// by default, this is the commit point
func WriteCommittedKeyToLog(w io.Writer, key []byte) bool {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(key)))

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
	int32holder := make([]byte, 4)
	n, err := r.Read(int32holder)
	if n == 0 || err != nil {
		log.Printf("failed parsing lengthOfKey: %d -> %v", n, err)
		return "", false
	}
	lengthOfKeyRead := binary.LittleEndian.Uint32(int32holder)

	keyBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfKeyRead)))
	if err != nil {
		log.Printf("failed reading key: %v", err)
		return "", false
	}

	return string(keyBytes), true
}

// WriteSnapshot ...
//
func WriteSnapshot(w io.Writer, added int64, taken int64) {

}

// ReadSnapshot ...
//
func ReadSnapshot(r io.Reader) (int64, int64, bool) {

	return 0, 0, true
}
