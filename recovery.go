package main

import (
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"log"

	"github.com/aarondwi/antri/ds"
)

var (
	// ErrFailedParsingContent raises when some error in binary data are found,
	// meaning corrupted data
	ErrFailedParsingContent = errors.New("Failed parsing content of log from buffer")

	// ErrRetryNotFound raises when RETRY message found, but no corresponding message
	ErrRetryNotFound = errors.New("RETRY message without the corresponding message")

	// ErrCommitNotFound raises when COMMIT message found, but no corresponding message
	ErrCommitNotFound = errors.New("COMMIT message without the corresponding message")

	// ErrUnknownCode raises when corrupted data is found
	ErrUnknownCode = errors.New("Found unknown indicator code beside NEW, RETRY, or COMMIT")

	// ErrSnapshotCorrupted raises when reading snapshot and found other than msgType 0
	ErrSnapshotCorrupted = errors.New("Found other types other than NEW in the snapshots")
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
func WriteNewMessageToLog(w io.Writer, pi *ds.PqItem) error {
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
		return err
	}
	return nil
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
func WriteRetriesOccurenceToLog(w io.Writer, pi *ds.PqItem) error {
	buf := make([]byte, 11)

	buf[0] = 1 // RETRY indicator
	binary.LittleEndian.PutUint64(buf[1:], uint64(pi.ScheduledAt))
	binary.LittleEndian.PutUint16(buf[9:], uint16(len(pi.Key)))
	buf = append(buf, pi.Key...)

	_, err := w.Write(buf)
	if err != nil {
		return err
	}

	return nil
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
func WriteCommitMessageToLog(w io.Writer, key []byte) error {
	buf := make([]byte, 3)

	buf[0] = 2 // COMMIT indicator
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(key)))

	buf = append(buf, key...)

	_, err := w.Write(buf)
	if err != nil {
		return err
	}
	return nil
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
func ReadLog(r io.Reader) (MsgWrapper, error) {
	indicatorHolder := make([]byte, 1)
	int16holder := make([]byte, 2)
	int32holder := make([]byte, 4)
	int64holder := make([]byte, 8)

	n, err := r.Read(indicatorHolder)
	if err != nil && err != io.EOF {
		log.Printf("failed parsing indicator: %d, and error %v", n, err)
		return MsgWrapper{}, ErrFailedParsingContent
	}
	if err == io.EOF {
		return MsgWrapper{}, io.EOF
	}

	indicator := indicatorHolder[0]
	if indicator == 0 { // new message
		n, err = r.Read(int64holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing scheduledAt: %d", n)
			return MsgWrapper{}, ErrFailedParsingContent
		}
		scheduledAtRead := binary.LittleEndian.Uint64(int64holder)

		n, err = r.Read(int16holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing lengthOfKey: %d", n)
			return MsgWrapper{}, ErrFailedParsingContent
		}
		lengthOfKeyRead := binary.LittleEndian.Uint16(int16holder)

		n, err = r.Read(int32holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing lengthOfValue: %d", n)
			return MsgWrapper{}, ErrFailedParsingContent
		}
		lengthOfValueRead := binary.LittleEndian.Uint32(int32holder)

		n, err = r.Read(int16holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing numOfRetries: %d", n)
			return MsgWrapper{}, ErrFailedParsingContent
		}
		numOfRetriesRead := binary.LittleEndian.Uint16(int16holder)

		keyBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfKeyRead)))
		if err != nil {
			log.Printf("failed reading key: %v", err)
			return MsgWrapper{}, ErrFailedParsingContent
		}

		valueBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfValueRead)))
		if err != nil {
			log.Printf("failed reading value: %v", err)
			return MsgWrapper{}, ErrFailedParsingContent
		}

		return MsgWrapper{
			msgType: 0,
			item: ds.PqItem{
				ScheduledAt: int64(scheduledAtRead),
				Key:         string(keyBytes),
				Value:       string(valueBytes),
				Retries:     int16(numOfRetriesRead)},
		}, nil
	} else if indicator == 1 {
		n, err = r.Read(int64holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing scheduledAt: %d", n)
			return MsgWrapper{}, ErrFailedParsingContent
		}
		scheduledAtRead := binary.LittleEndian.Uint64(int64holder)

		n, err = r.Read(int16holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing lengthOfKey: %d", n)
			return MsgWrapper{}, ErrFailedParsingContent
		}
		lengthOfKeyRead := binary.LittleEndian.Uint16(int16holder)

		keyBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfKeyRead)))
		if err != nil {
			log.Printf("failed reading key: %v", err)
			return MsgWrapper{}, ErrFailedParsingContent
		}

		return MsgWrapper{
			msgType: 1,
			item: ds.PqItem{
				ScheduledAt: int64(scheduledAtRead),
				Key:         string(keyBytes)},
		}, nil
	} else if indicator == 2 {
		n, err := r.Read(int16holder)
		if n == 0 || err != nil {
			log.Printf("failed parsing lengthOfKey: %d -> %v", n, err)
			return MsgWrapper{}, ErrFailedParsingContent
		}
		lengthOfKeyRead := binary.LittleEndian.Uint16(int16holder)

		keyBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfKeyRead)))
		if err != nil {
			log.Printf("failed reading key: %v", err)
			return MsgWrapper{}, ErrFailedParsingContent
		}

		return MsgWrapper{
			msgType: 2,
			item: ds.PqItem{
				Key: string(keyBytes)},
		}, nil
	} else {
		log.Printf("Unknown indicator number: %d", indicator)
		return MsgWrapper{}, ErrUnknownCode
	}
}

// ReadLogMultiple uses ReadLog internally
// and combine all of them into an array
func ReadLogMultiple(r io.Reader) ([]*ds.PqItem, error) {
	itemPlaceholder := []*ds.PqItem{}

	for {
		placeholder, err := ReadLog(r)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if err == io.EOF {
			break
		}
		if placeholder.msgType == 0 {
			itemPlaceholder = append(itemPlaceholder, &placeholder.item)
		} else if placeholder.msgType == 1 {
			pos := indexOfPqItemWithTheGivenKey(itemPlaceholder, placeholder.item.Key)
			if pos == -1 {
				log.Printf("Found Key: %s", placeholder.item.Key)
				return nil, ErrRetryNotFound
			}
			itemPlaceholder[pos].Retries++
		} else if placeholder.msgType == 2 {
			pos := indexOfPqItemWithTheGivenKey(itemPlaceholder, placeholder.item.Key)
			if pos == -1 {
				log.Printf("Found Key: %s", placeholder.item.Key)
				return nil, ErrCommitNotFound
			}
			itemPlaceholder = append(itemPlaceholder[:pos], itemPlaceholder[pos+1:]...)
		} else {
			log.Printf("Found unknown message format code : %d", placeholder.msgType)
			return nil, ErrUnknownCode
		}
	}

	return itemPlaceholder, nil
}

// ReadSnapshotContents reads NEW messages from r
func ReadSnapshotContents(r io.Reader) ([]*ds.PqItem, error) {
	itemPlaceholder := []*ds.PqItem{}
	for {
		placeholder, err := ReadLog(r)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if err == io.EOF {
			break
		}
		if placeholder.msgType != 0 {
			log.Printf("CORRUPTED SNAPSHOT DATA!!!! Found Code : %d", placeholder.msgType)
			return nil, ErrSnapshotCorrupted
		}
		itemPlaceholder = append(itemPlaceholder, &placeholder.item)
	}
	return itemPlaceholder, nil
}
