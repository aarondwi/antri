package main

import (
	"bytes"
	"log"
	"testing"
	"time"

	"github.com/aarondwi/antri/ds"
)

func TestWriteReadMessage(t *testing.T) {
	now := time.Now().Unix()
	buf := new(bytes.Buffer)

	src := &ds.PqItem{
		ScheduledAt: now,
		Key:         "ありがとう ございます",
		Value:       "ど いたしまして",
		Retries:     7}

	err := WriteNewMessageToLog(buf, src)
	if err != nil {
		log.Fatalf("write new message should be successful, but got error %v", err)
	}
	err = WriteRetriesOccurenceToLog(buf, src)
	if err != nil {
		log.Fatalf("write retry occurence should be successful, but got error %v", err)
	}
	err = WriteCommitMessageToLog(buf, []byte(src.Key))
	if err != nil {
		log.Fatalf("write commit message should be successful, but got error %v", err)
	}

	// re-read the NEW message
	dst, err := ReadLog(buf)
	if err != nil {
		log.Fatalf("reading NEW message should return `true`, but it is not")
	}
	if src.ScheduledAt != dst.item.ScheduledAt ||
		src.Key != dst.item.Key ||
		src.Value != dst.item.Value ||
		src.Retries != dst.item.Retries {
		log.Fatalf("Corrupted NEW message, expected %v, got %v", src, dst.item)
	}

	// re-read the RETRY message
	dst, err = ReadLog(buf)
	if err != nil {
		log.Fatalf("reading RETRY message should return `true`, but it is not")
	}
	if dst.item.Key != src.Key ||
		dst.item.ScheduledAt != src.ScheduledAt {
		log.Fatalf("Corrupted RETRY message, expected %s and %d, got %s and %d", src.Key, src.ScheduledAt, dst.item.Key, dst.item.ScheduledAt)
	}

	// re-read the COMMIT message
	dst, err = ReadLog(buf)
	if err != nil {
		log.Fatalf("reading COMMIT message should return `true`, but it is not")
	}
	if dst.item.Key != src.Key {
		log.Fatalf("Corrupted COMMIT message, expected %s, got %s", src.Key, dst.item.Key)
	}
}

func TestReadLogMultipleSuccess(t *testing.T) {
	now := time.Now().Unix()
	buf := new(bytes.Buffer)

	items := []*ds.PqItem{}
	items = append(items, &ds.PqItem{
		ScheduledAt: now,
		Key:         "abc",
		Value:       "ABC",
		Retries:     0})
	items = append(items, &ds.PqItem{
		ScheduledAt: now,
		Key:         "def",
		Value:       "DEF",
		Retries:     0})
	items = append(items, &ds.PqItem{
		ScheduledAt: now,
		Key:         "ghi",
		Value:       "GHI",
		Retries:     0})
	WriteNewMessageToLog(buf, items[0])
	WriteNewMessageToLog(buf, items[1])
	WriteNewMessageToLog(buf, items[2])
	WriteRetriesOccurenceToLog(buf, items[2])
	WriteRetriesOccurenceToLog(buf, items[0])
	WriteCommitMessageToLog(buf, []byte(items[1].Key))

	itemPlaceholder, err := ReadLogMultiple(buf)
	if err != nil {
		log.Fatal(err)
	}
	if len(itemPlaceholder) != 2 {
		log.Fatalf("should be 2, because 1 is committed, but got %d", len(itemPlaceholder))
	}
	if itemPlaceholder[0].Key != "abc" || itemPlaceholder[0].Retries != 1 {
		log.Fatalf("Corrupted positioning, got %v", itemPlaceholder[0])
	}
}

func TestReadLogMultipleFailed(t *testing.T) {
	now := time.Now().Unix()
	item := &ds.PqItem{
		ScheduledAt: now,
		Key:         "abc",
		Value:       "ABC",
		Retries:     0}

	// RETRY path
	buf := new(bytes.Buffer)
	WriteRetriesOccurenceToLog(buf, item)
	_, err := ReadLogMultiple(buf)
	if err == nil {
		log.Fatalf("should be error, because RETRY without NEW message")
	}

	// COMMIT PATH
	buf = new(bytes.Buffer)
	WriteCommitMessageToLog(buf, []byte(item.Key))
	_, err = ReadLogMultiple(buf)
	if err == nil {
		log.Fatalf("should be error, because COMMIT without NEW message")
	}

	// UNKNOWN CODE
	ErrBuffer := make([]byte, 1)
	ErrBuffer[0] = 10
	buf = new(bytes.Buffer)
	_, _ = buf.Write(ErrBuffer)
	_, err = ReadLogMultiple(buf)
	if err == nil {
		log.Fatalf("should be error, because UNKNOWN code")
	}
}
