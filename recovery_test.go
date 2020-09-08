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

	ok := WriteNewMessageToLog(buf, src)
	if !ok {
		log.Fatalf("write new message should return `true`, but it is `false`")
	}
	ok = WriteRetriesOccurenceToLog(buf, src)
	if !ok {
		log.Fatalf("write retry occurence should return `true`, but it is `false`")
	}
	ok = WriteCommitMessageToLog(buf, []byte(src.Key))
	if !ok {
		log.Fatalf("write commit message should return `true`, but it is `false`")
	}

	// re-read the NEW message
	dst, ok := ReadLog(buf)
	if !ok {
		log.Fatalf("reading NEW message should return `true`, but it is not")
	}
	if src.ScheduledAt != dst.item.ScheduledAt ||
		src.Key != dst.item.Key ||
		src.Value != dst.item.Value ||
		src.Retries != dst.item.Retries {
		log.Fatalf("Corrupted NEW message, expected %v, got %v", src, dst.item)
	}

	// re-read the RETRY message
	dst, ok = ReadLog(buf)
	if !ok {
		log.Fatalf("reading RETRY message should return `true`, but it is not")
	}
	if dst.item.Key != src.Key ||
		dst.item.ScheduledAt != src.ScheduledAt {
		log.Fatalf("Corrupted RETRY message, expected %s and %d, got %s and %d", src.Key, src.ScheduledAt, dst.item.Key, dst.item.ScheduledAt)
	}

	// re-read the COMMIT message
	dst, ok = ReadLog(buf)
	if !ok {
		log.Fatalf("reading COMMIT message should return `true`, but it is not")
	}
	if dst.item.Key != src.Key {
		log.Fatalf("Corrupted COMMIT message, expected %s, got %s", src.Key, dst.item.Key)
	}
}
