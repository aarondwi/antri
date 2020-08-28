package main

import (
	"bytes"
	"log"
	"testing"
	"time"

	"github.com/aarondwi/antri/ds"
)

func TestWriteReadNewMessage(t *testing.T) {
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

	// re-read the message
	dst, ok := ReadMessageFromLog(buf)
	if !ok {
		log.Fatalf("reading message should be true, but it is not")
	}
	if dst == nil {
		log.Fatalf("Should not be nil!")
	}
	if src.ScheduledAt != dst.item.ScheduledAt ||
		src.Key != dst.item.Key ||
		src.Value != dst.item.Value ||
		src.Retries != dst.item.Retries {
		log.Fatalf("Expected %v, got %v", src, dst.item)
	}

	// re-read the retry occurence
	dst, ok = ReadMessageFromLog(buf)
	if !ok {
		log.Fatalf("reading message should be true, but it is not")
	}
	if dst == nil {
		log.Fatalf("Should not be nil!")
	}
	if dst.retry.retryKey != src.Key ||
		dst.retry.scheduledAt != src.ScheduledAt {
		log.Fatalf("Corrupted retry message, expected %s, got %v", src.Key, dst.retry)
	}
}

func TestWriteReadForCommit(t *testing.T) {
	buf := new(bytes.Buffer)
	src := "ありがとう ございます"

	ok := WriteCommittedKeyToLog(buf, []byte(src))
	if !ok {
		log.Fatalf("if success should return `true`, but it is `false`")
	}

	dst, ok := ReadCommittedKeyFromLog(buf)
	if !ok {
		log.Fatalf("Should be `true`!, but it is not")
	}
	if src != dst {
		log.Fatalf("Some changes happen from write to read: %s -> %s", src, dst)
	}
}
