package main

import (
	"bytes"
	"log"
	"testing"
	"time"

	"github.com/aarondwi/antri/ds"
)

func TestWriteReadPqitemToLog(t *testing.T) {
	now := time.Now().Unix()
	buf := new(bytes.Buffer)

	src := &ds.PqItem{
		ScheduledAt: now,
		Key:         "ありがとう ございます",
		Value:       "ど いたしまして",
		Retries:     7}

	ok := WritePqItemToLog(buf, src)
	if !ok {
		log.Fatalf("if success should return `true`, but it is `false`")
	}
	dst := ReadPqItemFromLog(buf)
	if dst == nil {
		log.Fatalf("Should not be nil!")
	}
	if src.ScheduledAt != dst.ScheduledAt ||
		src.Key != dst.Key ||
		src.Value != dst.Value ||
		src.Retries != dst.Retries {
		log.Fatalf("Expected %v, got %v", src, dst)
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
