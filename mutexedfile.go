package main

import (
	"os"
	"sync"
)

// MutexedFile holds a file for wal and its mutex
type MutexedFile struct {
	M *sync.Mutex
	F *os.File
}
