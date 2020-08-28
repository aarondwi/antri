package main

import (
	"os"
	"sync"
)

type wal struct {
	M *sync.Mutex
	F *os.File
	C int
	N int
}
