package main

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/aarondwi/antri/ds"
)

func TestFileSequenceNumberAsString(t *testing.T) {
	seqNumber := "0000000000000015"
	filename := "data/wal-" + seqNumber
	result := fileSequenceNumberAsString(filename)
	if result != seqNumber {
		log.Fatalf("it should return the sequence number (after `-`), but got %s", result)
	}
}

func TestIndexOfPqItemWithTheGivenKey(t *testing.T) {
	arr := []*ds.PqItem{}
	arr = append(arr, &ds.PqItem{Key: "abc"})
	arr = append(arr, &ds.PqItem{Key: "def"})
	arr = append(arr, &ds.PqItem{Key: "ghi"})

	result := indexOfPqItemWithTheGivenKey(arr, "def")
	if result != 1 {
		log.Fatalf("`def` not at pos 1, but at %d", result)
	}

	result = indexOfPqItemWithTheGivenKey(arr, "notfound")
	if result != -1 {
		log.Fatalf("`notfound`should not be exist, but found at %d", result)
	}
}

type mockFileInfo struct {
	name     string
	size     int64
	filemode os.FileMode
	modtime  time.Time
	isDir    bool
}

func (mfi *mockFileInfo) Name() string {
	return mfi.name
}
func (mfi *mockFileInfo) Size() int64 {
	return mfi.size
}
func (mfi *mockFileInfo) Mode() os.FileMode {
	return mfi.filemode
}
func (mfi *mockFileInfo) ModTime() time.Time {
	return mfi.modtime
}
func (mfi *mockFileInfo) IsDir() bool {
	return mfi.isDir
}
func (mfi *mockFileInfo) Sys() interface{} {
	return nil
}

func TestSortedListOfItemInDirMatchingARegex(t *testing.T) {
	files := []os.FileInfo{}

	// mock all the os.FileInfo data
	files = append(files, &mockFileInfo{name: "data/.cache", isDir: true})
	files = append(files, &mockFileInfo{name: "data/wal-0000000000000003", isDir: false})
	files = append(files, &mockFileInfo{name: "data/wal-0000000000000006", isDir: false})
	files = append(files, &mockFileInfo{name: "data/wal-0000000000000008", isDir: false})
	files = append(files, &mockFileInfo{name: "data/snapshot-0000000000000001", isDir: false})
	files = append(files, &mockFileInfo{name: "data/snapshot-0000000000000010", isDir: false})
	files = append(files, &mockFileInfo{name: "data/placeholder-0000000000000010", isDir: false})
	files = append(files, &mockFileInfo{name: "data/placeholder", isDir: true})
	files = append(files, &mockFileInfo{name: "data/wal-0000000000000004", isDir: false})
	files = append(files, &mockFileInfo{name: "data/wal-0000000000000005", isDir: false})
	files = append(files, &mockFileInfo{name: "data/.gitignore", isDir: false})
	files = append(files, &mockFileInfo{name: "data/wal-0000000000000009", isDir: false})
	files = append(files, &mockFileInfo{name: "data/wal-0000000000000011", isDir: false})
	files = append(files, &mockFileInfo{name: "data/wal-0000000000000010", isDir: false})
	files = append(files, &mockFileInfo{name: "data/.dockerignore", isDir: false})
	files = append(files, &mockFileInfo{name: "data/wal-XXXXX", isDir: true})
	files = append(files, &mockFileInfo{name: "data/wal-0000000000000002", isDir: false})
	files = append(files, &mockFileInfo{name: "data/wal-0000000000000007", isDir: false})
	files = append(files, &mockFileInfo{name: "data/somethingelse", isDir: false})

	snapshotFiles := sortedListOfItemInDirMatchingARegex(files, "snapshot", "data/snapshot-0000000000000011")
	if len(snapshotFiles) != 2 {
		log.Fatalf("should return 2 snapshot files, but got: %d", len(snapshotFiles))
	}
	if snapshotFiles[1] != "data/snapshot-0000000000000010" {
		log.Fatalf("snapshotFiles should be sorted ascending, and we got: \n%v", snapshotFiles)
	}

	walFiles := sortedListOfItemInDirMatchingARegex(files, "wal", "data/wal-0000000000000005")
	if len(walFiles) != 3 {
		log.Fatalf("should return 3 wal files, but got: %d", len(walFiles))
	}
	if walFiles[1] != "data/wal-0000000000000003" {
		log.Fatalf("walFiles should be sorted ascending, and we got: \n%v", walFiles)
	}
}
