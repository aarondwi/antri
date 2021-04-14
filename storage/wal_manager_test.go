package storage

import (
	"bytes"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

var WAL_TEST_DIR = "waltestdir"
var letterBytes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
var l = len(letterBytes)

func randStringBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(l)]
	}
	return b
}

func TestWalManagerRecordLoad(t *testing.T) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(500*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)

	buf := randStringBytes(1024)
	rh, err := wm.Record(buf)
	if err != nil {
		log.Fatalf("It should not error, because in range, have space, and not closed yet. But instead we got %v", err)
	}
	rh.GetFileNumber()

	wm.Close()
	_, err = wm.Record(buf)
	if err == nil || err != ErrWalManagerClosed {
		log.Fatal("It should return ErrWalManagerClosed, but it is not")
	}

	// should still be able to, even though already closed
	writtenBuf, err := wm.Load(1)
	if err != nil {
		log.Fatalf("It should not error, because the file is OK, but instead we got %v", err)
	}
	if len(writtenBuf) != 1 {
		log.Fatalf("It should be of length 1, because we only wrote once, but instead we got length %d", len(writtenBuf))
	}

	if !bytes.Equal(buf, writtenBuf[0]) {
		log.Fatalf("Should be the same, because we only wrote and directly read it, but instead they are\n %v \nand\n %v",
			buf, writtenBuf[0])
	}
}

func TestWalManagerCorruptedFile(t *testing.T) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(500*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)

	buf := randStringBytes(1024)
	rh, err := wm.Record(buf)
	if err != nil {
		log.Fatalf("It should not error, because in range, have space, and not closed yet. But instead we got %v", err)
	}
	rh.GetFileNumber()
	wm.Close()

	// corrupt the file manually
	f, err := os.OpenFile(
		wm.generateFilename(1),
		WAL_FILE_FLAG,
		WAL_FILE_MODE)
	f.Seek(rand.Int63n(1024), 0) // take random places
	f.Write([]byte{byte(rand.Int())})
	f.Sync()
	f.Close()

	_, err = wm.Load(1)
	if err == nil || err != ErrWalFileCorrupted {
		log.Fatal("It should error because we already corrupt it, but it is not")
	}
}

func TestWalManagerMoreThan1WalFile(t *testing.T) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		128*1024,
		time.Duration(100*time.Microsecond),
		false)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)

	numberOf64KBInA32MB := 32 * 1024 / 64
	sizeOf64KB := 64 * 1024
	bufArray := make([][]byte, 0, 1024)
	batchIDTracker := 1 // first batch starts with 1

	log.Println("Start writing to wal number 1")
	for i := 0; i < numberOf64KBInA32MB; i++ {
		buf := randStringBytes(sizeOf64KB - RECORD_METADATA_SIZE)
		rh, err := wm.Record(buf)
		if err != nil {
			log.Fatalf("Should not error, because will be mapped correctly, but instead we got %v", err)
		}

		if rh.GetBatchId() != uint64(batchIDTracker) {
			log.Fatalf("Some batch are skipped, expecting %d, received %d", batchIDTracker, rh.GetBatchId())
		}
		batchIDTracker++

		if rh.GetFileNumber() != 1 {
			log.Fatalf("It should still be 1, but we got %d", rh.GetFileNumber())
		}
		bufArray = append(bufArray, buf)
	}
	log.Println("Finish writing to wal number 1")

	log.Println("Start writing to wal number 2")
	for i := 0; i < numberOf64KBInA32MB; i++ {
		buf := randStringBytes(sizeOf64KB - RECORD_METADATA_SIZE)
		rh, err := wm.Record(buf)
		if err != nil {
			log.Fatalf("Should not error, because will mapped correctly, but instead we got %v", err)
		}

		if rh.GetBatchId() != uint64(batchIDTracker) {
			log.Fatalf("Some batch are skipped, expecting %d, received %d", batchIDTracker, rh.GetBatchId())
		}
		batchIDTracker++

		if rh.GetFileNumber() != 2 {
			log.Fatalf("It should now be 2, but we got %d", rh.GetFileNumber())
		}
		bufArray = append(bufArray, buf)
	}
	log.Println("Finish writing to wal number 2")

	resArray := make([][]byte, 0, 1024)
	res, err := wm.Load(1)
	if err != nil {
		log.Fatalf("It should not error, because all is matched, but instead we got %v", err)
	}
	resArray = append(resArray, res...)

	res2, err := wm.Load(2)
	if err != nil {
		log.Fatalf("It should not error, because all is matched, but instead we got %v", err)
	}
	resArray = append(resArray, res2...)

	if len(bufArray) != len(resArray) {
		log.Fatalf("It should be the same length for the array, but instead we got %d and %d",
			len(bufArray),
			len(resArray))
	}
	for i := 0; i < len(resArray); i++ {
		if len(bufArray[i]) != len(resArray[i]) {
			log.Fatalf("Should be the same length, but they are not, at iteration %d: expected %d, received %d",
				i,
				len(bufArray[i]),
				len(resArray[i]))
		}
		if !bytes.Equal(bufArray[i], resArray[i]) {
			log.Fatalf("Should be the same content, but they are not, at iteration %d", i)
		}
	}

	wm.Close()
}

func TestNewWalManagerValidation(t *testing.T) {
	os.RemoveAll(WAL_TEST_DIR)

	// itemSizeLimit < 0
	_, err := NewWalManager(
		WAL_TEST_DIR,
		-64*1024,
		256*1024,
		time.Duration(500*time.Microsecond),
		true,
	)
	if err == nil || err != ErrSizeLimitOutOfRange {
		log.Fatal("It should return error, because itemSizeLimit < 0, but it doesn't")
	}

	// itemSizeLimit > WAL_ITEM_SIZE_LIMIT
	_, err = NewWalManager(
		WAL_TEST_DIR,
		64*1024+1,
		512*1024,
		time.Duration(500*time.Microsecond),
		true,
	)
	if err == nil || err != ErrSizeLimitOutOfRange {
		log.Fatal("It should return error, because itemSizeLimit > WAL_ITEM_SIZE_LIMIT, but it doesn't")
	}

	// batchSize < WAL_BATCH_LOWER_LIMIT
	_, err = NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		128*1024-1,
		time.Duration(500*time.Microsecond),
		true,
	)
	if err == nil || err != ErrSizeLimitOutOfRange {
		log.Fatal("It should return error, because batchSize < WAL_BATCH_LOWER_LIMIT, but it doesn't")
	}

	// batchSize > WAL_BATCH_UPPER_LIMIT
	_, err = NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		4*1024*1024+1,
		time.Duration(500*time.Microsecond),
		true,
	)
	if err == nil || err != ErrSizeLimitOutOfRange {
		log.Fatal("It should return error, because batchSize < WAL_BATCH_LOWER_LIMIT, but it doesn't")
	}
}

func BenchmarkWalManager_SeqFsync_Batch256KB_Buf1KB_NoWait(b *testing.B) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(500*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)
	defer wm.Close()

	buf := randStringBytes(1024)

	for i := 0; i < b.N; i++ {
		_, err := wm.Record(buf)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkWalManager_SeqFsync_Batch256KB_Buf4KB_NoWait(b *testing.B) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(500*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)
	defer wm.Close()

	buf := randStringBytes(4 * 1024)

	for i := 0; i < b.N; i++ {
		_, err := wm.Record(buf)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkWalManager_SeqFsync_Batch256KB_Buf8KB_NoWait(b *testing.B) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(500*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)
	defer wm.Close()

	buf := randStringBytes(8 * 1024)

	for i := 0; i < b.N; i++ {
		_, err := wm.Record(buf)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkWalManager_SeqFsync_Batch256KB_Buf16KB_NoWait(b *testing.B) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(500*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)
	defer wm.Close()

	buf := randStringBytes(16 * 1024)

	for i := 0; i < b.N; i++ {
		_, err := wm.Record(buf)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkWalManager_SeqFsync_Batch256KB_Buf1KB_Wait(b *testing.B) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(50*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)
	defer wm.Close()

	buf := randStringBytes(1024)

	for i := 0; i < b.N; i++ {
		rh, err := wm.Record(buf)
		if err != nil {
			log.Fatal(err)
		}
		rh.GetFileNumber()
	}
}

func BenchmarkWalManager_SeqFsync_Batch256KB_Buf4KB_Wait(b *testing.B) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(50*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)
	defer wm.Close()

	buf := randStringBytes(4 * 1024)

	for i := 0; i < b.N; i++ {
		rh, err := wm.Record(buf)
		if err != nil {
			log.Fatal(err)
		}
		rh.GetFileNumber()
	}
}

func BenchmarkWalManager_SeqFsync_Batch256KB_Buf8KB_Wait(b *testing.B) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(50*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)
	defer wm.Close()

	buf := randStringBytes(8 * 1024)

	for i := 0; i < b.N; i++ {
		rh, err := wm.Record(buf)
		if err != nil {
			log.Fatal(err)
		}
		rh.GetFileNumber()
	}
}

func BenchmarkWalManager_SeqFsync_Batch256KB_Buf16KB_Wait(b *testing.B) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(50*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)
	defer wm.Close()

	buf := randStringBytes(16 * 1024)

	for i := 0; i < b.N; i++ {
		rh, err := wm.Record(buf)
		if err != nil {
			log.Fatal(err)
		}
		rh.GetFileNumber()
	}
}

func BenchmarkWalManager_Parallel256Fsync_Batch256KB_Buf1KB_Wait(b *testing.B) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(500*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)
	defer wm.Close()

	buf := randStringBytes(1024)

	b.SetParallelism(256)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rh, err := wm.Record(buf)
			if err != nil {
				log.Fatal(err)
			}
			rh.GetFileNumber()
		}
	})
}

func BenchmarkWalManager_Parallel256Fsync_Batch256KB_Buf4KB_Wait(b *testing.B) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(500*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)
	defer wm.Close()

	buf := randStringBytes(4 * 1024)

	b.SetParallelism(256)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rh, err := wm.Record(buf)
			if err != nil {
				log.Fatal(err)
			}
			rh.GetFileNumber()
		}
	})
}

func BenchmarkWalManager_Parallel256Fsync_Batch256KB_Buf8KB_Wait(b *testing.B) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(500*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)
	defer wm.Close()

	buf := randStringBytes(8 * 1024)

	b.SetParallelism(256)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rh, err := wm.Record(buf)
			if err != nil {
				log.Fatal(err)
			}
			rh.GetFileNumber()
		}
	})
}

func BenchmarkWalManager_Parallel256Fsync_Batch256KB_Buf16KB_Wait(b *testing.B) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(500*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)
	defer wm.Close()

	buf := randStringBytes(16 * 1024)

	b.SetParallelism(256)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rh, err := wm.Record(buf)
			if err != nil {
				log.Fatal(err)
			}
			rh.GetFileNumber()
		}
	})
}

func BenchmarkWalManager_Parallel128Fsync_Batch256KB_Buf1KB_Wait(b *testing.B) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(500*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)
	defer wm.Close()

	buf := randStringBytes(1024)

	b.SetParallelism(128)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rh, err := wm.Record(buf)
			if err != nil {
				log.Fatal(err)
			}
			rh.GetFileNumber()
		}
	})
}

func BenchmarkWalManager_Parallel128Fsync_Batch256KB_Buf4KB_Wait(b *testing.B) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(500*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)
	defer wm.Close()

	buf := randStringBytes(4 * 1024)

	b.SetParallelism(128)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rh, err := wm.Record(buf)
			if err != nil {
				log.Fatal(err)
			}
			rh.GetFileNumber()
		}
	})
}

func BenchmarkWalManager_Parallel128Fsync_Batch256KB_Buf8KB_Wait(b *testing.B) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(500*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)
	defer wm.Close()

	buf := randStringBytes(8 * 1024)

	b.SetParallelism(128)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rh, err := wm.Record(buf)
			if err != nil {
				log.Fatal(err)
			}
			rh.GetFileNumber()
		}
	})
}

func BenchmarkWalManager_Parallel128Fsync_Batch256KB_Buf16KB_Wait(b *testing.B) {
	os.RemoveAll(WAL_TEST_DIR)

	wm, err := NewWalManager(
		WAL_TEST_DIR,
		64*1024,
		256*1024,
		time.Duration(500*time.Microsecond),
		true)
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}
	wm.Run(1)
	defer wm.Close()

	buf := randStringBytes(16 * 1024)

	b.SetParallelism(128)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rh, err := wm.Record(buf)
			if err != nil {
				log.Fatal(err)
			}
			rh.GetFileNumber()
		}
	})
}
