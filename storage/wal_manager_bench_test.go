package storage

import (
	"log"
	"os"
	"testing"
	"time"
)

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
