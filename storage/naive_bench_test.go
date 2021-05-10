package storage

import (
	"log"
	"os"
	"sync"
	"testing"
)

func Benchmark_Append_1KB_Parallel128_NoSync(b *testing.B) {
	os.Remove("test")

	mu := sync.Mutex{}
	f, err := os.OpenFile("test", os.O_CREATE|os.O_RDWR|os.O_APPEND, WAL_FILE_MODE)
	defer f.Close()
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}

	buf := randStringBytes(1 * 1024)
	b.SetParallelism(128)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			f.Write(buf)
			mu.Unlock()
		}
	})
}

func Benchmark_Append_1KB_NoSync(b *testing.B) {
	os.Remove("test")

	f, err := os.OpenFile("test", os.O_CREATE|os.O_RDWR|os.O_APPEND, WAL_FILE_MODE)
	defer f.Close()
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}

	buf := randStringBytes(1 * 1024)
	for i := 0; i < b.N; i++ {
		f.Write(buf)
	}
}

func Benchmark_Append_16KB_Parallel128_NoSync(b *testing.B) {
	os.Remove("test")

	mu := sync.Mutex{}
	f, err := os.OpenFile("test", os.O_CREATE|os.O_RDWR|os.O_APPEND, WAL_FILE_MODE)
	defer f.Close()
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}

	buf := randStringBytes(16 * 1024)
	b.SetParallelism(128)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			f.Write(buf)
			mu.Unlock()
		}
	})
}

func Benchmark_Append_16KB_NoSync(b *testing.B) {
	os.Remove("test")

	f, err := os.OpenFile("test", os.O_CREATE|os.O_RDWR|os.O_APPEND, WAL_FILE_MODE)
	defer f.Close()
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}

	buf := randStringBytes(16 * 1024)
	for i := 0; i < b.N; i++ {
		f.Write(buf)
	}
}

func Benchmark_Append_1KB_Parallel128_Sync(b *testing.B) {
	os.Remove("test")

	mu := sync.Mutex{}
	f, err := os.OpenFile("test", os.O_CREATE|os.O_RDWR|os.O_APPEND, WAL_FILE_MODE)
	defer f.Close()
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}

	buf := randStringBytes(1 * 1024)
	b.SetParallelism(128)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			f.Write(buf)
			f.Sync()
			mu.Unlock()
		}
	})
}

func Benchmark_Append_1KB_Sync(b *testing.B) {
	os.Remove("test")

	f, err := os.OpenFile("test", os.O_CREATE|os.O_RDWR|os.O_APPEND, WAL_FILE_MODE)
	defer f.Close()
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}

	buf := randStringBytes(1 * 1024)
	for i := 0; i < b.N; i++ {
		f.Write(buf)
		f.Sync()
	}
}

func Benchmark_Append_16KB_Parallel128_Sync(b *testing.B) {
	os.Remove("test")

	mu := sync.Mutex{}
	f, err := os.OpenFile("test", os.O_CREATE|os.O_RDWR|os.O_APPEND, WAL_FILE_MODE)
	defer f.Close()
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}

	buf := randStringBytes(16 * 1024)
	b.SetParallelism(128)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			f.Write(buf)
			f.Sync()
			mu.Unlock()
		}
	})
}

func Benchmark_Append_16KB_Sync(b *testing.B) {
	os.Remove("test")

	f, err := os.OpenFile("test", os.O_CREATE|os.O_RDWR|os.O_APPEND, WAL_FILE_MODE)
	defer f.Close()
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}

	buf := randStringBytes(16 * 1024)
	for i := 0; i < b.N; i++ {
		f.Write(buf)
		f.Sync()
	}
}

func Benchmark_Append_1KB_SyncPer100MB(b *testing.B) {
	os.Remove("test")

	f, err := os.OpenFile("test", os.O_CREATE|os.O_RDWR|os.O_APPEND, WAL_FILE_MODE)
	defer f.Close()
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}

	buf := randStringBytes(1 * 1024)
	for i := 0; i < b.N; i++ {
		f.Write(buf)
		if i%(1024*100) == 0 { // 1KB * 1024 * 100
			f.Sync()
		}
	}
}

func Benchmark_Append_16KB_SyncPer100MB(b *testing.B) {
	os.Remove("test")

	f, err := os.OpenFile("test", os.O_CREATE|os.O_RDWR|os.O_APPEND, WAL_FILE_MODE)
	defer f.Close()
	if err != nil {
		log.Fatalf("It should not return error, but we got %v", err)
	}

	buf := randStringBytes(16 * 1024)
	for i := 0; i < b.N; i++ {
		f.Write(buf)
		if i%(64*100) == 0 { // 16KB * 64 * 100
			f.Sync()
		}
	}
}
