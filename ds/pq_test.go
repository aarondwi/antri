package ds

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPq(t *testing.T) {
	pq := NewPriorityQueue()

	absoluteFirstItem := &PqItem{
		ScheduledAt: time.Now().Unix(),
		Key:         "absoluteFirst",
		Value:       []byte("absoluteFirst"),
		Retries:     0}

	res := pq.Peek()
	if res != nil {
		t.Fatalf("Should be nil, but it is not")
	}

	time.Sleep(1 * time.Second)
	pq.Insert(&PqItem{
		ScheduledAt: time.Now().Unix(),
		Key:         "first",
		Value:       []byte("first"),
		Retries:     0})
	pq.Insert(&PqItem{
		ScheduledAt: time.Now().Unix(),
		Key:         "second",
		Value:       []byte("second"),
		Retries:     0})

	res = pq.Peek()
	if string(res.Key) != "first" {
		t.Fatalf("Expected peek: \"first\", got %s", res.Key)
	}
	time.Sleep(1 * time.Second)
	pq.Insert(&PqItem{
		ScheduledAt: time.Now().Unix(),
		Key:         "third",
		Value:       []byte("third"),
		Retries:     0})
	time.Sleep(1 * time.Second)
	pq.Insert(&PqItem{
		ScheduledAt: time.Now().Unix(),
		Key:         "fourth",
		Value:       []byte("fourth"),
		Retries:     0})

	if pq.HeapSize() != 4 {
		t.Fatalf("Expected Size: 4, got %d", pq.HeapSize())
	}

	pq.Insert(absoluteFirstItem)
	temporary := pq.Pop().Value
	if !bytes.Equal(temporary, absoluteFirstItem.Value) {
		t.Fatalf("Expected %s, got %s", absoluteFirstItem.Value, temporary)
	}
	time.Sleep(1 * time.Second)
	pq.Insert(&PqItem{
		ScheduledAt: time.Now().Unix(),
		Key:         "fifth",
		Value:       []byte("fifth"),
		Retries:     0})

	// these 2 are concurrent, either ordering is valid
	pq.Pop()
	pq.Pop()

	temporary = pq.Pop().Value
	if string(temporary) != "third" {
		t.Fatalf("Expected %s, got %s", "third", temporary)
	}

	temporary = pq.Pop().Value
	if string(temporary) != "fourth" {
		t.Fatalf("Expected %s, got %s", "fourth", temporary)
	}

	temporary = pq.Pop().Value
	if string(temporary) != "fifth" {
		t.Fatalf("Expected %s, got %s", "fifth", temporary)
	}
}

func BenchmarkPq(b *testing.B) {
	pq := NewPriorityQueue()
	pool := sync.Pool{
		New: func() interface{} { return new(PqItem) },
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item := pool.Get().(*PqItem)
		b.StopTimer()
		content := fmt.Sprintf("key_%d", i+1)
		b.StartTimer()
		item.Key = content
		item.ScheduledAt = time.Now().Unix()
		pq.Insert(item)
		pq.Pop()
		pool.Put(item)
	}
}
