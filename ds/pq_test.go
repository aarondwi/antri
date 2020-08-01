package ds

import (
	"testing"
	"time"
)

func TestPq(t *testing.T) {
	pq := NewPq(10)

	absoluteFirstItem := &PqItem{
		ScheduledAt: time.Now().Unix(),
		Key:         "absoluteFirst",
		Value:       "absoluteFirst",
		Retries:     0}

	res := pq.Peek()
	if res != nil {
		t.Fatalf("Should be nil, but it is not")
	}

	time.Sleep(1 * time.Second)
	pq.Insert(&PqItem{
		ScheduledAt: time.Now().Unix(),
		Key:         "first",
		Value:       "first",
		Retries:     0})
	pq.Insert(&PqItem{
		ScheduledAt: time.Now().Unix(),
		Key:         "second",
		Value:       "second",
		Retries:     0})

	res = pq.Peek()
	if res.Key != "first" {
		t.Fatalf("Expected peek: \"first\", got %s", res.Key)
	}
	time.Sleep(1 * time.Second)
	pq.Insert(&PqItem{
		ScheduledAt: time.Now().Unix(),
		Key:         "third",
		Value:       "third",
		Retries:     0})
	time.Sleep(1 * time.Second)
	pq.Insert(&PqItem{
		ScheduledAt: time.Now().Unix(),
		Key:         "fourth",
		Value:       "fourth",
		Retries:     0})

	if pq.HeapSize() != 4 {
		t.Fatalf("Expected Size: 4, got %d", pq.HeapSize())
	}

	pq.Insert(absoluteFirstItem)
	temporary := pq.Pop().Value
	if temporary != absoluteFirstItem.Value {
		t.Fatalf("Expected %s, got %s", absoluteFirstItem.Value, temporary)
	}
	time.Sleep(1 * time.Second)
	pq.Insert(&PqItem{
		ScheduledAt: time.Now().Unix(),
		Key:         "fifth",
		Value:       "fifth",
		Retries:     0})

	// these 2 are concurrent, either ordering is valid
	pq.Pop()
	pq.Pop()

	temporary = pq.Pop().Value
	if temporary != "third" {
		t.Fatalf("Expected %s, got %s", "third", temporary)
	}

	temporary = pq.Pop().Value
	if temporary != "fourth" {
		t.Fatalf("Expected %s, got %s", "fourth", temporary)
	}

	temporary = pq.Pop().Value
	if temporary != "fifth" {
		t.Fatalf("Expected %s, got %s", "fifth", temporary)
	}
}
