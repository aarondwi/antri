package ds

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

func TestOrderedMap(t *testing.T) {
	om := NewOrderedMap(10)

	om.PeekExpire()
	om.Pop()
	om.Delete("notakey")

	om.Insert("key1", &PqItem{Key: "First"})
	om.Insert("key2", &PqItem{Key: "Second"})
	om.Insert("key3", &PqItem{Key: "Third"})

	result := om.PeekExpire()
	if result != time.Now().Unix()+10 {
		log.Fatalf("Should expire in 10s, but got %d", result)
	}

	om.Delete("key1")
	_, ok := om.Get("key1")
	if ok {
		log.Fatalf("Should not be `ok`, but it is")
	}
	item, ok := om.Get("key2")
	if !ok {
		log.Fatalf("Should be `ok`, but it is not")
	}
	if string(item.Key) != "Second" {
		log.Fatalf("`key2` should map to `Second`, but got %v", item)
	}
	om.Delete("key2")
	item, ok = om.Pop()
	if !ok {
		log.Fatalf("item `key3` should still be in orderedmap, but is not")
	}
	if string(item.Key) != "Third" {
		log.Fatalf("`key3` should map to `Third`, but got %s", item.Key)
	}

	l := om.Length()
	if l != 0 {
		log.Fatalf("now, the orderedmap should be empty, but it is still has %d items", l)
	}
}

func BenchmarkOrderedMap(b *testing.B) {
	om := NewOrderedMap(10)
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
		om.Insert(string(content), item)
		om.Delete(string(content))
		pool.Put(item)
	}
}
