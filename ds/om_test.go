package ds

import (
	"log"
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
	if item.Key != "Second" {
		log.Fatalf("`key2` should map to `Second`, but got %v", item)
	}
	om.Delete("key2")
	item, ok = om.Pop()
	if !ok {
		log.Fatalf("item `key3` should still be in orderedmap, but is not")
	}
	if item.Key != "Third" {
		log.Fatalf("`key3` should map to `Third`, but got %s", item.Key)
	}

	l := om.Length()
	if l != 0 {
		log.Fatalf("now, the orderedmap should be empty, but it is still has %d items", l)
	}
}
