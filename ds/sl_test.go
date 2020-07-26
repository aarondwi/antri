package ds

import (
	"log"
	"math/rand"
	"testing"
	"time"
)

func TestSkipListInsertAndPop(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	sl := &Sl{}

	afterTheFirstSec := time.Now().Unix()
	time.Sleep(1 * time.Second)

	currentTime := time.Now().Unix()
	sl.Insert(currentTime, "key1")
	sl.Insert(currentTime, "key2")
	sl.Insert(currentTime, "key3")
	time.Sleep(1 * time.Second)

	sl.Insert(afterTheFirstSec, "in_the_middle")

	currentTime = time.Now().Unix()
	sl.Insert(currentTime, "key4")
	sl.Insert(currentTime, "key5")

	result := sl.Pop()
	if result.Key != "in_the_middle" {
		log.Fatalf("Ordering failed, should be `in_the_middle`, but got %s", result.Key)
	}

	result = sl.Pop()
	if result.Key != "key1" {
		log.Fatalf("Ordering failed, should be `key1`, but got %s", result.Key)
	}
	result = sl.Pop()
	if result.Key != "key2" {
		log.Fatalf("Ordering failed, should be `key2`, but got %s", result.Key)
	}
	result = sl.Pop()
	if result.Key != "key3" {
		log.Fatalf("Ordering failed, should be `key3`, but got %s", result.Key)
	}
	result = sl.Pop()
	if result.Key != "key4" {
		log.Fatalf("Ordering failed, should be `key4`, but got %s", result.Key)
	}
	result = sl.Pop()
	if result.Key != "key5" {
		log.Fatalf("Ordering failed, should be `key5`")
	}
}

func TestPopEmptySkipList(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	sl := &Sl{}
	result := sl.Pop()
	if result != nil {
		log.Fatalf("Should be nil, but got %v", result)
	}

	result = sl.findExact(time.Now().Unix(), "nothing")
	if result != nil {
		log.Fatalf("Should be nil, but got %v", result)
	}
}

func TestSkipListDelete(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	sl := &Sl{}

	afterTheFirstSec := time.Now().Unix()
	time.Sleep(1 * time.Second)

	currentTime := time.Now().Unix()
	sl.Insert(currentTime, "key1")
	sl.Insert(currentTime, "key2")
	sl.Insert(currentTime, "key3")
	time.Sleep(1 * time.Second)
	oldTime := currentTime

	sl.Insert(afterTheFirstSec, "in_the_middle")

	currentTime = time.Now().Unix()
	sl.Insert(currentTime, "key4")
	sl.Insert(currentTime, "key5")

	sl.Delete(afterTheFirstSec, "in_the_middle")
	sl.Delete(oldTime, "not_found")
	sl.Delete(oldTime, "key2")
	sl.Delete(currentTime, "key5")

	result := sl.Pop()
	if result.Key != "key1" {
		log.Fatalf("Ordering failed, should be `key1`, but got %s", result.Key)
	}
	result = sl.Pop()
	if result.Key != "key3" {
		log.Fatalf("Ordering failed, should be `key3`, but got %s", result.Key)
	}
	result = sl.Pop()
	if result.Key != "key4" {
		log.Fatalf("Ordering failed, should be `key4`, but got %s", result.Key)
	}
}
