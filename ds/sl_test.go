package ds

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"testing"
	"time"
)

func TestSkipListInsertAndPop(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	sl := &Sl{}

	afterTheFirstSec := time.Now().Unix()
	time.Sleep(1 * time.Second)

	currentTime := time.Now().Unix()
	sl.Insert(fmt.Sprintf("%d_%s", currentTime, "key1"))
	sl.Insert(fmt.Sprintf("%d_%s", currentTime, "key2"))
	sl.Insert(fmt.Sprintf("%d_%s", currentTime, "key3"))
	time.Sleep(1 * time.Second)

	sl.Insert(fmt.Sprintf("%d_%s", afterTheFirstSec, "in_the_middle"))

	currentTime = time.Now().Unix()
	sl.Insert(fmt.Sprintf("%d_%s", currentTime, "key4"))
	sl.Insert(fmt.Sprintf("%d_%s", currentTime, "key5"))

	result, ok := sl.Pop()
	if !ok || !strings.Contains(result, "in_the_middle") {
		log.Fatalf("Ordering failed, should contain `in_the_middle`, but got %s", result)
	}

	result, ok = sl.Pop()
	if !ok || !strings.Contains(result, "key1") {
		log.Fatalf("Ordering failed, should contain `key1`, but got %s", result)
	}
	result, ok = sl.Pop()
	if !ok || !strings.Contains(result, "key2") {
		log.Fatalf("Ordering failed, should contain `key2`, but got %s", result)
	}
	result, ok = sl.Pop()
	if !ok || !strings.Contains(result, "key3") {
		log.Fatalf("Ordering failed, should contain `key3`, but got %s", result)
	}
	result, ok = sl.Pop()
	if !ok || !strings.Contains(result, "key4") {
		log.Fatalf("Ordering failed, should contain `key4`, but got %s", result)
	}
	result, ok = sl.Pop()
	if !ok || !strings.Contains(result, "key5") {
		log.Fatalf("Ordering failed, should contain `key5`")
	}
}

func TestPopEmptySkipList(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	sl := &Sl{}
	result, ok := sl.Pop()
	if ok {
		log.Fatalf("Should not be found, but it is, and got %v", result)
	}

	node := sl.findExact("nothing")
	if node != nil {
		log.Fatalf("Should be nil, but got %v", node)
	}
}

func TestSkipListDelete(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	sl := &Sl{}

	afterTheFirstSec := time.Now().Unix()
	time.Sleep(1 * time.Second)

	currentTime := time.Now().Unix()
	sl.Insert(fmt.Sprintf("%d_%s", currentTime, "key1"))
	sl.Insert(fmt.Sprintf("%d_%s", currentTime, "key2"))
	sl.Insert(fmt.Sprintf("%d_%s", currentTime, "key3"))
	time.Sleep(1 * time.Second)
	oldTime := currentTime

	sl.Insert(fmt.Sprintf("%d_%s", afterTheFirstSec, "in_the_middle"))

	currentTime = time.Now().Unix()
	sl.Insert(fmt.Sprintf("%d_%s", currentTime, "key4"))
	sl.Insert(fmt.Sprintf("%d_%s", currentTime, "key5"))

	sl.Delete(fmt.Sprintf("%d_%s", afterTheFirstSec, "in_the_middle"))
	sl.Delete(fmt.Sprintf("%d_%s", oldTime, "notfound"))
	sl.Delete(fmt.Sprintf("%d_%s", oldTime, "key2"))
	sl.Delete(fmt.Sprintf("%d_%s", currentTime, "key5"))

	result, ok := sl.Pop()
	if !ok || !strings.Contains(result, "key1") {
		log.Fatalf("Ordering failed, should contain `key1`, but got %s", result)
	}
	result, ok = sl.Pop()
	if !ok || !strings.Contains(result, "key3") {
		log.Fatalf("Ordering failed, should contain `key3`, but got %s", result)
	}
	result, ok = sl.Pop()
	if !ok || !strings.Contains(result, "key4") {
		log.Fatalf("Ordering failed, should contain `key4`, but got %s", result)
	}
}
