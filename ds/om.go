package ds

import (
	"math"
	"time"
)

type orderedMapItem struct {
	expireOn int64
	Key      string
}

// OrderedMap is a map with a slice combined into a single api
// used as a priority queue with fast lookup API
//
// we can use slice/array internally
// because the expected inflight retrived task should be small
// and the task tracker size is relatively small
// using an array would reduce unnecessary cache miss
// from a skiplist/b-tree
//
// this implementation is NOT goroutine-safe
type OrderedMap struct {
	kv              map[string]*PqItem
	arr             []orderedMapItem
	secondsToExpire int64
	len             int
}

// NewOrderedMap returns a default orderedMap API
func NewOrderedMap(secondsToExpire int64) *OrderedMap {
	return &OrderedMap{
		kv: make(map[string]*PqItem),
		// buffer allocation a bit
		arr:             make([]orderedMapItem, 0, 1000),
		secondsToExpire: secondsToExpire,
	}
}

// Insert the item into the orderedmap
// also tracks with its expire time
// each item will be expired `secondsToExpire` from insertion time
func (op *OrderedMap) Insert(key string, item *PqItem) {
	itemTracker := orderedMapItem{
		Key:      key,
		expireOn: time.Now().Unix() + op.secondsToExpire,
	}

	op.kv[key] = item
	op.arr = append(op.arr, itemTracker)
	op.len++
}

// Delete the key from the orderedmap
// also remove it from array by scanning it
func (op *OrderedMap) Delete(key string) {
	delete(op.kv, key)
	pos := -1
	for it, item := range op.arr {
		if item.Key == key {
			pos = it
			break
		}
	}
	if pos != -1 {
		op.arr = append(op.arr[:pos], op.arr[pos+1:]...)
		op.len--
	}
}

// Get the item from the orderedmap
func (op *OrderedMap) Get(key string) (*PqItem, bool) {
	res, ok := op.kv[key]
	return res, ok
}

// PeekExpire an item from the frontmost in arr
// without removing it (if any)
func (op *OrderedMap) PeekExpire() int64 {
	if op.len > 0 {
		return op.arr[0].expireOn
	}
	return math.MaxInt64
}

// Pop an item from the frontmost in arr
// also remove it from the kv (if any)
func (op *OrderedMap) Pop() (*PqItem, bool) {
	if op.len > 0 {
		result := op.kv[op.arr[0].Key]
		delete(op.kv, op.arr[0].Key)
		op.arr = op.arr[1:]
		op.len--
		return result, true
	}
	return nil, false
}

// Length / number of items in the orderedmap
func (op *OrderedMap) Length() int {
	return op.len
}
