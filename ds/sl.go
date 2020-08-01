package ds

import (
	"math/rand"
	"strings"
)

var (
	skiplistHeight = 4
	probability    = 0.25
)

// SlItem represents our skiplist node
//
// This implementation is NOT thread-safe
type SlItem struct {
	Key    string
	level  int
	after  []*SlItem
	before []*SlItem
}

// Sl is our skiplist implementation
// This API does not implement batch retrieval
// rather, more like skiplist-based priority queue
//
// This implementation is NOT thread-safe
type Sl struct {
	head *SlItem
}

// find searches the position of the node just before the scheduledAt
// used when inserting
func (sl *Sl) findUntilLevel(key string, targetLevel int) *SlItem {
	if sl.head == nil {
		return nil
	}
	current := sl.head

	if strings.Compare(key, sl.head.Key) < 0 {
		return nil
	}
	for i := skiplistHeight - 1; i >= targetLevel; i-- {
		for current.after[i] != nil && strings.Compare(key, current.after[i].Key) > 0 {
			current = current.after[i]
		}
	}
	return current
}

// find is a wrapper for findUntilLevel 0
func (sl *Sl) find(key string) *SlItem {
	return sl.findUntilLevel(key, 0)
}

// Insert our SlItem to the skiplist
// at the same time, also manage all the pointer
func (sl *Sl) Insert(key string) {
	newNode := &SlItem{
		Key:    key,
		after:  make([]*SlItem, 4),
		before: make([]*SlItem, 4),
	}
	current := sl.find(key)

	if current == nil && sl.head == nil {
		// be the head, and the only node in the skiplist
		sl.head = newNode
	} else if current == nil && sl.head != nil { // meaning gonna be new head
		sl.head.before[0] = newNode
		newNode.after[0] = sl.head
		sl.head = newNode
	} else {
		// not gonna be a head
		// do connect for level 0 first
		after := current.after[0]
		if after != nil {
			after.before[0] = newNode
		}
		current.after[0] = newNode
		newNode.before[0] = current
		newNode.after[0] = after

		currentLevel := 1
		var newplace *SlItem
		for {
			rng := rand.Float64()
			if currentLevel == skiplistHeight {
				break
			}
			if rng < probability {
				newplace = sl.findUntilLevel(key, currentLevel)
				if newplace.after[currentLevel] != nil {
					newplace.after[currentLevel].before[currentLevel] = newNode
					newNode.after[currentLevel] = newplace
				}
				newplace.after[currentLevel] = newNode
				newNode.before[currentLevel] = newplace
				currentLevel++
			} else {
				break
			}
		}
	}
}

// findExact searches the position of the node with the same key and scheduledAt
// used when deleting
func (sl *Sl) findExact(key string) *SlItem {
	if sl.head == nil {
		return nil
	}
	current := sl.head

	found := false
	for i := skiplistHeight - 1; i >= 0; i-- {
		for {
			if current.Key == key {
				found = true
				break
			}
			if current.after[i] != nil &&
				strings.Compare(key, current.after[i].Key) >= 0 {
				current = current.after[i]
			} else {
				break
			}
		}
		if found {
			return current
		}
	}
	return nil
}

// Delete our SlItem to the skiplist
// at the same time, also manage all the pointer
func (sl *Sl) Delete(key string) {
	node := sl.findExact(key)
	if node != nil {
		if node == sl.head {
			// we separate the case when the deleted is head
			// because we need to reassign the head position
			sl.head = sl.head.after[0]
			for i := skiplistHeight - 1; i > 0; i-- {
				sl.head.before[i] = nil
			}
		} else {
			for i := skiplistHeight - 1; i >= 0; i-- {
				after := node.after[i]
				before := node.before[i]

				if after != nil && before != nil {
					after.before[i] = before
					before.after[i] = after
				} else if before != nil {
					before.after[i] = nil
				} else if after != nil {
					after.before[i] = nil
				}
				// both nil
				// no need to do anything
			}
		}
	}
}

// Pop the earliest item in the skiplist, in our case, the most front
func (sl *Sl) Pop() (string, bool) {
	if sl.head == nil { // no item
		return "", false
	}
	previousHead := sl.head

	// last one item
	if sl.head.after[0] == nil {
		sl.head = nil
		return previousHead.Key, true
	}

	// the next one in level 0 gonna be the head
	sl.head = sl.head.after[0]

	// all the `before` of the previousHead `after` pointer
	// points to the new head
	for i := skiplistHeight - 1; i > 0; i-- {
		if previousHead.after[i] != nil {
			previousHead.after[i].before[i] = sl.head
		}
	}

	// current head doesn't need before
	for i := skiplistHeight - 1; i > 0; i-- {
		sl.head.before[i] = nil
	}

	return previousHead.Key, true
}
