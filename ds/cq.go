package ds

// CircularQueue implements bounded-size int64 queue.
// This implementation is NOT goroutine-safe
type CircularQueue struct {
	arr         []int64
	maxSize     int
	currentSize int
	head        int
	tail        int
}

// NewCircularQueue creates a CircularQueue with size n
func NewCircularQueue(n int) *CircularQueue {
	return &CircularQueue{
		arr:         make([]int64, n),
		maxSize:     n,
		currentSize: 0,
		head:        0,
		tail:        0,
	}
}

// Push n into circularQueue.
// Returns whether the push success or not (because of full)
func (c *CircularQueue) Push(n int64) bool {
	if c.IsFull() {
		return false
	}

	c.arr[c.head] = n
	c.head = c.getNextIndex(c.head)
	c.currentSize++
	return true
}

// Pop one item from circularQueue.
// Also returns whether it returns a dummy or not
func (c *CircularQueue) Pop() (int64, bool) {
	if c.IsEmpty() {
		return 0, false
	}
	result := c.arr[c.tail]
	c.tail = c.getNextIndex(c.tail)
	c.currentSize--
	return result, true
}

func (c *CircularQueue) getNextIndex(index int) int {
	if index == c.maxSize-1 {
		return 0
	}
	return index + 1
}

// IsFull checks whether circularQueue has no remaining slots
func (c *CircularQueue) IsFull() bool {
	return c.currentSize == c.maxSize
}

// IsEmpty checks whether circularQueue has remaining slots
func (c *CircularQueue) IsEmpty() bool {
	return c.currentSize == 0
}
