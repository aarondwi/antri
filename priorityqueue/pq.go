package priorityqueue

type PqItem struct {
	ScheduledAt int64
	Key         []byte
	Value       []byte
}

// Pq is our main priority queue implementation
// the sort order is determined by scheduledAt
// with smaller value returned earlier
type Pq struct {
	heapArray []*PqItem
	size      int
	maxsize   int
}

func NewPq(maxsize int) *Pq {
	maxheap := &Pq{
		heapArray: []*PqItem{},
		size:      0,
		maxsize:   maxsize,
	}
	return maxheap
}

func (m *Pq) HeapSize() int {
	return m.size
}

func (m *Pq) leaf(index int) bool {
	return (index >= (m.size/2) && index <= m.size)
}

func (m *Pq) parent(index int) int {
	return (index - 1) / 2
}

func (m *Pq) leftchild(index int) int {
	return 2*index + 1
}

func (m *Pq) rightchild(index int) int {
	return 2*index + 2
}

func (m *Pq) Insert(item *PqItem) error {
	m.heapArray = append(m.heapArray, item)
	m.size++
	m.upHeapify(m.size - 1)
	return nil
}

func (m *Pq) swap(first, second int) {
	temp := m.heapArray[first]
	m.heapArray[first] = m.heapArray[second]
	m.heapArray[second] = temp
}

func (m *Pq) greater(first, second int) bool {
	return m.heapArray[first].ScheduledAt < m.heapArray[second].ScheduledAt
}

func (m *Pq) upHeapify(index int) {
	for m.greater(index, m.parent(index)) {
		m.swap(index, m.parent(index))
		index = m.parent(index)
	}
}

func (m *Pq) downHeapify(current int) {
	if m.leaf(current) {
		return
	}
	largest := current
	leftChildIndex := m.leftchild(current)
	rightRightIndex := m.rightchild(current)
	//If current is smallest then return
	if leftChildIndex < m.size && m.greater(leftChildIndex, largest) {
		largest = leftChildIndex
	}
	if rightRightIndex < m.size && m.greater(rightRightIndex, largest) {
		largest = rightRightIndex
	}
	if largest != current {
		m.swap(current, largest)
		m.downHeapify(largest)
	}
}

func (m *Pq) Pop() *PqItem {
	top := m.heapArray[0]
	m.heapArray[0] = m.heapArray[m.size-1]
	m.heapArray = m.heapArray[:(m.size)-1]
	m.size--
	m.downHeapify(0)
	return top
}

func (m *Pq) Peek() *PqItem {
	if m.HeapSize() > 0 {
		return m.heapArray[0]
	}
	return nil
}
