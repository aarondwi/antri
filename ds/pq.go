package ds

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"log"
)

// PqItem is our task object
//
// This implementation is NOT thread-safe
type PqItem struct {
	ScheduledAt int64  `json:"scheduledAt"`
	Key         string `json:"key"`
	Value       string `json:"value"`
	Retries     int16  `json:"retries"`
}

// Pq is our main priority queue implementation
// the sort order is determined by ScheduledAt
// with smaller value returned earlier
//
// This implementation is NOT thread-safe
type Pq struct {
	heapArray []*PqItem
	size      int
	maxsize   int
}

// NewPq setups our priorityqueue with the config
func NewPq(maxsize int) *Pq {
	maxheap := &Pq{
		heapArray: []*PqItem{},
		size:      0,
		maxsize:   maxsize,
	}
	return maxheap
}

// HeapSize returns our priorityqueue size
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

// Insert an item into the priorityqueue
// and reorder its internal
//
// in theory, if the later work always scheduled earlier
// this gonna be bit slower, cause lots of swapping (log2(m.HeapSize()))
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
	rightChildIndex := m.rightchild(current)
	//If current is smallest then return
	if leftChildIndex < m.size && m.greater(leftChildIndex, largest) {
		largest = leftChildIndex
	}
	if rightChildIndex < m.size && m.greater(rightChildIndex, largest) {
		largest = rightChildIndex
	}
	if largest != current {
		m.swap(current, largest)
		m.downHeapify(largest)
	}
}

// Pop returns one item from the priorityqueue
// and removing it
func (m *Pq) Pop() *PqItem {
	top := m.heapArray[0]
	m.heapArray[0] = m.heapArray[m.size-1]
	m.heapArray = m.heapArray[:(m.size)-1]
	m.size--
	m.downHeapify(0)
	return top
}

// Peek returns one item from the priorityqueue
// but not removing it
func (m *Pq) Peek() *PqItem {
	if m.HeapSize() > 0 {
		return m.heapArray[0]
	}
	return nil
}

// WritePqItemToLog format PqItem into binary data to write into storage.
// we separate it from Insert so we can use it on concurrently with main lock.
// Designed as a method because gonna be using sync.Pool later.
//
// our log format => 20 bytes metadata + key length + value length
//
// scheduledAt int64 -> 8 bytes
//
// length of key int32 -> 4 bytes
//
// length of value int32 -> 4 bytes
//
// number of retries int16 -> 2 bytes
//
// key string/[]byte
//
// value string/[]byte
func (m *Pq) WritePqItemToLog(w io.Writer, pi *PqItem) bool {
	buf := make([]byte, 18)

	binary.LittleEndian.PutUint64(buf, uint64(pi.ScheduledAt))
	binary.LittleEndian.PutUint32(buf[8:], uint32(len(pi.Key)))
	binary.LittleEndian.PutUint32(buf[12:], uint32(len(pi.Value)))
	binary.LittleEndian.PutUint16(buf[16:], uint16(pi.Retries))
	buf = append(buf, pi.Key...)
	buf = append(buf, pi.Value...)

	_, err := w.Write(buf)
	if err != nil {
		return false
	}
	return true
}

// ReadPqItemFromLog reads log into a *PqItem.
// Designed as a method because gonna be using sync.Pool later.
func (m *Pq) ReadPqItemFromLog(r io.Reader) *PqItem {
	int16holder := make([]byte, 2)
	int32holder := make([]byte, 4)
	int64holder := make([]byte, 8)

	n, err := r.Read(int64holder)
	if n == 0 || err != nil {
		log.Printf("failed parsing scheduledAt: %d", n)
		return nil
	}
	scheduledAtRead := binary.LittleEndian.Uint64(int64holder)

	n, err = r.Read(int32holder)
	if n == 0 || err != nil {
		log.Printf("failed parsing lengthOfKey: %d", n)
		return nil
	}
	lengthOfKeyRead := binary.LittleEndian.Uint32(int32holder)

	n, err = r.Read(int32holder)
	if n == 0 || err != nil {
		log.Printf("failed parsing lengthOfValue: %d", n)
		return nil
	}
	lengthOfValueRead := binary.LittleEndian.Uint32(int32holder)

	n, err = r.Read(int16holder)
	if n == 0 || err != nil {
		log.Printf("failed parsing numOfRetries: %d", n)
		return nil
	}
	numOfRetriesRead := binary.LittleEndian.Uint16(int16holder)

	keyBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfKeyRead)))
	if err != nil {
		log.Print(err)
		return nil
	}

	valueBytes, err := ioutil.ReadAll(io.LimitReader(r, int64(lengthOfValueRead)))
	if err != nil {
		log.Print(err)
		return nil
	}

	return &PqItem{
		ScheduledAt: int64(scheduledAtRead),
		Key:         string(keyBytes),
		Value:       string(valueBytes),
		Retries:     int16(numOfRetriesRead),
	}
}
