package ds

import (
	"log"
	"testing"
)

func TestCircularQueue(t *testing.T) {
	cq := NewCircularQueue(5)

	// PHASE 1
	for i := 0; i < 4; i++ {
		ok := cq.Push(int64(i))
		if !ok {
			t.Fatalf("Phase 1: It should be ok because still slots left, but it is not")
		}
	}
	log.Println(cq)

	for i := 0; i < 2; i++ {
		res, ok := cq.Pop()
		if !ok {
			t.Fatalf("Phase 1: It should be ok because still has remaining item, but it is not")
		}
		if res != int64(i) {
			t.Fatalf("Phase 1: It should be the same, because same order, but instead we got %d and %d", res, int64(i))
		}
	}
	log.Println(cq)

	// PHASE 2
	for i := 0; i < 3; i++ {
		ok := cq.Push(int64(10 + i))
		if !ok {
			log.Println(cq)
			t.Fatalf("Phase 2: It should be ok because there are slots left, but it is not")
		}
	}

	ok := cq.Push(100)
	if ok {
		t.Fatalf("Phase 2: It should fail because no slots left, but it does")
	}

	// PHASE 3
	result := []int64{2, 3, 10, 11, 12}
	for i := 0; i < 5; i++ {
		res, ok := cq.Pop()
		if !ok {
			t.Fatalf("Phase 3: It should be ok because still has remaining items, but it is not")
		}
		if res != result[i] {
			t.Fatalf("Phase 3: It should be ok because still has remaining items, but it is not")
		}
	}

	_, ok = cq.Pop()
	if ok {
		t.Fatalf("Phase 3: It should fail because still no remaining items, but it is not")
	}
}

func BenchmarkCircularQueue(b *testing.B) {
	cq := NewCircularQueue(100)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			ok := cq.Push(int64(j))
			if !ok {
				b.Fatalf("On Push, should always be ok, but it is not, on iteration %d -> %d", i, j)
			}
		}
		for j := 0; j < 10; j++ {
			_, ok := cq.Pop()
			if !ok {
				b.Fatalf("On Pop, should always be ok, but it is not, on iteration %d -> %d", i, j)
			}
		}
	}
}
