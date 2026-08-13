package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestMemTablePutGetDelete(t *testing.T) {
	opts := DefaultMemTableOptions()
	mem := NewMemTable(opts)

	// Put & Get
	mem.Put([]byte("knight"), []byte("vessel"))
	val, tombstone, found := mem.Get([]byte("knight"))
	if !found {
		t.Fatalf("expected key 'knight' to be found")
	}
	if tombstone {
		t.Fatalf("expected key 'knight' to not be tombstone")
	}
	if !bytes.Equal(val, []byte("vessel")) {
		t.Fatalf("expected value 'vessel', got '%s'", string(val))
	}

	// Update existing key
	mem.Put([]byte("knight"), []byte("shade"))
	val, tombstone, found = mem.Get([]byte("knight"))
	if !found || tombstone || !bytes.Equal(val, []byte("shade")) {
		t.Fatalf("expected updated value 'shade', got '%s'", string(val))
	}

	// Delete key (Tombstone)
	mem.Delete([]byte("knight"))
	val, tombstone, found = mem.Get([]byte("knight"))
	if !found {
		t.Fatalf("expected key 'knight' to be found as tombstone")
	}
	if !tombstone {
		t.Fatalf("expected tombstone flag to be true")
	}

	// Get non-existent key
	_, _, found = mem.Get([]byte("hornet"))
	if found {
		t.Fatalf("expected key 'hornet' to not be found")
	}
}

func TestMemTableIterator(t *testing.T) {
	mem := NewMemTable(DefaultMemTableOptions())

	keys := []string{"delta", "alpha", "charlie", "bravo"}
	for _, k := range keys {
		mem.Put([]byte(k), []byte("val_"+k))
	}

	iter := mem.Iterator()
	expectedOrder := []string{"alpha", "bravo", "charlie", "delta"}
	idx := 0

	for iter.HasNext() {
		node := iter.Next()
		if string(node.Key) != expectedOrder[idx] {
			t.Errorf("at index %d: expected key '%s', got '%s'", idx, expectedOrder[idx], string(node.Key))
		}
		idx++
	}

	if idx != len(expectedOrder) {
		t.Fatalf("expected %d iterated items, got %d", len(expectedOrder), idx)
	}
}

func TestMemTableFlushThreshold(t *testing.T) {
	opts := DefaultMemTableOptions()
	opts.FlushThreshold = 100 // small threshold for testing

	mem := NewMemTable(opts)

	if mem.IsFull() {
		t.Fatalf("expected empty MemTable to not be full")
	}

	// Add 60 bytes of key+val
	isFull := mem.Put([]byte("key1_10bytes"), make([]byte, 50))
	if isFull || mem.IsFull() {
		t.Fatalf("expected MemTable not to be full at 60 bytes")
	}

	// Add another 50 bytes -> total 110 bytes (exceeds 100 threshold)
	isFull = mem.Put([]byte("key2_10bytes"), make([]byte, 40))
	if !isFull || !mem.IsFull() {
		t.Fatalf("expected MemTable to be full after exceeding threshold")
	}
}

func TestMemTableConcurrent(t *testing.T) {
	mem := NewMemTable(DefaultMemTableOptions())
	var wg sync.WaitGroup

	numGoroutines := 10
	opsPerGoroutine := 100

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gId int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("key_%d_%d", gId, i))
				val := []byte(fmt.Sprintf("val_%d_%d", gId, i))
				mem.Put(key, val)
				mem.Get(key)
			}
		}(g)
	}

	wg.Wait()

	if mem.SizeBytes() <= 0 {
		t.Fatalf("expected positive memory size after concurrent operations")
	}
}

func BenchmarkMemTablePut(b *testing.B) {
	mem := NewMemTable(DefaultMemTableOptions())
	val := []byte("bench_value_geo_soul_100")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		mem.Put(key, val)
	}
}
