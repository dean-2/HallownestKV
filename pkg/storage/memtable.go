package storage

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)

const (
	DefaultMaxLevel       = 16
	DefaultP              = 0.25
	DefaultFlushThreshold = 4 * 1024 * 1024 // 4 MB
)

// Node represents an entry in the SkipList.
type Node struct {
	Key       []byte
	Value     []byte
	Tombstone bool
	forward   []*Node
}

// MemTableOptions defines configuration for the Soul MemTable.
type MemTableOptions struct {
	MaxLevel       int
	P              float32
	FlushThreshold int64
}

// DefaultMemTableOptions provides standard defaults for MemTable.
func DefaultMemTableOptions() MemTableOptions {
	return MemTableOptions{
		MaxLevel:       DefaultMaxLevel,
		P:              DefaultP,
		FlushThreshold: DefaultFlushThreshold,
	}
}

// MemTable is a thread-safe, SkipList-backed write buffer for HallownestKV.
type MemTable struct {
	mu        sync.RWMutex
	head      *Node
	level     int
	sizeBytes int64
	opts      MemTableOptions
	rnd       *rand.Rand
}

// NewMemTable creates a new initialized MemTable.
func NewMemTable(opts MemTableOptions) *MemTable {
	if opts.MaxLevel <= 0 {
		opts.MaxLevel = DefaultMaxLevel
	}
	if opts.P <= 0 || opts.P >= 1 {
		opts.P = DefaultP
	}
	if opts.FlushThreshold <= 0 {
		opts.FlushThreshold = DefaultFlushThreshold
	}

	head := &Node{
		forward: make([]*Node, opts.MaxLevel),
	}

	return &MemTable{
		head:  head,
		level: 1,
		opts:  opts,
		rnd:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// randomLevel generates a random height level for a new node.
func (m *MemTable) randomLevel() int {
	lvl := 1
	for float32(m.rnd.Float64()) < m.opts.P && lvl < m.opts.MaxLevel {
		lvl++
	}
	return lvl
}

// Put inserts or updates a key-value pair in the MemTable.
// Returns true if the memory size threshold has been reached/exceeded.
func (m *MemTable) Put(key, value []byte) bool {
	return m.insert(key, value, false)
}

// Delete inserts a tombstone marker for the specified key.
// Returns true if the memory size threshold has been reached/exceeded.
func (m *MemTable) Delete(key []byte) bool {
	return m.insert(key, nil, true)
}

func (m *MemTable) insert(key, value []byte, tombstone bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	update := make([]*Node, m.opts.MaxLevel)
	curr := m.head

	// Traverse from top level down to find insert position per level
	for i := m.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && bytes.Compare(curr.forward[i].Key, key) < 0 {
			curr = curr.forward[i]
		}
		update[i] = curr
	}

	curr = curr.forward[0]

	// Key already exists -> Update value/tombstone and update size
	if curr != nil && bytes.Equal(curr.Key, key) {
		oldSize := int64(len(curr.Key) + len(curr.Value))
		newSize := int64(len(key) + len(value))

		curr.Value = make([]byte, len(value))
		copy(curr.Value, value)
		curr.Tombstone = tombstone

		m.sizeBytes += (newSize - oldSize)
		return m.sizeBytes >= m.opts.FlushThreshold
	}

	// Key does not exist -> Insert new SkipList Node
	newLevel := m.randomLevel()
	if newLevel > m.level {
		for i := m.level; i < newLevel; i++ {
			update[i] = m.head
		}
		m.level = newLevel
	}

	newNode := &Node{
		Key:       make([]byte, len(key)),
		Value:     make([]byte, len(value)),
		Tombstone: tombstone,
		forward:   make([]*Node, newLevel),
	}
	copy(newNode.Key, key)
	copy(newNode.Value, value)

	for i := 0; i < newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	m.sizeBytes += int64(len(key) + len(value))
	return m.sizeBytes >= m.opts.FlushThreshold
}

// Get looks up a key in the MemTable.
// Returns (value, isTombstone, found).
func (m *MemTable) Get(key []byte) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	curr := m.head
	for i := m.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && bytes.Compare(curr.forward[i].Key, key) < 0 {
			curr = curr.forward[i]
		}
	}

	curr = curr.forward[0]
	if curr != nil && bytes.Equal(curr.Key, key) {
		valCopy := make([]byte, len(curr.Value))
		copy(valCopy, curr.Value)
		return valCopy, curr.Tombstone, true
	}

	return nil, false, false
}

// SizeBytes returns the total memory consumed by keys and values in the MemTable.
func (m *MemTable) SizeBytes() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes
}

// IsFull checks if the MemTable size has reached or exceeded the flush threshold.
func (m *MemTable) IsFull() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes >= m.opts.FlushThreshold
}

// Iterator returns an active iterator over all elements in ascending key order.
type Iterator struct {
	curr *Node
}

// Iterator creates an iterator positioned before the first element.
func (m *MemTable) Iterator() *Iterator {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return &Iterator{curr: m.head}
}

// HasNext checks if there is another element in the iteration.
func (it *Iterator) HasNext() bool {
	return it.curr != nil && it.curr.forward[0] != nil
}

// Next advances the iterator and returns the next Node.
func (it *Iterator) Next() *Node {
	if !it.HasNext() {
		return nil
	}
	it.curr = it.curr.forward[0]
	return it.curr
}
