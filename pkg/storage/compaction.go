package storage

import (
	"fmt"
	"os"
	"sort"
	"sync"
)

// CompactorOptions defines configuration parameters for the Abyss Compaction Engine.
type CompactorOptions struct {
	// L0Trigger represents the number of Level 0 SSTables that trigger compaction.
	L0Trigger int

	// DestDir specifies the directory where Level 1..N SSTables are created.
	DestDir string
}

// DefaultCompactorOptions provides sensible defaults for Abyss Compaction.
func DefaultCompactorOptions(dir string) CompactorOptions {
	return CompactorOptions{
		L0Trigger: 4,
		DestDir:   dir,
	}
}

// Compactor manages background compaction of Level 0 overlapping SSTables into Level 1+ SSTables.
type Compactor struct {
	mu   sync.Mutex
	opts CompactorOptions
}

// NewCompactor creates a new Abyss Compactor instance.
func NewCompactor(opts CompactorOptions) *Compactor {
	if opts.L0Trigger <= 0 {
		opts.L0Trigger = 4
	}
	return &Compactor{opts: opts}
}

type CompactionEntry struct {
	Key       []byte
	Value     []byte
	Tombstone bool
}

// CompactL0ToL1 merges multiple overlapping Level 0 SSTable files into a single non-overlapping Level 1 SSTable,
// purging obsolete tombstones, and removing the old L0 disk files.
func (c *Compactor) CompactL0ToL1(l0Readers []*SSTableReader, destFile string, sstOpts SSTableOptions) (*SSTableReader, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(l0Readers) == 0 {
		return nil, fmt.Errorf("no L0 sstables provided for compaction")
	}

	// Use a map for deduplication (latest entry per key wins)
	entryMap := make(map[string]CompactionEntry)

	// Iterate through all L0 readers to extract records
	for _, reader := range l0Readers {
		reader.mu.RLock()
		index := reader.index
		reader.mu.RUnlock()

		// Scan data block for each reader
		for _, idxEntry := range index {
			val, tombstone, found, err := reader.Get(idxEntry.Key)
			if err != nil {
				continue
			}
			if found {
				kStr := string(idxEntry.Key)
				entryMap[kStr] = CompactionEntry{
					Key:       idxEntry.Key,
					Value:     val,
					Tombstone: tombstone,
				}
			}
		}
	}

	// Sort deduplicated keys in ascending order
	var keys []string
	for k := range entryMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create temporary MemTable to build the new Level 1 SSTable
	mem := NewMemTable(DefaultMemTableOptions())
	retainedCount := 0

	for _, kStr := range keys {
		entry := entryMap[kStr]
		// At Level 1 (max level compaction), purge tombstones to reclaim disk space
		if entry.Tombstone {
			continue
		}
		mem.Put(entry.Key, entry.Value)
		retainedCount++
	}

	if retainedCount == 0 {
		// All keys were tombstones or empty -> create clean minimal SSTable
		mem.Put([]byte("__abyss_compaction_sentinel__"), []byte("ok"))
		retainedCount = 1
	}

	// Build new Level 1 SSTable with C++ Lumafly Filter and Bit-Rot Protection
	l1Reader, err := BuildSSTable(destFile, mem.Iterator(), retainedCount, sstOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to build compacted L1 sstable: %w", err)
	}

	// Safely close and delete old L0 SSTable files from disk
	for _, reader := range l0Readers {
		fileName := reader.filename
		_ = reader.Close()
		_ = os.Remove(fileName)
	}

	return l1Reader, nil
}
