package storage

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/utkarshraj/hallownestkv/pkg/bench"
)

func TestWALCrashRecoveryReplay(t *testing.T) {
	tempDir := t.TempDir()
	opts := bench.DefaultWALOptions(tempDir)

	wal, err := bench.OpenWAL(opts)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	records := []struct {
		key       []byte
		val       []byte
		tombstone bool
	}{
		{key: []byte("geo_knight"), val: []byte("vessel_500"), tombstone: false},
		{key: []byte("geo_hornet"), val: []byte("needle_1000"), tombstone: false},
		{key: []byte("geo_zote"), val: []byte("precept_57"), tombstone: true},
	}

	for _, rec := range records {
		_, err := wal.Append(rec.key, rec.val, rec.tombstone)
		if err != nil {
			t.Fatalf("failed appending to wal: %v", err)
		}
	}
	_ = wal.Close()

	// Simulate database crash recovery -> Replay WAL into fresh MemTable
	mem := NewMemTable(DefaultMemTableOptions())
	replayed, err := bench.ReplayWAL(tempDir, func(key, value []byte, tombstone bool) {
		if tombstone {
			mem.Delete(key)
		} else {
			mem.Put(key, value)
		}
	})

	if err != nil {
		t.Fatalf("failed replaying wal: %v", err)
	}
	if replayed != len(records) {
		t.Fatalf("expected %d replayed records, got %d", len(records), replayed)
	}

	// Verify recovered MemTable state
	val, tombstone, found := mem.Get([]byte("geo_knight"))
	if !found || tombstone || !bytes.Equal(val, []byte("vessel_500")) {
		t.Fatalf("recovered key geo_knight mismatch: val=%s, found=%v", string(val), found)
	}

	val, tombstone, found = mem.Get([]byte("geo_zote"))
	if !found || !tombstone {
		t.Fatalf("expected geo_zote to be recovered as tombstone")
	}
}

func TestAbyssLeveledCompaction(t *testing.T) {
	tempDir := t.TempDir()
	var l0Readers []*SSTableReader

	// Create 3 overlapping Level 0 SSTables
	for i := 0; i < 3; i++ {
		mem := NewMemTable(DefaultMemTableOptions())
		mem.Put([]byte("common_key"), []byte(fmt.Sprintf("val_ver_%d", i)))
		mem.Put([]byte(fmt.Sprintf("unique_key_%d", i)), []byte(fmt.Sprintf("val_%d", i)))
		if i == 2 {
			mem.Delete([]byte("tombstone_key"))
		}

		sstPath := filepath.Join(tempDir, fmt.Sprintf("l0_%d.sst", i))
		reader, err := BuildSSTable(sstPath, mem.Iterator(), 3, DefaultSSTableOptions())
		if err != nil {
			t.Fatalf("failed building L0 sstable %d: %v", i, err)
		}
		l0Readers = append(l0Readers, reader)
	}

	compactor := NewCompactor(DefaultCompactorOptions(tempDir))
	l1Path := filepath.Join(tempDir, "l1_compacted.sst")

	l1Reader, err := compactor.CompactL0ToL1(l0Readers, l1Path, DefaultSSTableOptions())
	if err != nil {
		t.Fatalf("failed executing L0 to L1 compaction: %v", err)
	}
	defer l1Reader.Close()

	// Verify deduplicated latest value
	val, tombstone, found, err := l1Reader.Get([]byte("common_key"))
	if err != nil || !found || tombstone || !bytes.Equal(val, []byte("val_ver_2")) {
		t.Fatalf("expected common_key to have latest val_ver_2, got %s (found=%v, err=%v)", string(val), found, err)
	}

	// Verify tombstone was purged at Level 1 compaction
	_, _, found, _ = l1Reader.Get([]byte("tombstone_key"))
	if found {
		t.Fatalf("expected tombstone_key to be purged during Level 1 compaction")
	}
}
