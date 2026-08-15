package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSSTableBuildAndGet(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "000001.sst")

	mem := NewMemTable(DefaultMemTableOptions())
	entries := []struct {
		key       []byte
		val       []byte
		tombstone bool
	}{
		{key: []byte("geo_001"), val: []byte("geo_val_100"), tombstone: false},
		{key: []byte("geo_002"), val: []byte("geo_val_200"), tombstone: false},
		{key: []byte("geo_003"), val: []byte("geo_val_300"), tombstone: true}, // Tombstone
		{key: []byte("geo_004"), val: []byte("geo_val_400"), tombstone: false},
	}

	for _, e := range entries {
		if e.tombstone {
			mem.Delete(e.key)
		} else {
			mem.Put(e.key, e.val)
		}
	}

	opts := DefaultSSTableOptions()
	opts.SparseInterval = 2 // Small sparse index interval for testing

	reader, err := BuildSSTable(sstPath, mem.Iterator(), len(entries), opts)
	if err != nil {
		t.Fatalf("failed to build sstable: %v", err)
	}

	// Verify Point Lookups on active reader
	for _, e := range entries {
		val, tombstone, found, err := reader.Get(e.key)
		if err != nil {
			t.Fatalf("unexpected error getting key %s: %v", string(e.key), err)
		}
		if !found {
			t.Fatalf("expected key %s to be found", string(e.key))
		}
		if tombstone != e.tombstone {
			t.Errorf("key %s: expected tombstone %v, got %v", string(e.key), e.tombstone, tombstone)
		}
		if !e.tombstone && !bytes.Equal(val, e.val) {
			t.Errorf("key %s: expected val %s, got %s", string(e.key), string(e.val), string(val))
		}
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("failed closing sstable reader: %v", err)
	}

	// Reopen SSTable and verify deserialized C++ Bloom filter and Index
	reader2, err := OpenSSTable(sstPath)
	if err != nil {
		t.Fatalf("failed reopening sstable: %v", err)
	}
	defer reader2.Close()

	val, tombstone, found, err := reader2.Get([]byte("geo_002"))
	if err != nil || !found || tombstone || !bytes.Equal(val, []byte("geo_val_200")) {
		t.Fatalf("reopened sstable failed lookup for geo_002: val=%s, tombstone=%v, found=%v, err=%v", string(val), tombstone, found, err)
	}
}

func TestLumaflyBloomFilterShortCircuit(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "000002.sst")

	mem := NewMemTable(DefaultMemTableOptions())
	numKeys := 500

	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("knight_key_%05d", i))
		val := []byte(fmt.Sprintf("vessel_val_%05d", i))
		mem.Put(key, val)
	}

	reader, err := BuildSSTable(sstPath, mem.Iterator(), numKeys, DefaultSSTableOptions())
	if err != nil {
		t.Fatalf("failed to build sstable: %v", err)
	}
	defer reader.Close()

	// Existing keys must be found
	_, _, found, err := reader.Get([]byte("knight_key_00250"))
	if err != nil || !found {
		t.Fatalf("expected existing key knight_key_00250 to be found")
	}

	// Non-existent keys should short-circuit with 0 errors
	missingCount := 0
	for i := 1000; i < 1500; i++ {
		key := []byte(fmt.Sprintf("missing_key_%05d", i))
		_, _, found, err := reader.Get(key)
		if err != nil {
			t.Fatalf("unexpected error during get: %v", err)
		}
		if found {
			missingCount++
		}
	}

	// False positive rate for 500 missing keys with 0.01% target must be <= 2
	if missingCount > 2 {
		t.Fatalf("expected at most 2 false positives out of 500, got %d", missingCount)
	}
}

func TestSSTableCorruptedCRC(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "corrupt_test.sst")

	mem := NewMemTable(DefaultMemTableOptions())
	mem.Put([]byte("knight"), []byte("vessel_500"))

	reader, err := BuildSSTable(sstPath, mem.Iterator(), 1, DefaultSSTableOptions())
	if err != nil {
		t.Fatalf("failed building sstable: %v", err)
	}
	_ = reader.Close()

	// Corrupt a byte in the sstable data section
	data, err := os.ReadFile(sstPath)
	if err != nil {
		t.Fatalf("failed reading sstable file: %v", err)
	}

	// Flip a bit in the value section (offset 20)
	data[20] ^= 0xFF
	if err := os.WriteFile(sstPath, data, 0644); err != nil {
		t.Fatalf("failed writing corrupted sstable: %v", err)
	}

	reader2, err := OpenSSTable(sstPath)
	if err != nil {
		t.Fatalf("failed reopening sstable: %v", err)
	}
	defer reader2.Close()

	_, _, _, err = reader2.Get([]byte("knight"))
	if err == nil {
		t.Fatalf("expected CRC mismatch error on corrupted SSTable, got nil")
	}
}

func BenchmarkSSTableGet(b *testing.B) {
	tempDir := b.TempDir()
	sstPath := filepath.Join(tempDir, "bench_sstable.sst")

	mem := NewMemTable(DefaultMemTableOptions())
	numKeys := 10000

	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("bench_key_%06d", i))
		val := []byte(fmt.Sprintf("bench_val_%06d", i))
		mem.Put(key, val)
	}

	reader, err := BuildSSTable(sstPath, mem.Iterator(), numKeys, DefaultSSTableOptions())
	if err != nil {
		b.Fatalf("failed to build sstable: %v", err)
	}
	defer reader.Close()

	targetKey := []byte("bench_key_005000")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, err := reader.Get(targetKey)
		if err != nil {
			b.Fatalf("get error: %v", err)
		}
	}
}
