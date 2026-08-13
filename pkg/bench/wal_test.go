package bench

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWALAppendAndRecover(t *testing.T) {
	tempDir := t.TempDir()
	opts := DefaultWALOptions(tempDir)
	opts.SyncOnWrite = true

	wal, err := OpenWAL(opts)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	records := []struct {
		key       []byte
		val       []byte
		tombstone bool
	}{
		{key: []byte("geo_knight"), val: []byte("vessel_500"), tombstone: false},
		{key: []byte("geo_hornet"), val: []byte("silk_1000"), tombstone: false},
		{key: []byte("geo_zote"), val: []byte("precept_57"), tombstone: true},
	}

	for _, rec := range records {
		_, err := wal.Append(rec.key, rec.val, rec.tombstone)
		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("failed to close wal: %v", err)
	}

	// Re-open WAL and recover
	wal2, err := OpenWAL(opts)
	if err != nil {
		t.Fatalf("failed to reopen wal: %v", err)
	}
	defer wal2.Close()

	entries, err := wal2.Recover()
	if err != nil {
		t.Fatalf("failed to recover wal: %v", err)
	}

	if len(entries) != len(records) {
		t.Fatalf("expected %d entries, got %d", len(records), len(entries))
	}

	for i, expected := range records {
		actual := entries[i]
		if !bytes.Equal(actual.Key, expected.key) {
			t.Errorf("entry %d: expected key %s, got %s", i, expected.key, actual.Key)
		}
		if !bytes.Equal(actual.Value, expected.val) {
			t.Errorf("entry %d: expected val %s, got %s", i, expected.val, actual.Value)
		}
		if actual.Tombstone != expected.tombstone {
			t.Errorf("entry %d: expected tombstone %v, got %v", i, expected.tombstone, actual.Tombstone)
		}
	}
}

func TestWALCorruptedCRC(t *testing.T) {
	tempDir := t.TempDir()
	opts := DefaultWALOptions(tempDir)

	wal, err := OpenWAL(opts)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	_, err = wal.Append([]byte("radiance"), []byte("light"), false)
	if err != nil {
		t.Fatalf("failed to append record: %v", err)
	}
	_ = wal.Close()

	// Corrupt a byte in the payload
	walFilePath := filepath.Join(tempDir, "bench_0.wal")
	data, err := os.ReadFile(walFilePath)
	if err != nil {
		t.Fatalf("failed to read wal file: %v", err)
	}

	// Flip a bit in the value data section
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(walFilePath, data, 0644); err != nil {
		t.Fatalf("failed to write corrupted data: %v", err)
	}

	wal2, err := OpenWAL(opts)
	if err != nil {
		t.Fatalf("failed to reopen wal: %v", err)
	}
	defer wal2.Close()

	_, err = wal2.Recover()
	if err == nil {
		t.Fatalf("expected CRC mismatch error, but got nil")
	}
}

func BenchmarkWALAppend(b *testing.B) {
	tempDir := b.TempDir()
	opts := DefaultWALOptions(tempDir)
	opts.SyncOnWrite = false // async write for throughput test

	wal, err := OpenWAL(opts)
	if err != nil {
		b.Fatalf("failed to open wal: %v", err)
	}
	defer wal.Close()

	key := []byte("bench_key_geo")
	val := []byte("bench_val_lumafly_500")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := wal.Append(key, val, false)
		if err != nil {
			b.Fatalf("failed append: %v", err)
		}
	}
}
