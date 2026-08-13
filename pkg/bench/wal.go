package bench

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrCRCMismatch    = errors.New("wal: crc32 checksum mismatch, data corrupted")
	ErrIncompleteRead = errors.New("wal: incomplete record read")
	ErrWALClosed      = errors.New("wal: write-ahead log is closed")
)

const (
	// HeaderSize: CRC32 (4) + KeyLen (4) + ValLen (4) + Tombstone (1) = 13 bytes
	HeaderSize = 13
)

// LogEntry represents a single key-value record in the Write-Ahead Log.
type LogEntry struct {
	CRC32     uint32
	Key       []byte
	Value     []byte
	Tombstone bool
}

// WALOptions defines configurable parameters for the Bench Write-Ahead Log.
type WALOptions struct {
	// SyncOnWrite forces fsync() after every Append operation.
	SyncOnWrite bool

	// DirPath specifies the directory where WAL log files are persisted.
	DirPath string

	// SegmentSize specifies the max byte size before rolling to a new segment (0 = no rolling).
	SegmentSize int64
}

// DefaultWALOptions provides sensible defaults for local development.
func DefaultWALOptions(dir string) WALOptions {
	return WALOptions{
		SyncOnWrite: true,
		DirPath:     dir,
		SegmentSize: 16 * 1024 * 1024, // 16 MB default segment limit for Financial & ACID profile
	}
}

// WAL represents the Bench Engine Write-Ahead Log instance.
type WAL struct {
	mu     sync.Mutex
	opts   WALOptions
	file   *os.File
	closed bool
	size   int64
}

// OpenWAL creates or opens a WAL instance at the configured directory.
func OpenWAL(opts WALOptions) (*WAL, error) {
	if err := os.MkdirAll(opts.DirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create wal directory: %w", err)
	}

	walPath := filepath.Join(opts.DirPath, "bench_0.wal")
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat wal file: %w", err)
	}

	return &WAL{
		opts: opts,
		file: file,
		size: stat.Size(),
	}, nil
}

// EncodeRecord serializes a key-value entry into binary payload with CRC32 checksum.
// Record Format: [4-byte CRC32][4-byte Key Length][4-byte Value Length][1-byte Tombstone Flag][Key Bytes][Value Bytes]
func EncodeRecord(key, value []byte, tombstone bool) []byte {
	keyLen := uint32(len(key))
	valLen := uint32(len(value))
	totalLen := HeaderSize + keyLen + valLen

	buf := make([]byte, totalLen)

	// Write metadata fields (starting after 4-byte CRC offset)
	binary.BigEndian.PutUint32(buf[4:8], keyLen)
	binary.BigEndian.PutUint32(buf[8:12], valLen)
	if tombstone {
		buf[12] = 1
	} else {
		buf[12] = 0
	}

	// Write Key and Value bytes
	copy(buf[13:13+keyLen], key)
	copy(buf[13+keyLen:], value)

	// Compute CRC32 checksum over [KeyLen..ValueBytes]
	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], checksum)

	return buf
}

// Append writes a key-value pair or tombstone record to the WAL.
func (w *WAL) Append(key, value []byte, tombstone bool) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, ErrWALClosed
	}

	encoded := EncodeRecord(key, value, tombstone)
	n, err := w.file.Write(encoded)
	if err != nil {
		return 0, fmt.Errorf("failed to write wal entry: %w", err)
	}

	w.size += int64(n)

	if w.opts.SyncOnWrite {
		if err := w.file.Sync(); err != nil {
			return int64(n), fmt.Errorf("failed to fsync wal entry: %w", err)
		}
	}

	return int64(n), nil
}

// Sync forces an fsync on the underlying WAL file.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrWALClosed
	}
	return w.file.Sync()
}

// Recover reads the WAL sequentially from beginning to end, verifying CRC32 checksums.
func (w *WAL) Recover() ([]LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, ErrWALClosed
	}

	// Seek to start of file for sequential recovery
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek start of wal file: %w", err)
	}

	var entries []LogEntry
	headerBuf := make([]byte, HeaderSize)

	for {
		_, err := io.ReadFull(w.file, headerBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break // End of valid file read
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read wal header: %w", err)
		}

		expectedCRC := binary.BigEndian.Uint32(headerBuf[0:4])
		keyLen := binary.BigEndian.Uint32(headerBuf[4:8])
		valLen := binary.BigEndian.Uint32(headerBuf[8:12])
		tombstone := headerBuf[12] == 1

		bodyLen := keyLen + valLen
		bodyBuf := make([]byte, bodyLen)

		if _, err := io.ReadFull(w.file, bodyBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Truncated/incomplete write at end of file
				break
			}
			return nil, fmt.Errorf("failed to read wal record body: %w", err)
		}

		// Reconstruct payload buffer for CRC verification
		checkBuf := make([]byte, 9+bodyLen)
		copy(checkBuf[0:9], headerBuf[4:13])
		copy(checkBuf[9:], bodyBuf)

		actualCRC := crc32.ChecksumIEEE(checkBuf)
		if expectedCRC != actualCRC {
			return nil, fmt.Errorf("%w: expected %d, got %d", ErrCRCMismatch, expectedCRC, actualCRC)
		}

		key := make([]byte, keyLen)
		copy(key, bodyBuf[:keyLen])

		val := make([]byte, valLen)
		copy(val, bodyBuf[keyLen:])

		entries = append(entries, LogEntry{
			CRC32:     expectedCRC,
			Key:       key,
			Value:     val,
			Tombstone: tombstone,
		})
	}

	// Seek back to end of file to prepare for subsequent appends
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("failed to seek end of wal file: %w", err)
	}

	return entries, nil
}

// Close flushes and closes the active WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true
	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.file.Close()
}
