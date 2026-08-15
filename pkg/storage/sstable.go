package storage

/*
#cgo CXXFLAGS: -std=c++17 -O3 -I../../cpp/include
#include "../../cpp/include/lumafly_bloom.h"
#include <stdlib.h>
#include <stdbool.h>
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync"
	"unsafe"
)

var (
	ErrSSTableCorrupted = errors.New("sstable: file layout, checksum, or footer corrupted")
	ErrKeyNotFound      = errors.New("sstable: key not found")
)

const (
	FooterSize           = 32
	DefaultIndexInterval = 8
	RecordHeaderSize     = 13 // CRC32 (4) + KeyLen (4) + ValLen (4) + Tombstone (1)
)

type IndexEntry struct {
	Key    []byte
	Offset uint64
}

type Footer struct {
	IndexOffset  uint64
	IndexLen     uint64
	FilterOffset uint64
	FilterLen    uint64
}

type SSTableOptions struct {
	SparseInterval int
	FPRate         float64
}

// DefaultSSTableOptions provides ultra-durability defaults (0.01% FP rate, sparse interval = 8)
func DefaultSSTableOptions() SSTableOptions {
	return SSTableOptions{
		SparseInterval: DefaultIndexInterval,
		FPRate:         0.0001, // 0.01% target for Ultra-Durability profile (99.99% accuracy)
	}
}

type SSTableReader struct {
	mu          sync.RWMutex
	file        *os.File
	filename    string
	footer      Footer
	index       []IndexEntry
	bloomFilter *C.LumaflyBloom
}

// BuildSSTable flushes an ordered MemTable iterator to a Deepnest SSTable file on disk.
func BuildSSTable(filename string, iter *Iterator, expectedEntries int, opts SSTableOptions) (*SSTableReader, error) {
	if opts.SparseInterval <= 0 {
		opts.SparseInterval = DefaultIndexInterval
	}
	if opts.FPRate <= 0 {
		opts.FPRate = 0.0001
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create sstable file: %w", err)
	}

	// Create C++ Lumafly Bloom Filter
	cFilter := C.lumafly_create(C.size_t(expectedEntries), C.double(opts.FPRate))
	if cFilter == nil {
		file.Close()
		return nil, fmt.Errorf("failed to create C++ lumafly bloom filter")
	}
	defer C.lumafly_destroy(cFilter)

	var index []IndexEntry
	var currentOffset uint64
	keyCount := 0

	// 1. Write Data Block entries with CRC32 Bit-Rot Protection
	for iter.HasNext() {
		node := iter.Next()

		// Add key to C++ Lumafly Bloom Filter
		cKey := C.CBytes(node.Key)
		C.lumafly_add(cFilter, (*C.char)(cKey), C.size_t(len(node.Key)))
		C.free(cKey)

		// Record Sparse Index entry every N keys
		if keyCount%opts.SparseInterval == 0 {
			keyCopy := make([]byte, len(node.Key))
			copy(keyCopy, node.Key)
			index = append(index, IndexEntry{
				Key:    keyCopy,
				Offset: currentOffset,
			})
		}

		// Format record: [4B CRC32][4B KeyLen][4B ValLen][1B Tombstone][Key Bytes][Value Bytes]
		keyLen := uint32(len(node.Key))
		valLen := uint32(len(node.Value))
		recordLen := RecordHeaderSize + len(node.Key) + len(node.Value)

		recBuf := make([]byte, recordLen)
		binary.BigEndian.PutUint32(recBuf[4:8], keyLen)
		binary.BigEndian.PutUint32(recBuf[8:12], valLen)
		if node.Tombstone {
			recBuf[12] = 1
		} else {
			recBuf[12] = 0
		}
		copy(recBuf[13:13+keyLen], node.Key)
		copy(recBuf[13+keyLen:], node.Value)

		// Compute CRC32 checksum over [KeyLen..ValueBytes] for Bit-Rot Protection
		checksum := crc32.ChecksumIEEE(recBuf[4:])
		binary.BigEndian.PutUint32(recBuf[0:4], checksum)

		n, err := file.Write(recBuf)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("failed writing data block record: %w", err)
		}
		currentOffset += uint64(n)
		keyCount++
	}

	// 2. Write Sparse Index Block
	indexOffset := currentOffset
	var indexBuf bytes.Buffer
	for _, ie := range index {
		var entryHdr [12]byte
		binary.BigEndian.PutUint32(entryHdr[0:4], uint32(len(ie.Key)))
		binary.BigEndian.PutUint64(entryHdr[4:12], ie.Offset)
		indexBuf.Write(entryHdr[:4])
		indexBuf.Write(ie.Key)
		indexBuf.Write(entryHdr[4:12])
	}

	nIdx, err := file.Write(indexBuf.Bytes())
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed writing index block: %w", err)
	}
	indexLen := uint64(nIdx)
	currentOffset += indexLen

	// 3. Write C++ Lumafly Bloom Filter Block
	filterOffset := currentOffset
	serSize := C.lumafly_get_serialized_size(cFilter)
	filterBuf := make([]byte, serSize)
	writtenSer := C.lumafly_serialize(cFilter, (*C.char)(unsafe.Pointer(&filterBuf[0])), C.size_t(serSize))
	if writtenSer == 0 {
		file.Close()
		return nil, fmt.Errorf("failed to serialize C++ lumafly bloom filter")
	}

	nFlt, err := file.Write(filterBuf)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed writing bloom filter block: %w", err)
	}
	filterLen := uint64(nFlt)
	currentOffset += filterLen

	// 4. Write Footer (32 bytes)
	var footerBuf [FooterSize]byte
	binary.BigEndian.PutUint64(footerBuf[0:8], indexOffset)
	binary.BigEndian.PutUint64(footerBuf[8:16], indexLen)
	binary.BigEndian.PutUint64(footerBuf[16:24], filterOffset)
	binary.BigEndian.PutUint64(footerBuf[24:32], filterLen)

	if _, err := file.Write(footerBuf[:]); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed writing sstable footer: %w", err)
	}

	if err := file.Sync(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to fsync sstable file: %w", err)
	}

	file.Close()

	// Reopen for reading
	return OpenSSTable(filename)
}

// OpenSSTable opens an existing SSTable file, reads footer, sparse index, and C++ Bloom filter into memory.
func OpenSSTable(filename string) (*SSTableReader, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open sstable file: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat sstable file: %w", err)
	}

	if stat.Size() < FooterSize {
		file.Close()
		return nil, ErrSSTableCorrupted
	}

	// Read Footer
	var footerBuf [FooterSize]byte
	if _, err := file.ReadAt(footerBuf[:], stat.Size()-FooterSize); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed reading sstable footer: %w", err)
	}

	footer := Footer{
		IndexOffset:  binary.BigEndian.Uint64(footerBuf[0:8]),
		IndexLen:     binary.BigEndian.Uint64(footerBuf[8:16]),
		FilterOffset: binary.BigEndian.Uint64(footerBuf[16:24]),
		FilterLen:    binary.BigEndian.Uint64(footerBuf[24:32]),
	}

	// Read Sparse Index Block
	indexBuf := make([]byte, footer.IndexLen)
	if _, err := file.ReadAt(indexBuf, int64(footer.IndexOffset)); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed reading index block: %w", err)
	}

	var index []IndexEntry
	idxReader := bytes.NewReader(indexBuf)
	for idxReader.Len() > 0 {
		var kLen uint32
		if err := binary.Read(idxReader, binary.BigEndian, &kLen); err != nil {
			break
		}
		keyBuf := make([]byte, kLen)
		if _, err := io.ReadFull(idxReader, keyBuf); err != nil {
			file.Close()
			return nil, ErrSSTableCorrupted
		}
		var offset uint64
		if err := binary.Read(idxReader, binary.BigEndian, &offset); err != nil {
			file.Close()
			return nil, ErrSSTableCorrupted
		}
		index = append(index, IndexEntry{
			Key:    keyBuf,
			Offset: offset,
		})
	}

	// Read Lumafly Bloom Filter Block
	filterBuf := make([]byte, footer.FilterLen)
	if _, err := file.ReadAt(filterBuf, int64(footer.FilterOffset)); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed reading bloom filter block: %w", err)
	}

	cFilter := C.lumafly_deserialize((*C.char)(unsafe.Pointer(&filterBuf[0])), C.size_t(footer.FilterLen))
	if cFilter == nil {
		file.Close()
		return nil, fmt.Errorf("failed deserializing C++ lumafly bloom filter")
	}

	return &SSTableReader{
		file:        file,
		filename:    filename,
		footer:      footer,
		index:       index,
		bloomFilter: cFilter,
	}, nil
}

// Get performs a point lookup in the Deepnest SSTable.
// Returns (value, isTombstone, found, error).
func (r *SSTableReader) Get(key []byte) ([]byte, bool, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Step 1: C++ Lumafly Bloom Filter Short-Circuit
	cKey := C.CBytes(key)
	contains := bool(C.lumafly_contains(r.bloomFilter, (*C.char)(cKey), C.size_t(len(key))))
	C.free(cKey)

	if !contains {
		// Key definitely not present in SSTable (0 disk reads!)
		return nil, false, false, nil
	}

	// Step 2: Binary Search Sparse Index
	if len(r.index) == 0 {
		return nil, false, false, nil
	}

	// Search for the largest index entry <= key
	idx := sort.Search(len(r.index), func(i int) bool {
		return bytes.Compare(r.index[i].Key, key) > 0
	})

	var startOffset uint64
	var endOffset uint64 = r.footer.IndexOffset

	if idx > 0 {
		startOffset = r.index[idx-1].Offset
	} else {
		startOffset = 0
	}

	if idx < len(r.index) {
		endOffset = r.index[idx].Offset
	}

	// Step 3: Scan Data Block with CRC32 Bit-Rot Protection Verification
	scanLen := int64(endOffset - startOffset)
	dataBuf := make([]byte, scanLen)
	if _, err := r.file.ReadAt(dataBuf, int64(startOffset)); err != nil && err != io.EOF {
		return nil, false, false, fmt.Errorf("failed reading data block scan range: %w", err)
	}

	bufReader := bytes.NewReader(dataBuf)
	headerBuf := make([]byte, RecordHeaderSize)

	for bufReader.Len() > 0 {
		_, err := io.ReadFull(bufReader, headerBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, false, false, err
		}

		expectedCRC := binary.BigEndian.Uint32(headerBuf[0:4])
		keyLen := binary.BigEndian.Uint32(headerBuf[4:8])
		valLen := binary.BigEndian.Uint32(headerBuf[8:12])
		tombstone := headerBuf[12] == 1

		bodyLen := keyLen + valLen
		bodyBuf := make([]byte, bodyLen)

		if _, err := io.ReadFull(bufReader, bodyBuf); err != nil {
			break
		}

		// Reconstruct payload buffer for Bit-Rot CRC32 verification
		checkBuf := make([]byte, 9+bodyLen)
		copy(checkBuf[0:9], headerBuf[4:13])
		copy(checkBuf[9:], bodyBuf)

		actualCRC := crc32.ChecksumIEEE(checkBuf)
		if expectedCRC != actualCRC {
			return nil, false, false, fmt.Errorf("%w: sstable record crc mismatch (expected %d, got %d)", ErrSSTableCorrupted, expectedCRC, actualCRC)
		}

		currKey := bodyBuf[:keyLen]
		currVal := bodyBuf[keyLen:]

		if bytes.Equal(currKey, key) {
			valCopy := make([]byte, len(currVal))
			copy(valCopy, currVal)
			return valCopy, tombstone, true, nil
		}

		// Stop early if current key > target key (data block is strictly sorted)
		if bytes.Compare(currKey, key) > 0 {
			break
		}
	}

	return nil, false, false, nil
}

// Close releases the file handle and frees the native C++ Bloom filter memory.
func (r *SSTableReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.bloomFilter != nil {
		C.lumafly_destroy(r.bloomFilter)
		r.bloomFilter = nil
	}

	if r.file != nil {
		err := r.file.Close()
		r.file = nil
		return err
	}
	return nil
}
