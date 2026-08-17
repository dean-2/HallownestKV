package network

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/utkarshraj/hallownestkv/pkg/bench"
	"github.com/utkarshraj/hallownestkv/pkg/storage"
)

func TestStagwayServerPutGetDelete(t *testing.T) {
	tempDir := t.TempDir()
	walOpts := bench.DefaultWALOptions(tempDir)
	wal, err := bench.OpenWAL(walOpts)
	if err != nil {
		t.Fatalf("failed opening wal: %v", err)
	}
	defer wal.Close()

	mem := storage.NewMemTable(storage.DefaultMemTableOptions())

	// Bind to 127.0.0.1:0 for dynamic OS port allocation
	opts := DefaultStagwayServerOptions("127.0.0.1:0", 1)
	server := NewStagwayServer(opts, nil, mem, wal)

	if err := server.Start(); err != nil {
		t.Fatalf("failed starting stagway server: %v", err)
	}
	defer server.Stop()

	// Retrieve actual bound TCP address
	addr := server.listener.Addr().String()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed dialing stagway server at %s: %v", addr, err)
	}
	defer conn.Close()

	// 1. Send PutGeo: key="geo_knight", val="vessel_pure"
	key := []byte("geo_knight")
	val := []byte("vessel_pure")
	keyLen := uint32(len(key))
	valLen := uint32(len(val))

	reqBuf := make([]byte, 9+keyLen+valLen)
	reqBuf[0] = OpPut
	binary.BigEndian.PutUint32(reqBuf[1:5], keyLen)
	binary.BigEndian.PutUint32(reqBuf[5:9], valLen)
	copy(reqBuf[9:9+keyLen], key)
	copy(reqBuf[9+keyLen:], val)

	if _, err := conn.Write(reqBuf); err != nil {
		t.Fatalf("failed writing Put request: %v", err)
	}

	respBuf := make([]byte, 10)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		t.Fatalf("failed reading Put response: %v", err)
	}
	if respBuf[0] != 1 {
		t.Fatalf("expected Put response success=1, got %d", respBuf[0])
	}

	// 2. Send GetGeo: key="geo_knight"
	getReqBuf := make([]byte, 9+keyLen)
	getReqBuf[0] = OpGet
	binary.BigEndian.PutUint32(getReqBuf[1:5], keyLen)
	binary.BigEndian.PutUint32(getReqBuf[5:9], 0)
	copy(getReqBuf[9:], key)

	if _, err := conn.Write(getReqBuf); err != nil {
		t.Fatalf("failed writing Get request: %v", err)
	}

	getHdrBuf := make([]byte, 6)
	if _, err := io.ReadFull(conn, getHdrBuf); err != nil {
		t.Fatalf("failed reading Get header: %v", err)
	}
	found := getHdrBuf[0] == 1
	tombstone := getHdrBuf[1] == 1
	respValLen := binary.BigEndian.Uint32(getHdrBuf[2:6])

	if !found || tombstone {
		t.Fatalf("expected key to be found & active, got found=%v, tombstone=%v", found, tombstone)
	}

	respVal := make([]byte, respValLen)
	if _, err := io.ReadFull(conn, respVal); err != nil {
		t.Fatalf("failed reading Get value payload: %v", err)
	}

	if !bytes.Equal(respVal, val) {
		t.Fatalf("expected value %s, got %s", string(val), string(respVal))
	}

	// 3. Send FocusTombstone: key="geo_knight"
	delReqBuf := make([]byte, 9+keyLen)
	delReqBuf[0] = OpTombstone
	binary.BigEndian.PutUint32(delReqBuf[1:5], keyLen)
	binary.BigEndian.PutUint32(delReqBuf[5:9], 0)
	copy(delReqBuf[9:], key)

	if _, err := conn.Write(delReqBuf); err != nil {
		t.Fatalf("failed writing Tombstone request: %v", err)
	}

	delRespBuf := make([]byte, 10)
	if _, err := io.ReadFull(conn, delRespBuf); err != nil {
		t.Fatalf("failed reading Tombstone response: %v", err)
	}
	if delRespBuf[0] != 1 {
		t.Fatalf("expected Tombstone response success=1, got %d", delRespBuf[0])
	}

	// Wait 10ms for state to settle
	time.Sleep(10 * time.Millisecond)

	// 4. Verify GetGeo returns tombstone=true
	if _, err := conn.Write(getReqBuf); err != nil {
		t.Fatalf("failed writing final Get request: %v", err)
	}
	if _, err := io.ReadFull(conn, getHdrBuf); err != nil {
		t.Fatalf("failed reading final Get header: %v", err)
	}
	found = getHdrBuf[0] == 1
	tombstone = getHdrBuf[1] == 1

	if !found || !tombstone {
		t.Fatalf("expected key to return tombstone=true after deletion, got found=%v, tombstone=%v", found, tombstone)
	}
}
