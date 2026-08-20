package network

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/utkarshraj/hallownestkv/pkg/bench"
	"github.com/utkarshraj/hallownestkv/pkg/consensus"
	"github.com/utkarshraj/hallownestkv/pkg/storage"
)

var (
	ErrServerClosed = errors.New("stagway: server is closed")
	ErrNotLeader    = errors.New("stagway: node is not raft leader")
)

const (
	OpPut       byte = 0x01
	OpGet       byte = 0x02
	OpTombstone byte = 0x03
)

// StagwayServerOptions defines configuration for the gRPC & TCP network transport server.
type StagwayServerOptions struct {
	Addr   string
	NodeID int
}

// DefaultStagwayServerOptions provides default network server options.
func DefaultStagwayServerOptions(addr string, nodeID int) StagwayServerOptions {
	if addr == "" {
		addr = "127.0.0.1:50051"
	}
	return StagwayServerOptions{
		Addr:   addr,
		NodeID: nodeID,
	}
}

// StagwayServer is the high-performance network transport server for HallownestKV.
type StagwayServer struct {
	mu       sync.RWMutex
	opts     StagwayServerOptions
	listener net.Listener
	raft     *consensus.RaftNode
	memTable *storage.MemTable
	wal      *bench.WAL
	running  bool
}

// NewStagwayServer creates a new Stagway network server instance.
func NewStagwayServer(opts StagwayServerOptions, raft *consensus.RaftNode, memTable *storage.MemTable, wal *bench.WAL) *StagwayServer {
	return &StagwayServer{
		opts:     opts,
		raft:     raft,
		memTable: memTable,
		wal:      wal,
	}
}

// Start opens TCP listener and begins accepting client connections.
func (s *StagwayServer) Start() error {
	s.mu.Lock()
	listener, err := net.Listen("tcp", s.opts.Addr)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to listen on %s: %w", s.opts.Addr, err)
	}

	s.listener = listener
	s.running = true
	s.mu.Unlock()

	go s.acceptLoop()
	return nil
}

func (s *StagwayServer) acceptLoop() {
	for {
		s.mu.RLock()
		if !s.running {
			s.mu.RUnlock()
			return
		}
		listener := s.listener
		s.mu.RUnlock()

		conn, err := listener.Accept()
		if err != nil {
			s.mu.RLock()
			running := s.running
			s.mu.RUnlock()
			if !running {
				return
			}
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *StagwayServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	headerBuf := make([]byte, 9) // Op (1) + KeyLen (4) + ValLen (4)

	for {
		_, err := io.ReadFull(conn, headerBuf)
		if err != nil {
			return
		}

		// Detect HTTP/2 Preface from gRPC tools (grpcui / grpcurl / Postman)
		if headerBuf[0] == 'P' && headerBuf[1] == 'R' && headerBuf[2] == 'I' && headerBuf[3] == ' ' {
			s.handleGRPCConnection(conn)
			return
		}

		op := headerBuf[0]
		keyLen := binary.BigEndian.Uint32(headerBuf[1:5])
		valLen := binary.BigEndian.Uint32(headerBuf[5:9])

		bodyBuf := make([]byte, keyLen+valLen)
		if _, err := io.ReadFull(conn, bodyBuf); err != nil {
			return
		}

		key := bodyBuf[:keyLen]
		val := bodyBuf[keyLen:]

		switch op {
		case OpPut:
			idx, term, isLeader, err := s.PutGeo(key, val)
			s.sendWriteResponse(conn, idx, term, isLeader, err)

		case OpGet:
			resVal, tombstone, found, err := s.GetGeo(key)
			s.sendReadResponse(conn, resVal, tombstone, found, err)

		case OpTombstone:
			idx, term, isLeader, err := s.FocusTombstone(key)
			s.sendWriteResponse(conn, idx, term, isLeader, err)
		}
	}
}

func (s *StagwayServer) handleGRPCConnection(conn net.Conn) {
	// Send HTTP/2 SETTINGS frame (empty settings)
	settingsFrame := []byte{
		0x00, 0x00, 0x00, // Length: 0
		0x04,             // Type: SETTINGS (4)
		0x00,             // Flags: 0
		0x00, 0x00, 0x00, 0x00, // Stream ID: 0
	}
	_, _ = conn.Write(settingsFrame)

	// Send HTTP/2 SETTINGS ACK
	ackFrame := []byte{
		0x00, 0x00, 0x00, // Length: 0
		0x04,             // Type: SETTINGS (4)
		0x01,             // Flags: ACK (1)
		0x00, 0x00, 0x00, 0x00, // Stream ID: 0
	}
	_, _ = conn.Write(ackFrame)
}

func (s *StagwayServer) sendWriteResponse(conn net.Conn, index, term int, isLeader bool, err error) {
	resp := make([]byte, 10)
	if isLeader && err == nil {
		resp[0] = 1 // Success
	} else {
		resp[0] = 0 // Failure
	}
	binary.BigEndian.PutUint32(resp[1:5], uint32(index))
	binary.BigEndian.PutUint32(resp[5:9], uint32(term))
	if isLeader {
		resp[9] = 1
	} else {
		resp[9] = 0
	}
	_, _ = conn.Write(resp)
}

func (s *StagwayServer) sendReadResponse(conn net.Conn, val []byte, tombstone, found bool, err error) {
	valLen := uint32(len(val))
	resp := make([]byte, 6+valLen)
	if found && err == nil {
		resp[0] = 1
	} else {
		resp[0] = 0
	}
	if tombstone {
		resp[1] = 1
	} else {
		resp[1] = 0
	}
	binary.BigEndian.PutUint32(resp[2:6], valLen)
	if valLen > 0 {
		copy(resp[6:], val)
	}
	_, _ = conn.Write(resp)
}

// PutGeo processes a client write operation through Raft consensus & LSM storage engine.
func (s *StagwayServer) PutGeo(key, value []byte) (int, int, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.running {
		return -1, -1, false, ErrServerClosed
	}

	// 1. Submit through Raft consensus if cluster active
	if s.raft != nil {
		idx, term, isLeader := s.raft.Submit(key, value, false)
		if !isLeader {
			return -1, term, false, ErrNotLeader
		}

		// Also log to local WAL and MemTable
		if s.wal != nil {
			_, _ = s.wal.Append(key, value, false)
		}
		if s.memTable != nil {
			s.memTable.Put(key, value)
		}

		return idx, term, true, nil
	}

	// Local standalone write mode
	if s.wal != nil {
		_, _ = s.wal.Append(key, value, false)
	}
	if s.memTable != nil {
		s.memTable.Put(key, value)
	}

	return 1, 1, true, nil
}

// GetGeo processes a client point lookup query from the in-memory MemTable.
func (s *StagwayServer) GetGeo(key []byte) ([]byte, bool, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.running {
		return nil, false, false, ErrServerClosed
	}

	if s.memTable != nil {
		val, tombstone, found := s.memTable.Get(key)
		return val, tombstone, found, nil
	}

	return nil, false, false, nil
}

// FocusTombstone processes a client key deletion operation.
func (s *StagwayServer) FocusTombstone(key []byte) (int, int, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.running {
		return -1, -1, false, ErrServerClosed
	}

	if s.raft != nil {
		idx, term, isLeader := s.raft.Submit(key, nil, true)
		if !isLeader {
			return -1, term, false, ErrNotLeader
		}

		if s.wal != nil {
			_, _ = s.wal.Append(key, nil, true)
		}
		if s.memTable != nil {
			s.memTable.Delete(key)
		}

		return idx, term, true, nil
	}

	if s.wal != nil {
		_, _ = s.wal.Append(key, nil, true)
	}
	if s.memTable != nil {
		s.memTable.Delete(key)
	}

	return 1, 1, true, nil
}

// Stop cleanly shuts down the network listener.
func (s *StagwayServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	s.running = false
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}
