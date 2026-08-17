package consensus

import (
	"fmt"
	"sync"
)

// Role represents the Raft consensus state machine role of a node.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// RaftLogEntry represents a log entry stored in the Raft log sequence.
type RaftLogEntry struct {
	Index     int
	Term      int
	Key       []byte
	Value     []byte
	Tombstone bool
}

// RequestVoteArgs is sent by Candidates to request votes during leader election.
type RequestVoteArgs struct {
	Term         int // Candidate's current term
	CandidateID  int // Candidate requesting vote
	LastLogIndex int // Index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

// RequestVoteReply is the response sent by Followers to a RequestVote request.
type RequestVoteReply struct {
	Term        int  // CurrentTerm for candidate to update itself
	VoteGranted bool // True means candidate received vote
}

// AppendEntriesArgs is sent by Leader to replicate log entries & send periodic heartbeats.
type AppendEntriesArgs struct {
	Term         int            // Leader's current term
	LeaderID     int            // So follower can redirect clients
	PrevLogIndex int            // Index of log entry immediately preceding new ones
	PrevLogTerm  int            // Term of PrevLogIndex entry
	Entries      []RaftLogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int            // Leader's commitIndex
}

// AppendEntriesReply is the response sent by Followers to an AppendEntries request.
type AppendEntriesReply struct {
	Term    int  // CurrentTerm for leader to update itself
	Success bool // True if follower contained entry matching PrevLogIndex & PrevLogTerm
}

// ApplyMsg is sent to the application state machine (LSM Storage Engine) when log entries are committed.
type ApplyMsg struct {
	CommandValid bool
	CommandIndex int
	CommandTerm  int
	Key          []byte
	Value        []byte
	Tombstone    bool
}

// PeerTransport defines the interface for communicating between Raft nodes in a cluster.
type PeerTransport interface {
	SendRequestVote(peerID int, args *RequestVoteArgs, reply *RequestVoteReply) bool
	SendAppendEntries(peerID int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool
}

// MockNetwork implements PeerTransport for local multi-node in-memory testing.
type MockNetwork struct {
	mu    sync.RWMutex
	nodes map[int]*RaftNode
}

// NewMockNetwork creates a local test network connecting Raft nodes.
func NewMockNetwork() *MockNetwork {
	return &MockNetwork{
		nodes: make(map[int]*RaftNode),
	}
}

// RegisterNode registers a RaftNode in the test network.
func (m *MockNetwork) RegisterNode(id int, node *RaftNode) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[id] = node
}

// SendRequestVote delivers RequestVote RPC to target node.
func (m *MockNetwork) SendRequestVote(peerID int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	m.mu.RLock()
	targetNode, ok := m.nodes[peerID]
	m.mu.RUnlock()

	if !ok || targetNode == nil {
		return false
	}

	err := targetNode.RequestVote(*args, reply)
	return err == nil
}

// SendAppendEntries delivers AppendEntries RPC to target node.
func (m *MockNetwork) SendAppendEntries(peerID int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	m.mu.RLock()
	targetNode, ok := m.nodes[peerID]
	m.mu.RUnlock()

	if !ok || targetNode == nil {
		return false
	}

	err := targetNode.AppendEntries(*args, reply)
	return err == nil
}

func fmtNode(id int, term int, role Role) string {
	return fmt.Sprintf("[Node %d | Term %d | %s]", id, term, role)
}
