package consensus

import (
	"math/rand"
	"sync"
	"time"
)

const (
	HeartbeatInterval  = 50 * time.Millisecond
	MinElectionTimeout = 150 * time.Millisecond
	MaxElectionTimeout = 300 * time.Millisecond
)

// RaftNode represents an active participant node in the Radiance Raft cluster.
type RaftNode struct {
	mu        sync.Mutex
	id        int
	peers     []int
	transport PeerTransport

	// Persistent state on all nodes
	currentTerm int
	votedFor    int
	log         []RaftLogEntry

	// Volatile state on all nodes
	commitIndex int
	lastApplied int
	role        Role

	// Volatile state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int

	// Timers, channels, and deadline
	electionDeadline time.Time
	applyCh          chan ApplyMsg
	killed           bool
	rnd              *rand.Rand
}

// NewRaftNode initializes a new RaftNode.
func NewRaftNode(id int, peers []int, transport PeerTransport, applyCh chan ApplyMsg) *RaftNode {
	r := &RaftNode{
		id:          id,
		peers:       peers,
		transport:   transport,
		currentTerm: 0,
		votedFor:    -1,
		role:        Follower,
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[int]int),
		matchIndex:  make(map[int]int),
		applyCh:     applyCh,
		rnd:         rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*10000)),
	}

	// 1-indexed log (dummy entry at index 0)
	r.log = append(r.log, RaftLogEntry{Index: 0, Term: 0})

	r.resetElectionTimeout()
	go r.runElectionLoop()

	return r
}

func (r *RaftNode) randomElectionDuration() time.Duration {
	delta := int64(MaxElectionTimeout - MinElectionTimeout)
	extra := time.Duration(r.rnd.Int63n(delta))
	return MinElectionTimeout + extra
}

func (r *RaftNode) resetElectionTimeout() {
	r.electionDeadline = time.Now().Add(r.randomElectionDuration())
}

func (r *RaftNode) runElectionLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.mu.Lock()
			if r.killed {
				r.mu.Unlock()
				return
			}
			if r.role != Leader && time.Now().After(r.electionDeadline) {
				r.startElection()
				r.resetElectionTimeout()
			}
			r.mu.Unlock()
		}
	}
}

// startElection transitions node to Candidate and requests votes from peers.
func (r *RaftNode) startElection() {
	r.role = Candidate
	r.currentTerm++
	r.votedFor = r.id
	term := r.currentTerm
	candidateID := r.id

	lastLogIndex := len(r.log) - 1
	lastLogTerm := r.log[lastLogIndex].Term

	votesReceived := 1 // Vote for self
	totalPeers := len(r.peers) + 1
	majorityNeeded := (totalPeers / 2) + 1

	if votesReceived >= majorityNeeded {
		r.becomeLeader()
		return
	}

	var voteMu sync.Mutex

	for _, peerID := range r.peers {
		go func(pID int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  candidateID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}

			if r.transport.SendRequestVote(pID, &args, &reply) {
				r.mu.Lock()
				defer r.mu.Unlock()

				if r.role == Candidate && r.currentTerm == term {
					if reply.Term > r.currentTerm {
						r.becomeFollower(reply.Term)
						return
					}
					if reply.VoteGranted {
						voteMu.Lock()
						votesReceived++
						granted := votesReceived
						voteMu.Unlock()

						if granted >= majorityNeeded && r.role == Candidate {
							r.becomeLeader()
						}
					}
				}
			}
		}(peerID)
	}
}

func (r *RaftNode) becomeFollower(term int) {
	r.role = Follower
	r.currentTerm = term
	r.votedFor = -1
	r.resetElectionTimeout()
}

func (r *RaftNode) becomeLeader() {
	r.role = Leader
	for _, pID := range r.peers {
		r.nextIndex[pID] = len(r.log)
		r.matchIndex[pID] = 0
	}
	r.broadcastHeartbeats()
	go r.runHeartbeatLoop()
}

func (r *RaftNode) runHeartbeatLoop() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.mu.Lock()
			if r.killed || r.role != Leader {
				r.mu.Unlock()
				return
			}
			r.broadcastHeartbeats()
			r.mu.Unlock()
		}
	}
}

func (r *RaftNode) broadcastHeartbeats() {
	if r.role != Leader {
		return
	}

	term := r.currentTerm
	leaderID := r.id
	leaderCommit := r.commitIndex

	for _, peerID := range r.peers {
		prevIndex := r.nextIndex[peerID] - 1
		if prevIndex < 0 {
			prevIndex = 0
		}
		prevTerm := r.log[prevIndex].Term

		entries := make([]RaftLogEntry, len(r.log[prevIndex+1:]))
		copy(entries, r.log[prevIndex+1:])

		go func(pID int, pIdx int, pTerm int, entriesToSend []RaftLogEntry) {
			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: pIdx,
				PrevLogTerm:  pTerm,
				Entries:      entriesToSend,
				LeaderCommit: leaderCommit,
			}
			reply := AppendEntriesReply{}

			if r.transport.SendAppendEntries(pID, &args, &reply) {
				r.mu.Lock()
				defer r.mu.Unlock()

				if r.role == Leader && r.currentTerm == term {
					if reply.Term > r.currentTerm {
						r.becomeFollower(reply.Term)
						return
					}

					if reply.Success {
						r.nextIndex[pID] = pIdx + len(entriesToSend) + 1
						r.matchIndex[pID] = r.nextIndex[pID] - 1
						r.checkAdvanceCommitIndex()
					} else {
						if r.nextIndex[pID] > 1 {
							r.nextIndex[pID]--
						}
					}
				}
			}
		}(peerID, prevIndex, prevTerm, entries)
	}
}

func (r *RaftNode) checkAdvanceCommitIndex() {
	for N := len(r.log) - 1; N > r.commitIndex; N-- {
		if r.log[N].Term != r.currentTerm {
			continue
		}

		count := 1 // Count self
		for _, pID := range r.peers {
			if r.matchIndex[pID] >= N {
				count++
			}
		}

		majorityNeeded := ((len(r.peers) + 1) / 2) + 1
		if count >= majorityNeeded {
			r.commitIndex = N
			r.applyLogs()
			break
		}
	}
}

func (r *RaftNode) applyLogs() {
	for r.commitIndex > r.lastApplied {
		r.lastApplied++
		entry := r.log[r.lastApplied]

		if r.applyCh != nil && entry.Index > 0 {
			r.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
				Key:          entry.Key,
				Value:        entry.Value,
				Tombstone:    entry.Tombstone,
			}
		}
	}
}

// RequestVote handles vote request from Candidate node.
func (r *RaftNode) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}

	reply.Term = r.currentTerm
	reply.VoteGranted = false

	if args.Term < r.currentTerm {
		return nil
	}

	lastLogIndex := len(r.log) - 1
	lastLogTerm := r.log[lastLogIndex].Term

	// Check if candidate's log is at least as up-to-date as receiver's log
	logUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		logUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		logUpToDate = true
	}

	if (r.votedFor == -1 || r.votedFor == args.CandidateID) && logUpToDate {
		r.votedFor = args.CandidateID
		reply.VoteGranted = true
		r.resetElectionTimeout()
	}

	return nil
}

// AppendEntries handles heartbeat & log entry replication from Leader.
func (r *RaftNode) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}

	reply.Term = r.currentTerm
	reply.Success = false

	if args.Term < r.currentTerm {
		return nil
	}

	r.resetElectionTimeout()
	if r.role != Follower {
		r.role = Follower
	}

	// Verify log consistency at PrevLogIndex & PrevLogTerm
	if args.PrevLogIndex >= len(r.log) || r.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return nil
	}

	// Append any new entries not already in log
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(r.log) {
			if r.log[idx].Term != entry.Term {
				r.log = r.log[:idx] // Truncate conflicting log entries
				r.log = append(r.log, entry)
			}
		} else {
			r.log = append(r.log, entry)
		}
	}

	reply.Success = true

	if args.LeaderCommit > r.commitIndex {
		if args.LeaderCommit < len(r.log)-1 {
			r.commitIndex = args.LeaderCommit
		} else {
			r.commitIndex = len(r.log) - 1
		}
		r.applyLogs()
	}

	return nil
}

// Submit allows clients to propose key-value write commands to the cluster.
// Returns (index, term, isLeader).
func (r *RaftNode) Submit(key, value []byte, tombstone bool) (int, int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role != Leader {
		return -1, r.currentTerm, false
	}

	index := len(r.log)
	term := r.currentTerm

	entry := RaftLogEntry{
		Index:     index,
		Term:      term,
		Key:       key,
		Value:     value,
		Tombstone: tombstone,
	}

	r.log = append(r.log, entry)
	r.broadcastHeartbeats()

	return index, term, true
}

// GetState returns (currentTerm, isLeader).
func (r *RaftNode) GetState() (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.role == Leader
}

// Kill cleanly shuts down the RaftNode timers.
func (r *RaftNode) Kill() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.killed = true
}
