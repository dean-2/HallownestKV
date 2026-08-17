package consensus

import (
	"bytes"
	"testing"
	"time"
)

func TestInitialLeaderElection(t *testing.T) {
	net := NewMockNetwork()

	nodeIDs := []int{0, 1, 2}
	nodes := make(map[int]*RaftNode)

	for _, id := range nodeIDs {
		peers := []int{}
		for _, p := range nodeIDs {
			if p != id {
				peers = append(peers, p)
			}
		}

		applyCh := make(chan ApplyMsg, 100)
		node := NewRaftNode(id, peers, net, applyCh)
		nodes[id] = node
		net.RegisterNode(id, node)
	}

	defer func() {
		for _, node := range nodes {
			node.Kill()
		}
	}()

	// Wait for election timeout & heartbeat cycle
	time.Sleep(500 * time.Millisecond)

	leaderCount := 0
	leaderTerm := 0

	for id, node := range nodes {
		term, isLeader := node.GetState()
		if isLeader {
			leaderCount++
			leaderTerm = term
			t.Logf("Node %d is elected Leader in Term %d", id, term)
		}
	}

	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader in cluster, got %d", leaderCount)
	}
	if leaderTerm < 1 {
		t.Fatalf("expected term >= 1, got %d", leaderTerm)
	}
}

func TestReElectionOnLeaderCrash(t *testing.T) {
	net := NewMockNetwork()

	nodeIDs := []int{0, 1, 2}
	nodes := make(map[int]*RaftNode)

	for _, id := range nodeIDs {
		peers := []int{}
		for _, p := range nodeIDs {
			if p != id {
				peers = append(peers, p)
			}
		}

		applyCh := make(chan ApplyMsg, 100)
		node := NewRaftNode(id, peers, net, applyCh)
		nodes[id] = node
		net.RegisterNode(id, node)
	}

	defer func() {
		for _, node := range nodes {
			node.Kill()
		}
	}()

	time.Sleep(500 * time.Millisecond)

	var oldLeaderID int = -1
	var oldTerm int = 0

	for id, node := range nodes {
		term, isLeader := node.GetState()
		if isLeader {
			oldLeaderID = id
			oldTerm = term
			break
		}
	}

	if oldLeaderID == -1 {
		t.Fatalf("expected initial leader to be elected")
	}

	t.Logf("Simulating crash on Leader Node %d (Term %d)", oldLeaderID, oldTerm)
	nodes[oldLeaderID].Kill()
	net.RegisterNode(oldLeaderID, nil) // Network disconnect

	// Wait for follower election timeout
	time.Sleep(600 * time.Millisecond)

	newLeaderCount := 0
	newLeaderID := -1
	newTerm := 0

	for id, node := range nodes {
		if id == oldLeaderID {
			continue
		}
		term, isLeader := node.GetState()
		if isLeader {
			newLeaderCount++
			newLeaderID = id
			newTerm = term
		}
	}

	if newLeaderCount != 1 {
		t.Fatalf("expected 1 new leader elected after old leader crash, got %d", newLeaderCount)
	}

	if newTerm <= oldTerm {
		t.Fatalf("expected new term > old term (%d), got %d", oldTerm, newTerm)
	}

	t.Logf("Node %d successfully elected new Leader in Term %d after Node %d crash", newLeaderID, newTerm, oldLeaderID)
}

func TestLogReplication(t *testing.T) {
	net := NewMockNetwork()

	nodeIDs := []int{0, 1, 2}
	nodes := make(map[int]*RaftNode)
	applyChans := make(map[int]chan ApplyMsg)

	for _, id := range nodeIDs {
		peers := []int{}
		for _, p := range nodeIDs {
			if p != id {
				peers = append(peers, p)
			}
		}

		applyCh := make(chan ApplyMsg, 100)
		applyChans[id] = applyCh
		node := NewRaftNode(id, peers, net, applyCh)
		nodes[id] = node
		net.RegisterNode(id, node)
	}

	defer func() {
		for _, node := range nodes {
			node.Kill()
		}
	}()

	time.Sleep(500 * time.Millisecond)

	var leaderID int = -1
	for id, node := range nodes {
		_, isLeader := node.GetState()
		if isLeader {
			leaderID = id
			break
		}
	}

	if leaderID == -1 {
		t.Fatalf("expected leader to be elected")
	}

	// Submit key-value transaction to Leader
	key := []byte("knight_geo_100")
	val := []byte("radiance_committed")

	index, term, isLeader := nodes[leaderID].Submit(key, val, false)
	if !isLeader {
		t.Fatalf("expected Submit to succeed on leader node %d", leaderID)
	}
	if index <= 0 {
		t.Fatalf("expected positive log index, got %d", index)
	}

	t.Logf("Submitted command to Leader %d at Index %d (Term %d)", leaderID, index, term)

	// Wait for majority replication & commit
	time.Sleep(400 * time.Millisecond)

	appliedCount := 0
	for id, applyCh := range applyChans {
		select {
		case msg := <-applyCh:
			if !msg.CommandValid {
				t.Errorf("Node %d received invalid command", id)
			}
			if !bytes.Equal(msg.Key, key) || !bytes.Equal(msg.Value, val) {
				t.Errorf("Node %d payload mismatch: key=%s, val=%s", id, string(msg.Key), string(msg.Value))
			}
			appliedCount++
		default:
			// Node has not applied yet or is slow
		}
	}

	if appliedCount < 2 { // Majority of 3 nodes is 2
		t.Fatalf("expected at least 2 nodes to apply committed entry, got %d", appliedCount)
	}

	t.Logf("Successfully replicated and applied command across %d nodes", appliedCount)
}
