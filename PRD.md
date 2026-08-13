# Product Requirements Document (PRD): HallownestKV

**Project Name:** `HallownestKV`  
**Module Name:** `github.com/utkarshraj/hallownestkv`  
**Primary Languages:** Go (1.22+) & C++ (C++17/20 via `cgo`)  
**Architecture Style:** Hybrid Go/C++ Distributed LSM-Tree Key-Value Store with Raft Consensus  

---

## 1. Executive Summary & Core Objective

`HallownestKV` is a high-performance, strongly consistent, fault-tolerant distributed key-value store built from scratch using a hybrid **Go** and **C++** architecture. It pairs a high-level concurrent Go control plane (Raft consensus, gRPC network transport, and log orchestration) with a ultra-low-latency **C++ storage & indexing engine** linked seamlessly via `cgo`.

The project is designed to demonstrate low-level systems engineering concepts: explicit disk format specification, zero-copy memory management, SIMD-accelerated hashing, C/C++ FFI via `cgo`, crash-recovery invariants, and distributed quorum systems.

### Lore-to-Systems Mapping

| Lore Term | Technical Component | Language / Layer | System Behavior / Function |
| :--- | :--- | :--- | :--- |
| **Geo** | Key-Value Pair | Go / C++ | Fundamental data payload stored and queried in the system. |
| **Soul** | MemTable | Go / C++ | In-memory SkipList write buffer before disk flush. |
| **Focus** | Flush Procedure | Go / C++ | Asynchronous transition of immutable MemTable to disk SSTable. |
| **Bench** | Write-Ahead Log (WAL) | Go | Append-only persistent record guaranteeing ACID durability on restart. |
| **Deepnest** | SSTable Files & Levels | C++ Core | Immutable, sorted multi-level disk storage (`Level 0` to `Level N`). |
| **Lumafly** | Bloom Filter | C++ (SIMD / Murmur3) | Probabilistic hash structure in C++ attached to SSTables to eliminate false disk reads. |
| **Abyss** | Compaction Engine | C++ Core | Background C++ multi-way merge process consolidating SSTables and clearing tombstones without Go GC pauses. |
| **Radiance** | Raft Leader Node | Go | Cluster node broadcasting state via heartbeats and driving consensus. |
| **Stagway** | gRPC Network Layer | Go | High-speed inter-node RPC protocol and client transport mechanism. |

---

## 2. Functional Requirements

### 2.1 Storage Engine (LSM-Tree)
* **Write-Ahead Log (Bench Engine - Go):**
  * Record format: `[4-byte CRC32][4-byte Key Length][4-byte Value Length][1-byte Tombstone Flag][Key Bytes][Value Bytes]`.
  * Synchronous or batched disk flushing via `fsync()`.
  * Sequential log recovery upon process launch.
* **In-Memory Write Buffer (Soul MemTable - Go / C++):**
  * Thread-safe concurrent SkipList implementation.
  * Threshold-based immutability triggering background disk flush (Default: `4MB`).
* **Disk Files & Indexing (Deepnest SSTables & Lumafly Filters - C++ via `cgo`):**
  * C++ native implementation in `cpp/src/` exposed to Go via `extern "C"` header (`cpp/include/lumafly_sstable.h`).
  * Immutable block layout: Data Block -> Sparse Index Block -> Bloom Filter Block -> Footer.
  * High-speed MurmurHash3 / xxHash C++ Lumafly Bloom Filters tuned for false positive probability $p \le 0.01$.
  * Binary search lookups over SSTable Sparse Indexes.
* **Garbage Collection (Abyss Compactor - C++ Engine):**
  * Asynchronous Leveled Compaction in C++ merging `Level 0` overlapping key ranges into disjoint `Level 1..N` files.
  * Deletion tombstone cleanup during compaction at the max level with zero Go GC overhead.

### 2.2 Distributed Consensus Layer (Radiance Raft - Go)
* **State Machine:**
  * Node roles: `Follower`, `Candidate`, `Leader`.
  * Persistent cluster state (`currentTerm`, `votedFor`, `log[]`) persisted to disk before responding to RPCs.
* **Leader Election:**
  * Randomized election timeout ($150\text{ ms} - 300\text{ ms}$) using Go channels and timers.
  * Majority vote acquisition ($N/2 + 1$) for leadership transition.
* **Log Replication & Consistency:**
  * `AppendEntries` heartbeats sent periodically ($50\text{ ms}$).
  * Strict index and term validation before entry commit.
  * Sub-millisecond local reads using **Read Index / Leader Lease**.

### 2.3 Network Protocol & Interfaces (Stagway Transport - Go)
* **gRPC API (`api/proto/hallownest.proto`):**
  * `PutGeo(key, value)`: Insert/update entry.
  * `GetGeo(key)`: Point lookup returning value or `404 Not Found`.
  * `FocusTombstone(key)`: Insert tombstone record for key deletion.
* **CLI Client (`cmd/hallownest-cli`):**
  * Interactive shell for interacting with active cluster nodes.

---

## 3. Non-Functional & Performance Targets

* **Throughput Target:** $\ge 50,000$ write ops/sec under 100 concurrent client connections on a single cluster node.
* **Latency Benchmarks:**
  * P99 Write Latency: $< 5\text{ ms}$
  * P99 Read Latency (Cache Hit / Lumafly Short-Circuit in C++): $< 1\text{ ms}$
  * P99 Read Latency (Disk Scan): $< 15\text{ ms}$
* **Durability Guarantee:** Zero data loss upon abrupt process termination (`SIGKILL`) for committed entries.
* **Fault Tolerance:** A 3-node cluster must survive 1 node crash without losing quorum or write capability.

---

## 4. Architecture & Data Flow

```
                      ┌────────────────────────────────────────┐
                      │    Client Application / CLI Client     │
                      └───────────────────┬────────────────────┘
                                          │ (gRPC Stagway Protocol)
                                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Layer 3: Network & API Server (Stagway Transport Layer - Go)               │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Layer 2: Distributed Consensus Engine (Radiance Raft Protocol - Go)        │
│  - Leader Election | Log Replication | Heartbeat Dispatcher | Read Index    │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │ (Committed Entries)
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Layer 1: Local Storage Engine (Hybrid Go / C++ LSM-Tree Core)              │
│                                                                             │
│  ┌─────────────────────────┐          ┌──────────────────────────────────┐  │
│  │ Bench Engine (WAL Log)  │          │ Soul MemTable (SkipList Buffer)  │  │
│  │       (Go Engine)       │          │           (Go Engine)            │  │
│  └────────────┬────────────┘          └─────────────────┬────────────────┘  │
│               │ (Crash Recovery)                        │ (Focus Flush)     │
│               └───────────────────────┬─────────────────┘                   │
│                                       ▼                                     │
│                ┌──────────────────────────────────────────────┐             │
│                │ `cgo` FFI Bridge (extern "C" C++ bindings)    │             │
│                └──────────────────────┬───────────────────────┘             │
│                                       ▼                                     │
│                ┌──────────────────────────────────────────────┐             │
│                │ Deepnest SSTables & Lumafly Filters (C++ Core)│             │
│                │  ├── C++ Lumafly Murmur3 Bloom Filters       │             │
│                │  └── C++ Abyss Leveled Compactor Engine      │             │
│                └──────────────────────────────────────────────┘             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Development Roadmap & Milestones

### Phase 1: Local Storage Engine Core
* [x] Initialize Go module and repository structure (`hallownestkv`).
* [x] Implement persistent Write-Ahead Log (`pkg/bench/wal.go`) with CRC32 checksums.
* [x] Implement concurrent SkipList MemTable (`pkg/storage/memtable.go`).
* [ ] Build native C++ Lumafly Bloom Filter & Deepnest SSTable Builder with `cgo` bindings (`cpp/` & `pkg/storage/sstable.go`).

### Phase 2: Compaction & Recovery
* [ ] Build WAL crash recovery replay module.
* [ ] Implement background C++ Abyss Leveled Compactor (`cpp/src/abyss_compactor.cpp` & `pkg/storage/compaction.go`).

### Phase 3: Consensus & Cluster Network
* [ ] Define Protobuf RPC schemas (`api/proto/hallownest.proto`).
* [ ] Implement Radiance Raft election timer, vote handler, and log replication loops (`pkg/consensus/`).
* [ ] Wire up Stagway gRPC server (`pkg/network/`).

### Phase 4: Benchmarking, Testing & CI/CD
* [ ] Implement chaos fault-injection tests (simulating `SIGKILL` and network splits).
* [ ] Set up automated benchmarks (`go test -bench` & C++ benchmarks) and GitHub Actions workflow.

---

## 6. How to Run & Verify

### Build Targets
```bash
# Build C++ native libraries and Go binaries
make build

# Run unit tests with race detection enabled
make test

# Execute engine performance benchmarks
make bench
```

### Quickstart Local Cluster
```bash
# Spin up a 3-node cluster locally using Docker
docker-compose up -d

# Execute a PutGeo request via CLI
./bin/hallownest-cli put --key "knight" --value "vessel"

# Execute a GetGeo request
./bin/hallownest-cli get --key "knight"
```
