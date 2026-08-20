package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/utkarshraj/hallownestkv/pkg/bench"
	"github.com/utkarshraj/hallownestkv/pkg/consensus"
	"github.com/utkarshraj/hallownestkv/pkg/network"
	"github.com/utkarshraj/hallownestkv/pkg/storage"
)

type PutJSONRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type ResponseJSON struct {
	Success   bool   `json:"success"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value,omitempty"`
	Found     bool   `json:"found,omitempty"`
	Tombstone bool   `json:"tombstone,omitempty"`
	Index     int    `json:"index,omitempty"`
	Term      int    `json:"term,omitempty"`
	Message   string `json:"message,omitempty"`
}

const DashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HallownestKV — Distributed Engine Control Console</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&family=Fira+Code:wght@400;500&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-dark: #090D16;
            --card-bg: rgba(15, 23, 42, 0.75);
            --card-border: rgba(56, 189, 248, 0.2);
            --primary-cyan: #38BDF8;
            --accent-blue: #2563EB;
            --accent-green: #10B981;
            --accent-red: #EF4444;
            --text-main: #F8FAFC;
            --text-muted: #94A3B8;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', sans-serif; }
        body { background-color: var(--bg-dark); color: var(--text-main); min-height: 100vh; padding: 24px; background-image: radial-gradient(circle at 50% 0%, rgba(37, 99, 235, 0.15), transparent 70%); }
        .container { max-width: 1200px; margin: 0 auto; }
        
        header { display: flex; justify-content: space-between; align-items: center; padding-bottom: 24px; border-bottom: 1px solid var(--card-border); margin-bottom: 28px; }
        .logo { font-size: 24px; font-weight: 700; background: linear-gradient(135deg, #38BDF8, #818CF8); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .status-badge { background: rgba(16, 185, 129, 0.15); border: 1px solid var(--accent-green); color: var(--accent-green); padding: 6px 14px; border-radius: 20px; font-size: 13px; font-weight: 600; display: flex; align-items: center; gap: 8px; }
        .status-dot { width: 8px; height: 8px; background: var(--accent-green); border-radius: 50%; box-shadow: 0 0 10px var(--accent-green); }

        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 20px; margin-bottom: 28px; }
        .card { background: var(--card-bg); border: 1px solid var(--card-border); border-radius: 16px; padding: 20px; backdrop-filter: blur(12px); box-shadow: 0 8px 32px rgba(0,0,0,0.3); }
        .card h3 { font-size: 12px; text-transform: uppercase; color: var(--text-muted); letter-spacing: 1px; margin-bottom: 12px; }
        .card .metric { font-size: 26px; font-weight: 700; color: var(--primary-cyan); font-family: 'Fira Code', monospace; }

        .console-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
        @media (max-width: 900px) { .console-grid { grid-template-columns: 1fr; } }

        .form-group { margin-bottom: 16px; }
        label { display: block; font-size: 13px; color: var(--text-muted); margin-bottom: 6px; font-weight: 500; }
        input { width: 100%; padding: 12px 14px; background: rgba(2, 6, 23, 0.6); border: 1px solid var(--card-border); border-radius: 8px; color: var(--text-main); font-family: 'Fira Code', monospace; font-size: 14px; outline: none; }
        input:focus { border-color: var(--primary-cyan); box-shadow: 0 0 12px rgba(56, 189, 248, 0.3); }

        .btn-group { display: flex; gap: 12px; }
        button { flex: 1; padding: 12px; border: none; border-radius: 8px; font-weight: 600; cursor: pointer; transition: all 0.2s ease; }
        .btn-primary { background: linear-gradient(135deg, #2563EB, #38BDF8); color: white; }
        .btn-primary:hover { opacity: 0.9; transform: translateY(-1px); }
        .btn-danger { background: rgba(239, 68, 68, 0.2); border: 1px solid var(--accent-red); color: var(--accent-red); }
        .btn-danger:hover { background: rgba(239, 68, 68, 0.3); }

        .log-box { background: rgba(2, 6, 23, 0.8); border: 1px solid var(--card-border); border-radius: 12px; padding: 16px; height: 320px; overflow-y: auto; font-family: 'Fira Code', monospace; font-size: 12px; }
        .log-entry { margin-bottom: 8px; padding-bottom: 8px; border-bottom: 1px solid rgba(255,255,255,0.05); }
        .log-time { color: var(--text-muted); }
        .log-success { color: var(--accent-green); }
        .log-error { color: var(--accent-red); }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div>
                <div class="logo">HALLOWNEST-KV</div>
                <div style="font-size: 12px; color: var(--text-muted);">Distributed Hybrid Go/C++ Storage Engine Console</div>
            </div>
            <div class="status-badge">
                <div class="status-dot"></div>
                <span id="nodeStatus">CLUSTER ACTIVE</span>
            </div>
        </header>

        <div class="grid">
            <div class="card">
                <h3>Node Role</h3>
                <div class="metric" id="nodeRole">LEADER</div>
            </div>
            <div class="card">
                <h3>Raft Term</h3>
                <div class="metric" id="nodeTerm">1</div>
            </div>
            <div class="card">
                <h3>MemTable Size</h3>
                <div class="metric" id="nodeMem">0 B</div>
            </div>
            <div class="card">
                <h3>Network Ports</h3>
                <div style="font-size: 14px; font-family: 'Fira Code'; color: var(--text-main);">
                    gRPC: <span style="color: var(--primary-cyan);">:50051</span><br>REST: <span style="color: var(--primary-cyan);">:8080</span>
                </div>
            </div>
        </div>

        <div class="console-grid">
            <div class="card">
                <h3 style="margin-bottom: 16px; font-size: 14px; color: var(--text-main);">Interactive Storage Console</h3>
                <div class="form-group">
                    <label>Target Key</label>
                    <input type="text" id="inputKey" placeholder="e.g. geo_account_1001">
                </div>
                <div class="form-group">
                    <label>Value Payload</label>
                    <input type="text" id="inputValue" placeholder="e.g. balance_5000">
                </div>
                <div class="btn-group">
                    <button class="btn-primary" onclick="doPut()">PUT (Store)</button>
                    <button class="btn-primary" style="background: #4F46E5;" onclick="doGet()">GET (Query)</button>
                    <button class="btn-danger" onclick="doDelete()">DELETE</button>
                </div>
            </div>

            <div class="card">
                <h3 style="margin-bottom: 16px; font-size: 14px; color: var(--text-main);">Live Operation Feed</h3>
                <div class="log-box" id="logBox">
                    <div class="log-entry"><span class="log-time">[Console]</span> HallownestKV Web Console Initialized. Ready for queries.</div>
                </div>
            </div>
        </div>
    </div>

    <script>
        async function updateStatus() {
            try {
                const res = await fetch('/status');
                const data = await res.json();
                document.getElementById('nodeRole').innerText = data.is_leader ? 'LEADER' : 'FOLLOWER';
                document.getElementById('nodeTerm').innerText = data.term;
                document.getElementById('nodeMem').innerText = data.memtable_bytes + ' B';
            } catch (e) {}
        }
        setInterval(updateStatus, 2000);
        updateStatus();

        function addLog(msg, isError) {
            const box = document.getElementById('logBox');
            const time = new Date().toLocaleTimeString();
            const entry = document.createElement('div');
            entry.className = 'log-entry';
            entry.innerHTML = '<span class="log-time">[' + time + ']</span> <span class="' + (isError ? 'log-error' : 'log-success') + '">' + msg + '</span>';
            box.prepend(entry);
        }

        async function doPut() {
            const k = document.getElementById('inputKey').value;
            const v = document.getElementById('inputValue').value;
            if(!k) return alert('Key is required');
            try {
                const res = await fetch('/put', { method: 'POST', body: JSON.stringify({key: k, value: v}) });
                const data = await res.json();
                addLog('PUT "' + k + '" -> OK (Index: ' + data.index + ', Term: ' + data.term + ')', false);
            } catch (e) { addLog('PUT Error: ' + e.message, true); }
        }

        async function doGet() {
            const k = document.getElementById('inputKey').value;
            if(!k) return alert('Key is required');
            try {
                const res = await fetch('/get?key=' + encodeURIComponent(k));
                const data = await res.json();
                if(data.found) {
                    if(data.tombstone) addLog('GET "' + k + '" -> KEY DELETED (Tombstone)', true);
                    else {
                        document.getElementById('inputValue').value = data.value;
                        addLog('GET "' + k + '" -> VALUE: "' + data.value + '"', false);
                    }
                } else addLog('GET "' + k + '" -> 404 Key Not Found', true);
            } catch (e) { addLog('GET Error: ' + e.message, true); }
        }

        async function doDelete() {
            const k = document.getElementById('inputKey').value;
            if(!k) return alert('Key is required');
            try {
                const res = await fetch('/delete?key=' + encodeURIComponent(k), { method: 'DELETE' });
                const data = await res.json();
                addLog('DELETE "' + k + '" -> TOMBSTONE INSERTED (Index: ' + data.index + ')', false);
            } catch (e) { addLog('DELETE Error: ' + e.message, true); }
        }
    </script>
</body>
</html>`

func main() {
	port := flag.Int("port", 50051, "gRPC server TCP port")
	httpPort := flag.Int("http-port", 8080, "HTTP REST Gateway TCP port")
	nodeID := flag.Int("node-id", 1, "Raft Node ID")
	dataDir := flag.String("data-dir", "./data/node1", "Directory path for WAL & SSTables")
	flag.Parse()

	fmt.Println("==================================================================")
	fmt.Println("               HALLOWNEST-KV STORAGE ENGINE DAEMON                ")
	fmt.Println("    Distributed Hybrid Go/C++ LSM-Tree & Raft Consensus System    ")
	fmt.Println("==================================================================")
	fmt.Printf("[Init] Node ID: %d | Data Dir: %s\n", *nodeID, *dataDir)

	// 1. Initialize local storage components (MemTable & WAL)
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed creating data directory: %v", err)
	}

	walOpts := bench.DefaultWALOptions(*dataDir)
	wal, err := bench.OpenWAL(walOpts)
	if err != nil {
		log.Fatalf("Failed opening Write-Ahead Log: %v", err)
	}
	defer wal.Close()

	memTable := storage.NewMemTable(storage.DefaultMemTableOptions())

	// 2. Perform WAL Crash Recovery Replay
	replayed, err := bench.ReplayWAL(*dataDir, func(key, value []byte, tombstone bool) {
		if tombstone {
			memTable.Delete(key)
		} else {
			memTable.Put(key, value)
		}
	})
	if err != nil {
		log.Printf("[Warning] WAL recovery replay error: %v", err)
	} else {
		fmt.Printf("[Recovery] Successfully replayed %d transactions from WAL into MemTable\n", replayed)
	}

	// 3. Initialize Raft Consensus Node
	applyCh := make(chan consensus.ApplyMsg, 1000)
	mockNet := consensus.NewMockNetwork()
	raftNode := consensus.NewRaftNode(*nodeID, []int{}, mockNet, applyCh)
	mockNet.RegisterNode(*nodeID, raftNode)
	defer raftNode.Kill()

	// Handle applied committed logs
	go func() {
		for msg := range applyCh {
			if msg.CommandValid {
				if msg.Tombstone {
					memTable.Delete(msg.Key)
				} else {
					memTable.Put(msg.Key, msg.Value)
				}
			}
		}
	}()

	// 4. Start Stagway Transport Network Server (gRPC Engine)
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", *port)
	stagwayOpts := network.DefaultStagwayServerOptions(grpcAddr, *nodeID)
	stagwayServer := network.NewStagwayServer(stagwayOpts, raftNode, memTable, wal)

	if err := stagwayServer.Start(); err != nil {
		log.Fatalf("Failed starting Stagway network server: %v", err)
	}
	defer stagwayServer.Stop()

	fmt.Printf("[Stagway Transport] gRPC Engine listening on tcp://%s\n", grpcAddr)

	// 5. Start HTTP REST Gateway & Web Dashboard Server
	httpAddr := fmt.Sprintf(":%d", *httpPort)
	httpMux := http.NewServeMux()

	httpMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(DashboardHTML))
	})

	httpMux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"success":false,"message":"Method not allowed, use POST"}`, http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, `{"success":false,"message":"Failed reading request body"}`, http.StatusBadRequest)
			return
		}

		var req PutJSONRequest
		if err := json.Unmarshal(body, &req); err != nil || req.Key == "" {
			http.Error(w, `{"success":false,"message":"Invalid JSON, 'key' is required"}`, http.StatusBadRequest)
			return
		}

		idx, term, isLeader, err := stagwayServer.PutGeo([]byte(req.Key), []byte(req.Value))
		if err != nil {
			resp, _ := json.Marshal(ResponseJSON{Success: false, Message: err.Error()})
			w.WriteHeader(http.StatusInternalServerError)
			w.Write(resp)
			return
		}

		resp, _ := json.Marshal(ResponseJSON{
			Success: true,
			Key:     req.Key,
			Value:   req.Value,
			Index:   idx,
			Term:    term,
			Message: fmt.Sprintf("Stored key '%s' via Raft Leader=%v", req.Key, isLeader),
		})
		w.Write(resp)
	})

	httpMux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, `{"success":false,"message":"Query parameter 'key' is required"}`, http.StatusBadRequest)
			return
		}

		val, tombstone, found, err := stagwayServer.GetGeo([]byte(key))
		if err != nil {
			resp, _ := json.Marshal(ResponseJSON{Success: false, Message: err.Error()})
			w.WriteHeader(http.StatusInternalServerError)
			w.Write(resp)
			return
		}

		if !found {
			resp, _ := json.Marshal(ResponseJSON{Success: false, Key: key, Found: false, Message: "404 Key Not Found"})
			w.WriteHeader(http.StatusNotFound)
			w.Write(resp)
			return
		}

		if tombstone {
			resp, _ := json.Marshal(ResponseJSON{Success: false, Key: key, Found: true, Tombstone: true, Message: "Key Deleted (Tombstone)"})
			w.Write(resp)
			return
		}

		resp, _ := json.Marshal(ResponseJSON{
			Success:   true,
			Key:       key,
			Value:     string(val),
			Found:     true,
			Tombstone: false,
		})
		w.Write(resp)
	})

	httpMux.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, `{"success":false,"message":"Query parameter 'key' is required"}`, http.StatusBadRequest)
			return
		}

		idx, term, isLeader, err := stagwayServer.FocusTombstone([]byte(key))
		if err != nil {
			resp, _ := json.Marshal(ResponseJSON{Success: false, Message: err.Error()})
			w.WriteHeader(http.StatusInternalServerError)
			w.Write(resp)
			return
		}

		resp, _ := json.Marshal(ResponseJSON{
			Success: true,
			Key:     key,
			Index:   idx,
			Term:    term,
			Message: fmt.Sprintf("Inserted tombstone for key '%s' via Raft Leader=%v", key, isLeader),
		})
		w.Write(resp)
	})

	httpMux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		term, isLeader := raftNode.GetState()
		resp, _ := json.Marshal(map[string]interface{}{
			"status":         "RUNNING",
			"node_id":        *nodeID,
			"term":           term,
			"is_leader":      isLeader,
			"grpc_port":      *port,
			"http_port":      *httpPort,
			"memtable_bytes": memTable.SizeBytes(),
		})
		w.Write(resp)
	})

	go func() {
		fmt.Printf("[HTTP Gateway & Web Console] REST API & Dashboard listening on http://localhost:%d\n", *httpPort)
		fmt.Printf("   ├── Web Console: http://localhost:%d/\n", *httpPort)
		fmt.Printf("   ├── GET         http://localhost:%d/status\n", *httpPort)
		fmt.Printf("   ├── GET         http://localhost:%d/get?key=...\n", *httpPort)
		fmt.Printf("   ├── POST        http://localhost:%d/put (JSON body: {\"key\":\"...\",\"value\":\"...\"})\n", *httpPort)
		fmt.Printf("   └── DELETE      http://localhost:%d/delete?key=...\n\n", *httpPort)

		if err := http.ListenAndServe(httpAddr, httpMux); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP Gateway error: %v", err)
		}
	}()

	// Wait for OS shutdown signal (CTRL+C or SIGTERM)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\n[Shutdown] HallownestKV daemon shut down.")
}
