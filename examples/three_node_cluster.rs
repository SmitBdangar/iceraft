//! Three-node in-process IceRaft cluster demo.
//!
//! Run with:
//! ```bash
//! cargo run --example three_node_cluster
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceraft::{
    LogEntry, MemStorage, NetworkTransport, RaftConfig, RaftError, RaftMessage, RaftMessageHandler,
    RaftNode,
};
use iceraft_network::proto::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};

// ── Loopback transport ───────────────────────────────────────────────────────

struct Loopback {
    peers: RwLock<HashMap<u64, Arc<dyn RaftMessageHandler>>>,
}

impl Loopback {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            peers: RwLock::new(HashMap::new()),
        })
    }
    async fn add(&self, id: u64, h: Arc<dyn RaftMessageHandler>) {
        self.peers.write().await.insert(id, h);
    }
}

#[async_trait]
impl NetworkTransport for Loopback {
    async fn send_append_entries(&self, target: u64, req: AppendEntriesRequest) -> Result<AppendEntriesResponse, RaftError> {
        let g = self.peers.read().await;
        g.get(&target).ok_or_else(|| RaftError::Network("unknown peer".into()))?.handle_append_entries(req).await
    }
    async fn send_request_vote(&self, target: u64, req: RequestVoteRequest) -> Result<RequestVoteResponse, RaftError> {
        let g = self.peers.read().await;
        g.get(&target).ok_or_else(|| RaftError::Network("unknown peer".into()))?.handle_request_vote(req).await
    }
    async fn send_install_snapshot(&self, target: u64, req: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, RaftError> {
        let g = self.peers.read().await;
        g.get(&target).ok_or_else(|| RaftError::Network("unknown peer".into()))?.handle_install_snapshot(req).await
    }
}

// ── main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("iceraft=debug,info")
        .init();

    let transport = Loopback::new();
    let mut nodes: Vec<Arc<RaftNode<MemStorage, Loopback>>> = Vec::new();

    // Boot 3 nodes.
    for id in 1u64..=3 {
        let peers: Vec<u64> = vec![1, 2, 3].into_iter().filter(|&p| p != id).collect();
        let config = RaftConfig {
            id,
            peers,
            heartbeat_interval_ms: 50,
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            ..Default::default()
        };
        let storage = Arc::new(MemStorage::new());
        let (apply_tx, mut apply_rx) = tokio::sync::mpsc::unbounded_channel::<LogEntry>();

        // Print applied entries.
        tokio::spawn(async move {
            while let Some(entry) = apply_rx.recv().await {
                println!(
                    "  [node {id}] applied index={} data={}",
                    entry.index,
                    String::from_utf8_lossy(&entry.data)
                );
            }
        });

        let node = Arc::new(RaftNode::start(config, storage, transport.clone(), apply_tx));
        nodes.push(node);
    }

    // Register handlers.
    for (i, node) in nodes.iter().enumerate() {
        transport.add((i + 1) as u64, node.clone()).await;
    }

    println!("⏳ Waiting for leader election…");
    sleep(Duration::from_millis(800)).await;

    // Submit 5 proposals.
    for i in 1u64..=5 {
        let data = format!("write:key{i}=value{i}");
        let mut committed = false;
        for node in &nodes {
            match node.propose(data.as_bytes().to_vec()).await {
                Ok(idx) => {
                    println!("✅ Proposal {i} committed at log index {idx}");
                    committed = true;
                    break;
                }
                Err(RaftError::NotLeader { .. }) => continue,
                Err(e) => eprintln!("error: {e}"),
            }
        }
        if !committed {
            eprintln!("❌ Proposal {i} was not committed!");
        }
    }

    // Show current leader.
    for node in &nodes {
        if let Some(leader) = node.leader().await {
            println!("👑 Current leader: node {leader}");
            break;
        }
    }

    // Shut down.
    for node in &nodes {
        node.shutdown();
    }
    println!("🛑 All nodes shut down.");
}
