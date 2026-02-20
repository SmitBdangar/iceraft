//! Integration tests for `iceraft-node` using in-memory storage + an
//! in-process loopback transport.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceraft_core::{LogIndex, NodeId, RaftConfig, RaftError};
use iceraft_network::proto::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use iceraft_network::transport::{NetworkTransport, RaftMessageHandler};
use iceraft_node::RaftNode;
use iceraft_storage::mem::MemStorage;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};

// ─── In-process loopback transport ───────────────────────────────────────────

/// Routes RPCs directly to the target node's handler – no network involved.
struct LoopbackTransport {
    /// Map from NodeId → handler (set after all nodes are created).
    handlers: RwLock<HashMap<NodeId, Arc<dyn RaftMessageHandler>>>,
}

impl LoopbackTransport {
    fn new() -> Self {
        Self {
            handlers: RwLock::new(HashMap::new()),
        }
    }

    async fn register(&self, id: NodeId, handler: Arc<dyn RaftMessageHandler>) {
        self.handlers.write().await.insert(id, handler);
    }
}

#[async_trait]
impl NetworkTransport for LoopbackTransport {
    async fn send_append_entries(
        &self,
        target: NodeId,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        let handlers = self.handlers.read().await;
        let handler = handlers
            .get(&target)
            .ok_or_else(|| RaftError::Network(format!("no handler for {target}")))?;
        handler.handle_append_entries(req).await
    }

    async fn send_request_vote(
        &self,
        target: NodeId,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RaftError> {
        let handlers = self.handlers.read().await;
        let handler = handlers
            .get(&target)
            .ok_or_else(|| RaftError::Network(format!("no handler for {target}")))?;
        handler.handle_request_vote(req).await
    }

    async fn send_install_snapshot(
        &self,
        target: NodeId,
        req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        let handlers = self.handlers.read().await;
        let handler = handlers
            .get(&target)
            .ok_or_else(|| RaftError::Network(format!("no handler for {target}")))?;
        handler.handle_install_snapshot(req).await
    }
}

// ─── Cluster builder ─────────────────────────────────────────────────────────

type TestNode = RaftNode<MemStorage, LoopbackTransport>;

/// Spin up `n` nodes that all talk to each other via a shared loopback transport.
async fn make_cluster(n: u64) -> Vec<Arc<TestNode>> {
    let all_ids: Vec<NodeId> = (1..=n).collect();
    let transport = Arc::new(LoopbackTransport::new());

    let mut nodes: Vec<Arc<TestNode>> = Vec::new();

    for id in &all_ids {
        let peers: Vec<NodeId> = all_ids.iter().filter(|&&p| p != *id).copied().collect();
        let config = RaftConfig {
            id: *id,
            peers,
            heartbeat_interval_ms: 50,
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            ..Default::default()
        };

        let storage = Arc::new(MemStorage::new());
        let (apply_tx, _apply_rx) = tokio::sync::mpsc::unbounded_channel();

        let node = Arc::new(RaftNode::start(
            config,
            storage,
            transport.clone(),
            apply_tx,
        ));
        nodes.push(node);
    }

    // Wire up the loopback routes after all nodes exist.
    for (i, node) in nodes.iter().enumerate() {
        let id = (i + 1) as NodeId;
        transport
            .register(id, node.clone() as Arc<dyn RaftMessageHandler>)
            .await;
    }

    nodes
}

/// Wait until at least one node reports a leader, up to `timeout_ms`.
async fn wait_for_leader(nodes: &[Arc<TestNode>], timeout_ms: u64) -> Option<NodeId> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    while tokio::time::Instant::now() < deadline {
        for node in nodes {
            if let Some(leader) = node.leader().await {
                return Some(leader);
            }
        }
        sleep(Duration::from_millis(20)).await;
    }
    None
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_single_node_becomes_leader() {
    let config = RaftConfig {
        id: 1,
        peers: vec![],
        heartbeat_interval_ms: 50,
        election_timeout_min_ms: 100,
        election_timeout_max_ms: 200,
        ..Default::default()
    };
    let storage = Arc::new(MemStorage::new());
    let transport = Arc::new(LoopbackTransport::new());
    let (apply_tx, _) = tokio::sync::mpsc::unbounded_channel();

    let node = Arc::new(RaftNode::start(config, storage, transport, apply_tx));

    // A single node with no peers should elect itself immediately.
    sleep(Duration::from_millis(500)).await;
    let leader = node.leader().await;
    assert_eq!(leader, Some(1), "single node should be its own leader");
}

#[tokio::test]
async fn test_three_node_leader_election() {
    let nodes = make_cluster(3).await;
    let leader = wait_for_leader(&nodes, 2_000).await;
    assert!(leader.is_some(), "a leader should be elected within 2s");
    println!("elected leader: {:?}", leader);
}

#[tokio::test]
async fn test_log_replication() {
    let nodes = make_cluster(3).await;
    let leader_id = wait_for_leader(&nodes, 2_000).await.expect("no leader");

    let leader_node = nodes
        .iter()
        .find(|n| {
            // We can't easily check id synchronously; use leader() to confirm.
            true // We'll just try all nodes
        })
        .unwrap();

    // Find the actual leader node and propose a value.
    let mut committed_idx: Option<LogIndex> = None;
    for node in &nodes {
        match node.propose(b"key=value".to_vec()).await {
            Ok(idx) => {
                committed_idx = Some(idx);
                break;
            }
            Err(RaftError::NotLeader { .. }) => continue,
            Err(e) => panic!("unexpected error: {e}"),
        }
    }

    assert!(
        committed_idx.is_some(),
        "proposal should have been committed"
    );
    println!("committed at index {:?}", committed_idx);
}

#[tokio::test]
async fn test_multiple_proposals_in_order() {
    let nodes = make_cluster(3).await;
    wait_for_leader(&nodes, 2_000).await.expect("no leader");

    let mut last_idx = 0u64;
    let mut success_count = 0;

    for i in 0..5u64 {
        for node in &nodes {
            match node.propose(format!("entry-{i}").into_bytes()).await {
                Ok(idx) => {
                    assert!(
                        idx > last_idx,
                        "commit index must be monotonically increasing"
                    );
                    last_idx = idx;
                    success_count += 1;
                    break;
                }
                Err(RaftError::NotLeader { .. }) => continue,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
    }

    assert_eq!(success_count, 5, "all 5 proposals should be committed");
}
