//! # iceraft
//!
//! **Production-grade Raft consensus library for async Rust.**
//!
//! ## Features
//!
//! - Fully async, built on [Tokio](https://tokio.rs)
//! - gRPC transport via [Tonic](https://github.com/hyperium/tonic)
//! - Pluggable storage backends ([`MemStorage`] + optional RocksDB)
//! - Leader election with randomised timeouts and pre-vote
//! - Log replication with fast-rollback conflict resolution
//! - Snapshot support (InstallSnapshot RPC)
//! - Prometheus metrics
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use iceraft::{RaftConfig, RaftNode, MemStorage};
//!
//! # async fn run() {
//! // 1. Configure a single-node cluster (for demo; add peers for real clusters).
//! let config = RaftConfig {
//!     id: 1,
//!     peers: vec![],          // no peers → instantly becomes leader
//!     ..Default::default()
//! };
//!
//! // 2. Choose a storage backend.
//! let storage = Arc::new(MemStorage::new());
//!
//! // 3. Build a no-op transport (for single-node demos).
//! // In production: use RaftGrpcClient with real peer addresses.
//! let transport = Arc::new(iceraft::NoopTransport);
//!
//! // 4. Application state machine receive channel.
//! let (apply_tx, mut apply_rx) = tokio::sync::mpsc::unbounded_channel();
//!
//! // 5. Start the node.
//! let node = RaftNode::start(config, storage, transport, apply_tx);
//!
//! // 6. Wait briefly for election to complete, then propose data.
//! tokio::time::sleep(std::time::Duration::from_millis(500)).await;
//! let idx = node.propose(b"hello world".to_vec()).await.unwrap();
//! println!("committed at index {idx}");
//! # }
//! ```

pub use iceraft_core::{
    EntryType, HardState, LogEntry, LogIndex, NodeId, Proposal, RaftConfig, RaftError, Snapshot,
    SnapshotMeta, Term,
};
pub use iceraft_metrics::RaftMetrics;
pub use iceraft_network::{
    client::{PeerInfo, RaftGrpcClient},
    server::RaftGrpcServer,
    transport::{NetworkTransport, RaftMessageHandler},
};
pub use iceraft_node::{RaftMessage, RaftNode, RaftRole};
pub use iceraft_storage::{mem::MemStorage, Storage};

// ─── NoopTransport ───────────────────────────────────────────────────────────

use async_trait::async_trait;
use iceraft_network::proto::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};

/// A no-op transport that drops all outbound RPCs.
///
/// Useful for single-node clusters and unit testing.
pub struct NoopTransport;

#[async_trait]
impl NetworkTransport for NoopTransport {
    async fn send_append_entries(
        &self,
        _target: NodeId,
        _req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        Err(RaftError::Network("noop transport".into()))
    }

    async fn send_request_vote(
        &self,
        _target: NodeId,
        _req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RaftError> {
        Err(RaftError::Network("noop transport".into()))
    }

    async fn send_install_snapshot(
        &self,
        _target: NodeId,
        _req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        Err(RaftError::Network("noop transport".into()))
    }
}
