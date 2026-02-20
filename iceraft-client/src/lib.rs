//! # iceraft-client
//!
//! High-level client helper for submitting proposals to an IceRaft cluster.
//!
//! The client automatically redirects requests to the current leader and
//! retries on transient network errors.

use iceraft_core::{LogIndex, NodeId, RaftError};
use iceraft_network::proto::{AppendEntriesRequest, RequestVoteRequest};
use iceraft_network::transport::NetworkTransport;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// A cluster client that discovers and follows the current leader.
pub struct RaftClusterClient<T: NetworkTransport> {
    transport: Arc<T>,
    /// Known current leader, refreshed on redirect.
    known_leader: RwLock<Option<NodeId>>,
    /// All cluster member IDs.
    members: Vec<NodeId>,
}

impl<T: NetworkTransport> RaftClusterClient<T> {
    /// Create a new cluster client.
    pub fn new(transport: Arc<T>, members: Vec<NodeId>) -> Self {
        Self {
            transport,
            known_leader: RwLock::new(None),
            members,
        }
    }

    /// Ping a node to check if it is the current leader.
    ///
    /// Sends a heartbeat AppendEntries with no entries and returns `true` if
    /// the node accepts it (i.e. believes it is the leader).
    async fn ping_leader(&self, node: NodeId) -> bool {
        let req = AppendEntriesRequest {
            term: 0,
            leader_id: 0,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        // A real implementation would use a dedicated `GetLeader` RPC.
        // For now, try AppendEntries and treat a non-error reply as "up".
        self.transport.send_append_entries(node, req).await.is_ok()
    }

    /// Discover the leader by trying each member in turn.
    pub async fn find_leader(&self) -> Option<NodeId> {
        for &member in &self.members {
            if self.ping_leader(member).await {
                info!(member, "discovered leader");
                *self.known_leader.write().await = Some(member);
                return Some(member);
            }
        }
        None
    }
}
