//! Pooled gRPC client stubs, one connection per peer.

use std::collections::HashMap;

use iceraft_core::{NodeId, RaftError};
use tonic::transport::Channel;
use tracing::{debug, warn};

use crate::proto::{
    raft_service_client::RaftServiceClient, AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::transport::NetworkTransport;
use async_trait::async_trait;

// ─── Peer info ───────────────────────────────────────────────────────────────

/// A single peer's connection information.
#[derive(Clone, Debug)]
pub struct PeerInfo {
    pub id: NodeId,
    /// gRPC endpoint, e.g. `http://127.0.0.1:50051`
    pub addr: String,
}

// ─── Client pool ─────────────────────────────────────────────────────────────

/// Lazily-connected, per-peer gRPC client stubs.
///
/// Uses tonic's built-in connection pooling / keep-alive.  Each [`Channel`] is
/// created once (lazily) and reused across calls.
#[derive(Clone)]
pub struct RaftGrpcClient {
    // Pre-built channels keyed by NodeId.
    channels: HashMap<NodeId, Channel>,
}

impl RaftGrpcClient {
    /// Build a client pool from a list of peers.  Channels are created eagerly
    /// at construction time to surface config errors early.
    pub async fn new(peers: Vec<PeerInfo>) -> Result<Self, RaftError> {
        let mut channels = HashMap::with_capacity(peers.len());
        for peer in peers {
            let ch = Channel::from_shared(peer.addr.clone())
                .map_err(|e| RaftError::Network(e.to_string()))?
                .connect_lazy();
            channels.insert(peer.id, ch);
        }
        Ok(Self { channels })
    }

    fn client(&self, target: NodeId) -> Result<RaftServiceClient<Channel>, RaftError> {
        let ch = self.channels.get(&target).ok_or_else(|| {
            RaftError::Network(format!("no channel registered for node {target}"))
        })?;
        Ok(RaftServiceClient::new(ch.clone()))
    }
}

// ─── NetworkTransport impl ───────────────────────────────────────────────────

#[async_trait]
impl NetworkTransport for RaftGrpcClient {
    async fn send_append_entries(
        &self,
        target: NodeId,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        debug!(target, "→ AppendEntries (entries={})", req.entries.len());
        let mut c = self.client(target)?;
        c.append_entries(req)
            .await
            .map(|r| r.into_inner())
            .map_err(|e| {
                warn!(%e, target, "AppendEntries RPC failed");
                RaftError::Network(e.to_string())
            })
    }

    async fn send_request_vote(
        &self,
        target: NodeId,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RaftError> {
        debug!(target, "→ RequestVote");
        let mut c = self.client(target)?;
        c.request_vote(req)
            .await
            .map(|r| r.into_inner())
            .map_err(|e| {
                warn!(%e, target, "RequestVote RPC failed");
                RaftError::Network(e.to_string())
            })
    }

    async fn send_install_snapshot(
        &self,
        target: NodeId,
        req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        debug!(target, "→ InstallSnapshot");
        let mut c = self.client(target)?;
        c.install_snapshot(req)
            .await
            .map(|r| r.into_inner())
            .map_err(|e| {
                warn!(%e, target, "InstallSnapshot RPC failed");
                RaftError::Network(e.to_string())
            })
    }
}
