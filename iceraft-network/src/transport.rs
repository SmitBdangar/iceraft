//! [`NetworkTransport`] trait + adapters.

use async_trait::async_trait;
use iceraft_core::{NodeId, RaftError};

use crate::proto::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};

// ─── Message Handler (used by the server) ────────────────────────────────────

/// A handler that the gRPC server calls when it receives RPCs.
///
/// Implemented by `RaftNode` in `iceraft-node`.
#[async_trait]
pub trait RaftMessageHandler: Send + Sync + 'static {
    async fn handle_append_entries(
        &self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError>;

    async fn handle_request_vote(
        &self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RaftError>;

    async fn handle_install_snapshot(
        &self,
        req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError>;
}

// ─── Outgoing Transport (used by the node) ───────────────────────────────────

/// The outbound transport used by `iceraft-node` to send RPCs to peers.
#[async_trait]
pub trait NetworkTransport: Send + Sync + 'static {
    async fn send_append_entries(
        &self,
        target: NodeId,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError>;

    async fn send_request_vote(
        &self,
        target: NodeId,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RaftError>;

    async fn send_install_snapshot(
        &self,
        target: NodeId,
        req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError>;
}
