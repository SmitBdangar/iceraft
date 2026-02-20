//! gRPC server – receives incoming RPCs from peers and dispatches them to a
//! [`RaftMessageHandler`].

use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::instrument;

use crate::proto::{
    raft_service_server::{RaftService, RaftServiceServer},
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use crate::transport::RaftMessageHandler;

// ─── Service impl ────────────────────────────────────────────────────────────

struct RaftServiceImpl<H: RaftMessageHandler> {
    handler: Arc<H>,
}

#[tonic::async_trait]
impl<H: RaftMessageHandler> RaftService for RaftServiceImpl<H> {
    #[instrument(skip(self, request), fields(leader = request.get_ref().leader_id))]
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        self.handler
            .handle_append_entries(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|e| Status::internal(e.to_string()))
    }

    #[instrument(skip(self, request), fields(candidate = request.get_ref().candidate_id))]
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        self.handler
            .handle_request_vote(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|e| Status::internal(e.to_string()))
    }

    #[instrument(skip(self, request), fields(leader = request.get_ref().leader_id))]
    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        self.handler
            .handle_install_snapshot(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|e| Status::internal(e.to_string()))
    }
}

// ─── Public builder ──────────────────────────────────────────────────────────

/// Wraps a [`RaftMessageHandler`] in a tonic gRPC server.
pub struct RaftGrpcServer;

impl RaftGrpcServer {
    /// Build a [`RaftServiceServer`] backed by the supplied handler.
    pub fn new<H: RaftMessageHandler>(handler: Arc<H>) -> RaftServiceServer<impl RaftService> {
        RaftServiceServer::new(RaftServiceImpl { handler })
    }
}
