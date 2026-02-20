//! Internal messages flowing through the Raft node's event loop.

use iceraft_core::{LogIndex, NodeId, Proposal, RaftError, Term};
use iceraft_network::proto::{AppendEntriesResponse, InstallSnapshotResponse, RequestVoteResponse};
use tokio::sync::oneshot;

/// Messages handled by the [`RaftNode`]'s internal event loop.
pub enum RaftMessage {
    /// A client-submitted command to be appended to the log.
    Propose(Proposal),

    /// Linearisable read: caller blocks until the read index is confirmed safe.
    ReadIndex {
        tx: oneshot::Sender<Result<LogIndex, RaftError>>,
    },

    /// Transfer leadership to a specific node.
    TransferLeader { target: NodeId },

    /// Ask for the current leader.
    GetLeader { tx: oneshot::Sender<Option<NodeId>> },

    /// Ask for the current term.
    GetTerm { tx: oneshot::Sender<Term> },

    /// Trigger a snapshot at the given log index.
    TriggerSnapshot { index: LogIndex },

    /// Shut down the node gracefully.
    Shutdown,

    // ── Internal async RPC responses ──
    AppendResponse {
        peer: NodeId,
        req_last_index: LogIndex,
        resp: AppendEntriesResponse,
    },
    VoteResponse {
        peer: NodeId,
        resp: RequestVoteResponse,
    },
    SnapshotResponse {
        peer: NodeId,
        req_last_index: LogIndex,
        resp: InstallSnapshotResponse,
    },
}
