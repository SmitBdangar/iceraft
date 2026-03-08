//! # iceraft-core
//!
//! Shared primitive types, configuration, and error definitions used across
//! all IceRaft crates.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

// ─── Primitive Aliases ───────────────────────────────────────────────────────

/// Unique identifier for a cluster member.
pub type NodeId = u64;

/// Raft term number (monotonically increasing).
pub type Term = u64;

/// Absolute log index (1-based; 0 means "no entry").
pub type LogIndex = u64;

// ─── Configuration ───────────────────────────────────────────────────────────

/// Runtime configuration for a Raft node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// Unique ID of this node within the cluster.
    pub id: NodeId,

    /// All peer node IDs (NOT including self).
    pub peers: Vec<NodeId>,

    /// Milliseconds between leader heartbeats.
    pub heartbeat_interval_ms: u64,

    /// Minimum election timeout in milliseconds.
    pub election_timeout_min_ms: u64,

    /// Maximum election timeout in milliseconds (randomly chosen in range).
    pub election_timeout_max_ms: u64,

    /// Maximum number of log entries per AppendEntries RPC.
    pub max_append_entries: usize,

    /// How many committed entries to accumulate before triggering a snapshot.
    pub snapshot_threshold: u64,

    /// Maximum log entries to keep after a snapshot (for slow-follower catch-up).
    pub snapshot_chunk_size: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            id: 1,
            peers: vec![],
            heartbeat_interval_ms: 100,
            election_timeout_min_ms: 300,
            election_timeout_max_ms: 600,
            max_append_entries: 128,
            snapshot_threshold: 10_000,
            snapshot_chunk_size: 1024 * 1024, // 1 MiB
        }
    }
}

impl RaftConfig {
    /// Validate the configuration, returning an error if it is invalid.
    pub fn validate(&self) -> Result<(), RaftError> {
        if self.election_timeout_min_ms >= self.election_timeout_max_ms {
            return Err(RaftError::InvalidConfig(
                "election_timeout_min_ms must be less than election_timeout_max_ms".into(),
            ));
        }
        if self.heartbeat_interval_ms >= self.election_timeout_min_ms {
            return Err(RaftError::InvalidConfig(
                "heartbeat_interval_ms must be less than election_timeout_min_ms".into(),
            ));
        }
        Ok(())
    }

    /// All member IDs (self + peers).
    pub fn members(&self) -> HashSet<NodeId> {
        let mut set: HashSet<NodeId> = self.peers.iter().copied().collect();
        set.insert(self.id);
        set
    }

    /// Quorum size for the current configuration.
    pub fn quorum(&self) -> usize {
        ((self.peers.len() + 1) / 2) + 1
    }
}

// ─── Log Entry ───────────────────────────────────────────────────────────────

/// Type of a log entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Normal application command.
    Normal,
    /// Cluster membership change.
    ConfChange,
}

/// A single entry in the Raft log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// The term in which this entry was created.
    pub term: Term,
    /// The absolute index of this entry.
    pub index: LogIndex,
    /// The payload – opaque bytes interpreted by the application state machine.
    pub data: Vec<u8>,
    /// Entry type.
    pub entry_type: EntryType,
}

// ─── Persistent State ────────────────────────────────────────────────────────

/// Raft hard state that must be persisted to stable storage before responding
/// to any RPC.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct HardState {
    /// Latest term the node has seen (initialised to 0).
    pub term: Term,
    /// Node that this node voted for in the current term, or 0 if none.
    pub vote: NodeId,
    /// Index of the highest log entry known to be committed.
    pub commit: LogIndex,
}

// ─── Snapshot ────────────────────────────────────────────────────────────────

/// Metadata attached to a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SnapshotMeta {
    /// The log index of the last entry included in the snapshot.
    pub last_included_index: LogIndex,
    /// The term of the last entry included in the snapshot.
    pub last_included_term: Term,
    /// Cluster membership at the time of the snapshot.
    pub conf_nodes: Vec<NodeId>,
}

/// A complete snapshot of the application state machine.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Snapshot {
    /// Opaque application data.
    pub data: Vec<u8>,
    /// Snapshot metadata.
    pub metadata: SnapshotMeta,
}

// ─── Proposal ────────────────────────────────────────────────────────────────

/// A client proposal to append data to the Raft log.
#[derive(Debug)]
pub struct Proposal {
    /// Arbitrary byte payload to apply to the state machine.
    pub data: Vec<u8>,
    /// Optional one-shot channel to notify the caller when the proposal is
    /// committed (returns the committed log index).
    pub tx: Option<tokio::sync::oneshot::Sender<Result<LogIndex, RaftError>>>,
}

// ─── Errors ──────────────────────────────────────────────────────────────────

/// Top-level error type for IceRaft.
#[derive(Debug, Error, Clone)]
pub enum RaftError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("not the leader; current leader is node {leader_id:?}")]
    NotLeader { leader_id: Option<NodeId> },

    #[error("storage error: {0}")]
    Storage(String),

    #[error("network error: {0}")]
    Network(String),

    #[error("node is shutting down")]
    Shutdown,

    #[error("log compacted: requested index {requested} has been compacted (first available: {first_available})")]
    LogCompacted {
        requested: LogIndex,
        first_available: LogIndex,
    },

    #[error("log unavailable: index {0} is beyond the last log index")]
    LogUnavailable(LogIndex),

    #[error("snapshot error: {0}")]
    Snapshot(String),

    #[error("channel closed")]
    ChannelClosed,

    #[error("timed out")]
    Timeout,

    #[error("unknown error: {0}")]
    Other(String),
}
