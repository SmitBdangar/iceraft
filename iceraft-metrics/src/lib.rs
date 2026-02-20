//! # iceraft-metrics
//!
//! Prometheus metrics for the IceRaft library.

use once_cell::sync::Lazy;
use prometheus::{register_int_counter, register_int_gauge, IntCounter, IntGauge};

/// All Prometheus metrics for a single Raft node.
#[derive(Clone)]
pub struct RaftMetrics {
    pub elections_started: IntCounter,
    pub votes_granted: IntCounter,
    pub leader_changes: IntCounter,
    pub append_entries_received: IntCounter,
    pub entries_committed: IntCounter,
    pub snapshots_installed: IntCounter,
    pub current_term: IntGauge,
    pub commit_index: IntGauge,
}

static GLOBAL_METRICS: Lazy<RaftMetrics> = Lazy::new(|| RaftMetrics {
    elections_started: register_int_counter!(
        "raft_elections_started_total",
        "Total number of elections started by this node"
    )
    .expect("register elections_started"),

    votes_granted: register_int_counter!(
        "raft_votes_granted_total",
        "Total number of votes granted to peers"
    )
    .expect("register votes_granted"),

    leader_changes: register_int_counter!(
        "raft_leader_changes_total",
        "Total number of times this node became leader"
    )
    .expect("register leader_changes"),

    append_entries_received: register_int_counter!(
        "raft_append_entries_received_total",
        "Total AppendEntries RPCs received"
    )
    .expect("register append_entries_received"),

    entries_committed: register_int_counter!(
        "raft_entries_committed_total",
        "Total log entries committed"
    )
    .expect("register entries_committed"),

    snapshots_installed: register_int_counter!(
        "raft_snapshots_installed_total",
        "Total snapshots installed from leader"
    )
    .expect("register snapshots_installed"),

    current_term: register_int_gauge!("raft_current_term", "Current Raft term")
        .expect("register current_term"),

    commit_index: register_int_gauge!("raft_commit_index", "Current commit index")
        .expect("register commit_index"),
});

impl RaftMetrics {
    /// Get the global metrics registry instances (registers on first call).
    pub fn new() -> Self {
        GLOBAL_METRICS.clone()
    }
}

impl Default for RaftMetrics {
    fn default() -> Self {
        Self::new()
    }
}
