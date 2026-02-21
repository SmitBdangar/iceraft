//! Raft persistent state and role-specific state.

use std::collections::HashMap;

use iceraft_core::{LogIndex, NodeId, Term};

// ─── Role ────────────────────────────────────────────────────────────────────

/// The three roles a Raft node can occupy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

// ─── Progress ────────────────────────────────────────────────────────────────

/// Replication progress tracked per follower by the leader.
#[derive(Debug, Clone)]
pub struct Progress {
    /// Next log index to send to this peer.
    pub next_index: LogIndex,
    /// Highest log index known to be replicated on this peer.
    pub match_index: LogIndex,
    /// Whether we are currently expecting a reply (to avoid redundant RPCs).
    pub in_flight: bool,
}

impl Progress {
    pub fn new(next_index: LogIndex) -> Self {
        Self {
            next_index,
            match_index: 0,
            in_flight: false,
        }
    }
}

// ─── RaftStateMachine ────────────────────────────────────────────────────────

/// All mutable Raft state held in memory (volatile + durable intent).
pub struct RaftStateMachine {
    // ── Persistent (must be saved before replying to RPC) ───────────────────
    pub current_term: Term,
    pub voted_for: Option<NodeId>,

    // ── Volatile ────────────────────────────────────────────────────────────
    pub role: RaftRole,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub leader_id: Option<NodeId>,

    // ── Leader-only ─────────────────────────────────────────────────────────
    pub progress: HashMap<NodeId, Progress>,

    // ── Election ────────────────────────────────────────────────────────────
    pub votes_received: std::collections::HashSet<NodeId>,
}

impl Default for RaftStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftStateMachine {
    pub fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            role: RaftRole::Follower,
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
            progress: HashMap::new(),
            votes_received: std::collections::HashSet::new(),
        }
    }

    /// Transition to follower for the given term.
    pub fn become_follower(&mut self, term: Term, leader_id: Option<NodeId>) {
        self.role = RaftRole::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.leader_id = leader_id;
        self.votes_received.clear();
        self.progress.clear();
    }

    /// Transition to candidate and start an election.
    pub fn become_candidate(&mut self, self_id: NodeId) {
        self.current_term += 1;
        self.role = RaftRole::Candidate;
        self.voted_for = Some(self_id);
        self.leader_id = None;
        self.votes_received.clear();
        self.votes_received.insert(self_id);
    }

    /// Transition to leader.
    pub fn become_leader(&mut self, self_id: NodeId, last_log_index: LogIndex, peers: &[NodeId]) {
        self.role = RaftRole::Leader;
        self.leader_id = Some(self_id);
        self.progress.clear();
        for &peer in peers {
            self.progress
                .insert(peer, Progress::new(last_log_index + 1));
        }
    }

    /// Calculate the new commit index from current match_index quorum,
    /// returning the new commit index if it advanced.
    pub fn maybe_commit(
        &mut self,
        quorum: usize,
        last_log_index: LogIndex,
        current_term: Term,
        term_of: impl Fn(LogIndex) -> Option<Term>,
    ) -> Option<LogIndex> {
        // Try to advance commit index from last_log_index downward.
        let mut n = last_log_index;
        while n > self.commit_index {
            // Only commit entries from the current term (Raft safety rule).
            if term_of(n) != Some(current_term) {
                n -= 1;
                continue;
            }

            let replicated = self
                .progress
                .values()
                .filter(|p| p.match_index >= n)
                .count()
                + 1; // +1 for self

            if replicated >= quorum {
                self.commit_index = n;
                return Some(n);
            }
            n -= 1;
        }
        None
    }
}
