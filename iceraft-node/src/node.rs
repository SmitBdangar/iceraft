//! `RaftNode` – the main async event loop driving the Raft state machine.

use std::sync::Arc;
use std::time::Duration;

use iceraft_core::{
    EntryType, HardState, LogEntry, LogIndex, NodeId, Proposal, RaftConfig, RaftError, Snapshot,
};
use iceraft_metrics::RaftMetrics;
use iceraft_network::{
    proto::{
        AppendEntriesRequest, AppendEntriesResponse, Entry as ProtoEntry,
        EntryType as ProtoEntryType, InstallSnapshotRequest, InstallSnapshotResponse,
        RequestVoteRequest, RequestVoteResponse, SnapshotMetadata,
    },
    transport::{NetworkTransport, RaftMessageHandler},
};
use iceraft_storage::Storage;
use rand::Rng;
use tokio::{
    sync::{mpsc, Mutex},
    time::{sleep, Instant},
};
use tracing::{debug, error, info, instrument, warn};

use crate::message::RaftMessage;
use crate::state::{RaftRole, RaftStateMachine};

// ─── Type aliases ────────────────────────────────────────────────────────────

type ProposalSender = mpsc::UnboundedSender<RaftMessage>;
type ProposalReceiver = mpsc::UnboundedReceiver<RaftMessage>;

// ─── RaftNode ────────────────────────────────────────────────────────────────

/// A running Raft node.
///
/// Spawn it with [`RaftNode::start`] and submit commands via [`RaftNode::propose`].
pub struct RaftNode<S: Storage, T: NetworkTransport> {
    config: Arc<RaftConfig>,
    storage: Arc<S>,
    transport: Arc<T>,
    metrics: Arc<RaftMetrics>,
    /// Channel for external callers to submit messages to the event loop.
    tx: ProposalSender,
    /// Shared state guard (used by the RPC handler impl below).
    inner: Arc<Mutex<RaftNodeInner<S, T>>>,
}

struct RaftNodeInner<S: Storage, T: NetworkTransport> {
    sm: RaftStateMachine,
    config: Arc<RaftConfig>,
    storage: Arc<S>,
    transport: Arc<T>,
    metrics: Arc<RaftMetrics>,
    tx: ProposalSender,
    /// Pending proposals waiting for their log entry to commit.
    pending: Vec<(
        LogIndex,
        tokio::sync::oneshot::Sender<Result<LogIndex, RaftError>>,
    )>,
    /// Snapshot data being assembled from InstallSnapshot chunks.
    snapshot_buf: Option<(SnapshotMeta, Vec<u8>)>,
}

/// Protobuf SnapshotMetadata alias (local shorthand).
type SnapshotMeta = iceraft_network::proto::SnapshotMetadata;

impl<S: Storage, T: NetworkTransport> RaftNode<S, T> {
    // ── Construction ─────────────────────────────────────────────────────────

    /// Create and immediately spawn the Raft event loop.
    ///
    /// `apply_tx` is called every time an entry is committed so the
    /// application state machine can apply it.
    pub fn start(
        config: RaftConfig,
        storage: Arc<S>,
        transport: Arc<T>,
        apply_tx: mpsc::UnboundedSender<LogEntry>,
    ) -> Self {
        let config = Arc::new(config);
        let metrics = Arc::new(RaftMetrics::new());

        let (tx, rx) = mpsc::unbounded_channel();

        let inner = Arc::new(Mutex::new(RaftNodeInner {
            sm: RaftStateMachine::new(),
            config: config.clone(),
            storage: storage.clone(),
            transport: transport.clone(),
            metrics: metrics.clone(),
            tx: tx.clone(),
            pending: Vec::new(),
            snapshot_buf: None,
        }));

        // Spawn the event loop.
        let inner_clone = inner.clone();
        let tx_clone = tx.clone();
        let cfg_clone = config.clone();
        tokio::spawn(async move {
            run_event_loop(inner_clone, rx, apply_tx, cfg_clone, tx_clone).await;
        });

        Self {
            config,
            storage,
            transport,
            metrics,
            tx,
            inner,
        }
    }

    // ── Public API ───────────────────────────────────────────────────────────

    /// Submit a command to the cluster.  Returns when the entry is committed.
    ///
    /// Fails with [`RaftError::NotLeader`] if this node is not the leader.
    pub async fn propose(&self, data: Vec<u8>) -> Result<LogIndex, RaftError> {
        let (otx, orx) = tokio::sync::oneshot::channel();
        self.tx
            .send(RaftMessage::Propose(Proposal {
                data,
                tx: Some(otx),
            }))
            .map_err(|_| RaftError::ChannelClosed)?;
        orx.await.map_err(|_| RaftError::ChannelClosed)?
    }

    /// Return the current leader (if known).
    pub async fn leader(&self) -> Option<NodeId> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.tx.send(RaftMessage::GetLeader { tx });
        rx.await.ok().flatten()
    }

    /// Return the current term.
    pub async fn term(&self) -> u64 {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.tx.send(RaftMessage::GetTerm { tx });
        rx.await.unwrap_or(0)
    }

    /// Gracefully shut down the node.
    pub fn shutdown(&self) {
        let _ = self.tx.send(RaftMessage::Shutdown);
    }

    /// Expose the metrics registry.
    pub fn metrics(&self) -> Arc<RaftMetrics> {
        self.metrics.clone()
    }
}

// ─── RaftMessageHandler impl  (gRPC server calls these) ──────────────────────

#[async_trait::async_trait]
impl<S: Storage, T: NetworkTransport> RaftMessageHandler for RaftNode<S, T> {
    async fn handle_append_entries(
        &self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        let mut g = self.inner.lock().await;
        g.on_append_entries(req).await
    }

    async fn handle_request_vote(
        &self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RaftError> {
        let mut g = self.inner.lock().await;
        g.on_request_vote(req).await
    }

    async fn handle_install_snapshot(
        &self,
        req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        let mut g = self.inner.lock().await;
        g.on_install_snapshot(req).await
    }
}

// ─── Main event loop ─────────────────────────────────────────────────────────

async fn run_event_loop<S: Storage, T: NetworkTransport>(
    inner: Arc<Mutex<RaftNodeInner<S, T>>>,
    mut rx: ProposalReceiver,
    apply_tx: mpsc::UnboundedSender<LogEntry>,
    config: Arc<RaftConfig>,
    tx: ProposalSender,
) {
    // Restore hard state from storage.
    {
        let mut g = inner.lock().await;
        match g.storage.load_hard_state().await {
            Ok(hs) => {
                g.sm.current_term = hs.term;
                g.sm.voted_for = if hs.vote == 0 { None } else { Some(hs.vote) };
                g.sm.commit_index = hs.commit;
                g.sm.last_applied = hs.commit;
            }
            Err(e) => error!("failed to load hard state: {e}"),
        }
    }

    let heartbeat = Duration::from_millis(config.heartbeat_interval_ms);
    let mut heartbeat_deadline = Instant::now() + heartbeat;
    let mut election_deadline = next_election_deadline(&config);

    info!(id = config.id, "raft node started");

    loop {
        let deadline = {
            let g = inner.lock().await;
            if g.sm.role == RaftRole::Leader {
                heartbeat_deadline
            } else {
                election_deadline
            }
        };
        let timeout = tokio::time::sleep_until(deadline);
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    None | Some(RaftMessage::Shutdown) => {
                        info!("raft node shutting down");
                        break;
                    }
                    Some(RaftMessage::Propose(proposal)) => {
                        let mut g = inner.lock().await;
                        g.on_proposal(proposal).await;
                    }
                    Some(RaftMessage::GetLeader { tx }) => {
                        let g = inner.lock().await;
                        let _ = tx.send(g.sm.leader_id);
                    }
                    Some(RaftMessage::GetTerm { tx }) => {
                        let g = inner.lock().await;
                        let _ = tx.send(g.sm.current_term);
                    }
                    Some(RaftMessage::ReadIndex { tx }) => {
                        let g = inner.lock().await;
                        let _ = tx.send(Ok(g.sm.commit_index));
                    }
                    Some(RaftMessage::TransferLeader { target }) => {
                        info!(target, "leader transfer requested (not yet implemented)");
                    }
                    Some(RaftMessage::TriggerSnapshot { index }) => {
                        info!(index, "snapshot trigger (application-driven)");
                    }
                    Some(RaftMessage::VoteResponse { peer, resp }) => {
                        let mut g = inner.lock().await;
                        g.handle_vote_response(peer, resp).await;
                    }
                    Some(RaftMessage::AppendResponse { peer, req_last_index, resp }) => {
                        let mut g = inner.lock().await;
                        g.handle_append_response(peer, req_last_index, resp).await;
                    }
                    Some(RaftMessage::SnapshotResponse { peer, req_last_index, resp }) => {
                        let mut g = inner.lock().await;
                        g.handle_snapshot_response(peer, req_last_index, resp).await;
                    }
                }
            }
            _ = timeout => {
                let now = Instant::now();
                let mut g = inner.lock().await;
                let role = g.sm.role.clone();

                if now >= election_deadline && role != RaftRole::Leader {
                    // Election timeout fired → start election
                    g.start_election().await;
                    election_deadline = next_election_deadline(&config);
                    heartbeat_deadline = now + heartbeat;
                } else if now >= heartbeat_deadline && role == RaftRole::Leader {
                    // Heartbeat timeout fired → replicate / send heartbeats
                    g.replicate_all().await;
                    // Advance commit + apply.
                    g.maybe_advance_commit().await;
                    g.apply_committed(&apply_tx).await;
                    heartbeat_deadline = now + heartbeat;
                }
            }
        }
    }
}

fn next_election_deadline(config: &RaftConfig) -> Instant {
    let jitter = rand::thread_rng()
        .gen_range(config.election_timeout_min_ms..=config.election_timeout_max_ms);
    Instant::now() + Duration::from_millis(jitter)
}

// ─── Inner node logic ────────────────────────────────────────────────────────

impl<S: Storage, T: NetworkTransport> RaftNodeInner<S, T> {
    // ── RPC handlers ─────────────────────────────────────────────────────────

    async fn on_append_entries(
        &mut self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        let reject = |conflict_index: u64, conflict_term: u64, term: u64| AppendEntriesResponse {
            term,
            success: false,
            conflict_index,
            conflict_term,
        };

        // Step down if we see a higher term.
        if req.term > self.sm.current_term {
            self.sm.become_follower(req.term, Some(req.leader_id));
            self.save_hard_state().await;
        }

        if req.term < self.sm.current_term {
            return Ok(reject(0, 0, self.sm.current_term));
        }

        // Recognised leader — reset election clock (sent via the event loop
        // externally; here we just note it in the state).
        self.sm.leader_id = Some(req.leader_id);

        // Consistency check on prev_log_index / prev_log_term.
        if req.prev_log_index > 0 {
            let our_term = self.storage.term(req.prev_log_index).await;
            match our_term {
                Err(RaftError::LogCompacted { .. }) => {
                    // Snapshot covers the prev entry – fine to continue.
                }
                Err(_) => return Ok(reject(req.prev_log_index, 0, self.sm.current_term)),
                Ok(t) if t != req.prev_log_term => {
                    // Find first index of the conflicting term for fast rollback.
                    let conflict_index = self.first_index_of_term(t, req.prev_log_index).await;
                    return Ok(reject(conflict_index, t, self.sm.current_term));
                }
                Ok(_) => {}
            }
        }

        // Convert and append entries.
        if !req.entries.is_empty() {
            let entries: Vec<LogEntry> = req
                .entries
                .into_iter()
                .map(proto_entry_to_log_entry)
                .collect();
            self.storage.append_entries(&entries).await?;
        }

        // Advance commit.
        if req.leader_commit > self.sm.commit_index {
            let last = self.storage.last_index().await.unwrap_or(0);
            self.sm.commit_index = req.leader_commit.min(last);
            self.save_hard_state().await;
        }

        self.metrics.append_entries_received.inc();
        Ok(AppendEntriesResponse {
            term: self.sm.current_term,
            success: true,
            conflict_index: 0,
            conflict_term: 0,
        })
    }

    async fn on_request_vote(
        &mut self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RaftError> {
        let deny = |term: u64| RequestVoteResponse {
            term,
            vote_granted: false,
        };

        if req.term > self.sm.current_term {
            self.sm.become_follower(req.term, None);
            self.save_hard_state().await;
        }

        if req.term < self.sm.current_term {
            return Ok(deny(self.sm.current_term));
        }

        let already_voted = matches!(self.sm.voted_for, Some(v) if v != req.candidate_id);
        if already_voted {
            return Ok(deny(self.sm.current_term));
        }

        // Check log up-to-dateness.
        let our_last_index = self.storage.last_index().await.unwrap_or(0);
        let our_last_term = if our_last_index > 0 {
            self.storage.term(our_last_index).await.unwrap_or(0)
        } else {
            0
        };

        let candidate_log_ok = req.last_log_term > our_last_term
            || (req.last_log_term == our_last_term && req.last_log_index >= our_last_index);

        if !candidate_log_ok {
            return Ok(deny(self.sm.current_term));
        }

        self.sm.voted_for = Some(req.candidate_id);
        self.save_hard_state().await;
        self.metrics.votes_granted.inc();

        Ok(RequestVoteResponse {
            term: self.sm.current_term,
            vote_granted: true,
        })
    }

    async fn on_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        if req.term > self.sm.current_term {
            self.sm.become_follower(req.term, Some(req.leader_id));
            self.save_hard_state().await;
        }

        if req.term < self.sm.current_term {
            return Ok(InstallSnapshotResponse {
                term: self.sm.current_term,
            });
        }

        let (meta, buf) = self.snapshot_buf.get_or_insert_with(|| {
            (
                SnapshotMetadata {
                    index: req.last_included_index,
                    term: req.last_included_term,
                    conf_nodes: vec![],
                },
                Vec::new(),
            )
        });

        buf.extend_from_slice(&req.data);

        if req.done {
            let snapshot = iceraft_core::Snapshot {
                data: buf.clone(),
                metadata: iceraft_core::SnapshotMeta {
                    last_included_index: meta.index,
                    last_included_term: meta.term,
                    conf_nodes: meta.conf_nodes.clone(),
                },
            };
            self.storage.apply_snapshot(snapshot).await?;
            self.sm.commit_index = self.sm.commit_index.max(req.last_included_index);
            self.sm.last_applied = self.sm.last_applied.max(req.last_included_index);
            self.snapshot_buf = None;
            self.save_hard_state().await;
            self.metrics.snapshots_installed.inc();
            info!(index = req.last_included_index, "snapshot installed");
        }

        Ok(InstallSnapshotResponse {
            term: self.sm.current_term,
        })
    }

    // ── Proposal ─────────────────────────────────────────────────────────────

    async fn on_proposal(&mut self, proposal: Proposal) {
        if self.sm.role != RaftRole::Leader {
            if let Some(tx) = proposal.tx {
                let _ = tx.send(Err(RaftError::NotLeader {
                    leader_id: self.sm.leader_id,
                }));
            }
            return;
        }

        let last_index = self.storage.last_index().await.unwrap_or(0);
        let new_index = last_index + 1;
        let entry = LogEntry {
            term: self.sm.current_term,
            index: new_index,
            data: proposal.data,
            entry_type: EntryType::Normal,
        };

        if let Err(e) = self.storage.append_entries(&[entry]).await {
            if let Some(tx) = proposal.tx {
                let _ = tx.send(Err(e));
            }
            return;
        }

        if let Some(tx) = proposal.tx {
            self.pending.push((new_index, tx));
        }

        // Immediately attempt replication.
        self.replicate_all().await;
    }

    // ── Election ─────────────────────────────────────────────────────────────

    async fn start_election(&mut self) {
        let self_id = self.config.id;
        self.sm.become_candidate(self_id);
        self.save_hard_state().await;

        let last_index = self.storage.last_index().await.unwrap_or(0);
        let last_term = if last_index > 0 {
            self.storage.term(last_index).await.unwrap_or(0)
        } else {
            0
        };

        info!(
            id = self_id,
            term = self.sm.current_term,
            "starting election"
        );
        self.metrics.elections_started.inc();

        let req = RequestVoteRequest {
            term: self.sm.current_term,
            candidate_id: self_id,
            last_log_index: last_index,
            last_log_term: last_term,
            pre_vote: false,
        };

        let quorum = self.config.quorum();
        let transport = self.transport.clone();

        // Fan out RequestVote RPCs as independent tasks.
        for i in 0..self.config.peers.len() {
            let peer = self.config.peers[i];
            let t = transport.clone();
            let r = req.clone();
            let tx = self.tx.clone();
            tokio::spawn(async move {
                if let Ok(resp) = t.send_request_vote(peer, r).await {
                    let _ = tx.send(RaftMessage::VoteResponse { peer, resp });
                }
            });
        }

        if self.sm.votes_received.len() >= quorum && self.sm.role == RaftRole::Candidate {
            let last_index = self.storage.last_index().await.unwrap_or(0);
            self.sm.become_leader(self_id, last_index, &self.config.peers);
            info!(id = self_id, term = self.sm.current_term, "became leader");
            self.metrics.leader_changes.inc();
            // Send immediate heartbeats.
            self.replicate_all().await;
        }
    }

    // ── Replication ──────────────────────────────────────────────────────────

    /// Send AppendEntries (or heartbeat) to every peer.
    async fn replicate_all(&mut self) {
        if self.sm.role != RaftRole::Leader {
            return;
        }

        for i in 0..self.config.peers.len() {
            let peer = self.config.peers[i];
            self.replicate_to(peer).await;
        }
    }

    async fn replicate_to(&mut self, peer: NodeId) {
        let progress = match self.sm.progress.get(&peer) {
            Some(p) => p.clone(),
            None => return,
        };

        let prev_log_index = progress.next_index - 1;
        let prev_log_term = if prev_log_index == 0 {
            0
        } else {
            self.storage.term(prev_log_index).await.unwrap_or(0)
        };

        let last_index = self.storage.last_index().await.unwrap_or(0);
        let entries = if last_index >= progress.next_index {
            match self
                .storage
                .entries(
                    progress.next_index,
                    last_index + 1,
                    Some((self.config.max_append_entries * 4096) as u64),
                )
                .await
            {
                Ok(e) => e
                    .into_iter()
                    .map(log_entry_to_proto_entry)
                    .collect::<Vec<_>>(),
                Err(RaftError::LogCompacted { .. }) => {
                    // Need to send snapshot instead.
                    self.send_snapshot_to(peer).await;
                    return;
                }
                Err(e) => {
                    warn!(peer, %e, "failed to read log for replication");
                    return;
                }
            }
        } else {
            vec![]
        };

        let req_last_index = prev_log_index + entries.len() as u64;
        let req = AppendEntriesRequest {
            term: self.sm.current_term,
            leader_id: self.config.id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.sm.commit_index,
        };

        let tx = self.tx.clone();
        let transport = self.transport.clone();
        tokio::spawn(async move {
            if let Ok(resp) = transport.send_append_entries(peer, req).await {
                let _ = tx.send(RaftMessage::AppendResponse {
                    peer,
                    req_last_index,
                    resp,
                });
            }
        });
    }

    async fn send_snapshot_to(&mut self, peer: NodeId) {
        match self.storage.snapshot().await {
            Ok(Some(snap)) => {
                let req = InstallSnapshotRequest {
                    term: self.sm.current_term,
                    leader_id: self.config.id,
                    last_included_index: snap.metadata.last_included_index,
                    last_included_term: snap.metadata.last_included_term,
                    offset: 0,
                    data: snap.data,
                    done: true,
                };
                let tx = self.tx.clone();
                let transport = self.transport.clone();
                let req_last_index = snap.metadata.last_included_index;
                tokio::spawn(async move {
                    if let Ok(resp) = transport.send_install_snapshot(peer, req).await {
                        let _ = tx.send(RaftMessage::SnapshotResponse {
                            peer,
                            req_last_index,
                            resp,
                        });
                    }
                });
            }
            Ok(None) => warn!(peer, "no snapshot available to send"),
            Err(e) => warn!(peer, %e, "failed to load snapshot"),
        }
    }

    // ── Commit + Apply ───────────────────────────────────────────────────────

    // ── Async RPC Callbacks ──────────────────────────────────────────────────

    async fn handle_vote_response(&mut self, peer: NodeId, resp: RequestVoteResponse) {
        if resp.term > self.sm.current_term {
            self.sm.become_follower(resp.term, None);
            self.save_hard_state().await;
            return;
        }
        if resp.vote_granted && self.sm.role == RaftRole::Candidate {
            self.sm.votes_received.insert(peer);
            let quorum = self.config.quorum();
            if self.sm.votes_received.len() >= quorum {
                let last_index = self.storage.last_index().await.unwrap_or(0);
                self.sm
                    .become_leader(self.config.id, last_index, &self.config.peers);
                info!(
                    id = self.config.id,
                    term = self.sm.current_term,
                    "became leader"
                );
                self.metrics.leader_changes.inc();
                self.replicate_all().await;
            }
        }
    }

    async fn handle_append_response(
        &mut self,
        peer: NodeId,
        req_last_index: LogIndex,
        resp: AppendEntriesResponse,
    ) {
        if resp.term > self.sm.current_term {
            self.sm.become_follower(resp.term, None);
            self.save_hard_state().await;
            return;
        }
        if self.sm.role != RaftRole::Leader {
            return;
        }
        if let Some(p) = self.sm.progress.get_mut(&peer) {
            if resp.success {
                p.match_index = p.match_index.max(req_last_index);
                p.next_index = p.match_index + 1;
                p.in_flight = false;
            } else {
                if resp.conflict_index > 0 {
                    p.next_index = resp.conflict_index;
                } else {
                    p.next_index = p.next_index.saturating_sub(1).max(1);
                }
            }
        }
        self.maybe_advance_commit().await;
    }

    async fn handle_snapshot_response(
        &mut self,
        peer: NodeId,
        req_last_index: LogIndex,
        resp: InstallSnapshotResponse,
    ) {
        if resp.term > self.sm.current_term {
            self.sm.become_follower(resp.term, None);
            self.save_hard_state().await;
            return;
        }
        if self.sm.role != RaftRole::Leader {
            return;
        }
        if let Some(p) = self.sm.progress.get_mut(&peer) {
            p.match_index = p.match_index.max(req_last_index);
            p.next_index = p.match_index + 1;
            p.in_flight = false;
        }
        self.maybe_advance_commit().await;
    }

    async fn maybe_advance_commit(&mut self) {
        if self.sm.role != RaftRole::Leader {
            return;
        }
        let last_index = self.storage.last_index().await.unwrap_or(0);
        let quorum = self.config.quorum();
        let term = self.sm.current_term;
        let storage = self.storage.clone();

        // ── Quorum commit check ───────────────────────────────────────────
        // Walk from last_index down and check replication counts directly.
        let mut new_commit = self.sm.commit_index;
        let mut n = last_index;
        while n > self.sm.commit_index {
            let entry_term = storage.term(n).await.unwrap_or(0);
            if entry_term == term {
                let replicated = self
                    .sm
                    .progress
                    .values()
                    .filter(|p| p.match_index >= n)
                    .count()
                    + 1; // +1 for leader itself

                if replicated >= quorum {
                    new_commit = n;
                    break;
                }
            }
            n -= 1;
        }

        if new_commit > self.sm.commit_index {
            self.sm.commit_index = new_commit;
            self.save_hard_state().await;
            debug!(commit_index = new_commit, "commit index advanced");
            self.metrics
                .entries_committed
                .inc_by(new_commit - self.sm.last_applied);
            self.metrics.commit_index.set(new_commit as i64);
        }

        // Notify pending proposals that have committed.
        let commit = self.sm.commit_index;
        let old_pending = std::mem::take(&mut self.pending);
        for (idx, tx) in old_pending {
            if idx <= commit {
                let _ = tx.send(Ok(idx));
            } else {
                self.pending.push((idx, tx));
            }
        }
    }

    async fn apply_committed(&mut self, apply_tx: &mpsc::UnboundedSender<LogEntry>) {
        let from = self.sm.last_applied + 1;
        let to = self.sm.commit_index;
        if from > to {
            return;
        }
        match self.storage.entries(from, to + 1, None).await {
            Ok(entries) => {
                for entry in entries {
                    let _ = apply_tx.send(entry.clone());
                    self.sm.last_applied = entry.index;
                }
            }
            Err(e) => error!("apply_committed: failed to read entries: {e}"),
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    async fn save_hard_state(&self) {
        let hs = HardState {
            term: self.sm.current_term,
            vote: self.sm.voted_for.unwrap_or(0),
            commit: self.sm.commit_index,
        };
        if let Err(e) = self.storage.save_hard_state(&hs).await {
            error!("failed to save hard state: {e}");
        }
        self.metrics.current_term.set(self.sm.current_term as i64);
    }

    /// Binary search for the first index at which `term` appears, up to `upper`.
    async fn first_index_of_term(&self, term: u64, upper: LogIndex) -> LogIndex {
        let first = self.storage.first_index().await.unwrap_or(1);
        let mut lo = first;
        let mut hi = upper;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.storage.term(mid).await {
                Ok(t) if t < term => lo = mid + 1,
                _ => hi = mid,
            }
        }
        lo
    }
}

// ─── Proto / core type conversions ───────────────────────────────────────────

fn proto_entry_to_log_entry(e: ProtoEntry) -> LogEntry {
    LogEntry {
        term: e.term,
        index: e.index,
        data: e.data,
        entry_type: if e.entry_type == ProtoEntryType::EntryConfChange as i32 {
            EntryType::ConfChange
        } else {
            EntryType::Normal
        },
    }
}

fn log_entry_to_proto_entry(e: LogEntry) -> ProtoEntry {
    ProtoEntry {
        term: e.term,
        index: e.index,
        data: e.data,
        entry_type: match e.entry_type {
            EntryType::Normal => ProtoEntryType::EntryNormal as i32,
            EntryType::ConfChange => ProtoEntryType::EntryConfChange as i32,
        },
    }
}
