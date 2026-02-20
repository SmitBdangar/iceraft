//! # iceraft-storage
//!
//! Defines the [`Storage`] trait that all storage backends must implement,
//! plus two built-in implementations:
//!
//! - [`MemStorage`] – lock-based in-memory store (testing / development)
//! - `RocksDbStorage` – durable on-disk store (feature-gated `rocksdb`)

pub mod mem;
#[cfg(feature = "rocksdb")]
pub mod rocks;

pub use mem::MemStorage;
#[cfg(feature = "rocksdb")]
pub use rocks::RocksDbStorage;

use async_trait::async_trait;
use iceraft_core::{HardState, LogEntry, LogIndex, RaftError, Snapshot, Term};

// ─── Storage Trait ───────────────────────────────────────────────────────────

/// The storage interface that every IceRaft backend must satisfy.
///
/// All methods are non-blocking from the caller's perspective; implementations
/// that do I/O should be async or offload to a dedicated thread pool.
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    // ── Hard state ──────────────────────────────────────────────────────────

    /// Persist hard state (term, vote, commit) atomically.
    async fn save_hard_state(&self, hs: &HardState) -> Result<(), RaftError>;

    /// Load the last persisted hard state. Returns a zero-initialised
    /// [`HardState`] if nothing has been saved yet.
    async fn load_hard_state(&self) -> Result<HardState, RaftError>;

    // ── Log ─────────────────────────────────────────────────────────────────

    /// Index of the first available log entry (entries before this have been
    /// replaced by a snapshot).  Returns `1` when there are no entries or no
    /// snapshot.
    async fn first_index(&self) -> Result<LogIndex, RaftError>;

    /// Index of the last log entry, or `0` if the log is empty.
    async fn last_index(&self) -> Result<LogIndex, RaftError>;

    /// Term of the entry at `index`.  May return
    /// [`RaftError::LogCompacted`] if the entry was already snapshotted.
    async fn term(&self, index: LogIndex) -> Result<Term, RaftError>;

    /// Return entries in the range `[lo, hi)`.  Implementations MAY cap the
    /// result at `max_size` bytes; callers must handle partial results.
    async fn entries(
        &self,
        lo: LogIndex,
        hi: LogIndex,
        max_size: Option<u64>,
    ) -> Result<Vec<LogEntry>, RaftError>;

    /// Append a batch of new entries to the log, truncating any conflicting
    /// suffix before appending.
    async fn append_entries(&self, entries: &[LogEntry]) -> Result<(), RaftError>;

    // ── Snapshots ───────────────────────────────────────────────────────────

    /// Return the most recent snapshot, or `None` if no snapshot has been
    /// taken yet.
    async fn snapshot(&self) -> Result<Option<Snapshot>, RaftError>;

    /// Persist a snapshot, removing all log entries that were included in it.
    async fn apply_snapshot(&self, snapshot: Snapshot) -> Result<(), RaftError>;
}
