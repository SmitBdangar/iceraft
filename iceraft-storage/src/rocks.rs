//! RocksDB storage backend (optional).

use crate::Storage;
use iceraft_core::{HardState, LogEntry, LogIndex, RaftError, Snapshot, Term};

/// A placeholder for RocksDB storage.
pub struct RocksDbStorage;

#[async_trait::async_trait]
impl Storage for RocksDbStorage {
    async fn append_entries(&self, _entries: &[LogEntry]) -> Result<(), RaftError> {
        unimplemented!("RocksDB storage not implemented in this version")
    }

    async fn entries(
        &self,
        _low: LogIndex,
        _high: LogIndex,
        _max_size: Option<u64>,
    ) -> Result<Vec<LogEntry>, RaftError> {
        unimplemented!()
    }

    async fn term(&self, _index: LogIndex) -> Result<Term, RaftError> {
        unimplemented!()
    }

    async fn first_index(&self) -> Result<LogIndex, RaftError> {
        unimplemented!()
    }

    async fn last_index(&self) -> Result<LogIndex, RaftError> {
        unimplemented!()
    }

    async fn save_hard_state(&self, _hs: &HardState) -> Result<(), RaftError> {
        unimplemented!()
    }

    async fn load_hard_state(&self) -> Result<HardState, RaftError> {
        unimplemented!()
    }

    async fn apply_snapshot(&self, _snapshot: Snapshot) -> Result<(), RaftError> {
        unimplemented!()
    }

    async fn snapshot(&self) -> Result<Option<Snapshot>, RaftError> {
        unimplemented!()
    }
}
