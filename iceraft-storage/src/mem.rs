//! In-memory storage backend – suitable for tests and prototyping.

use std::sync::Arc;

use async_trait::async_trait;
use iceraft_core::{HardState, LogEntry, LogIndex, RaftError, Snapshot, SnapshotMeta, Term};
use parking_lot::RwLock;

use crate::Storage;

// ─── Inner State ─────────────────────────────────────────────────────────────

#[derive(Default)]
struct MemStorageInner {
    hard_state: HardState,
    /// All entries currently in memory.  The slice is shifted so that
    /// `entries[0].index` equals the first available index after a snapshot.
    entries: Vec<LogEntry>,
    snapshot: Option<Snapshot>,
}

impl MemStorageInner {
    fn first_index(&self) -> LogIndex {
        match &self.snapshot {
            Some(s) => s.metadata.last_included_index + 1,
            None => 1,
        }
    }

    fn last_index(&self) -> LogIndex {
        if let Some(last) = self.entries.last() {
            last.index
        } else if let Some(s) = &self.snapshot {
            s.metadata.last_included_index
        } else {
            0
        }
    }

    fn term(&self, index: LogIndex) -> Result<Term, RaftError> {
        if index == 0 {
            return Ok(0);
        }
        // Check if covered by snapshot
        if let Some(s) = &self.snapshot {
            if index <= s.metadata.last_included_index {
                if index == s.metadata.last_included_index {
                    return Ok(s.metadata.last_included_term);
                }
                return Err(RaftError::LogCompacted {
                    requested: index,
                    first_available: s.metadata.last_included_index + 1,
                });
            }
        }
        let first = self.first_index();
        if index < first {
            return Err(RaftError::LogCompacted {
                requested: index,
                first_available: first,
            });
        }
        let offset = (index - first) as usize;
        self.entries
            .get(offset)
            .map(|e| e.term)
            .ok_or(RaftError::LogUnavailable(index))
    }

    fn entries(
        &self,
        lo: LogIndex,
        hi: LogIndex,
        max_size: Option<u64>,
    ) -> Result<Vec<LogEntry>, RaftError> {
        let first = self.first_index();
        let last = self.last_index();

        if lo < first {
            return Err(RaftError::LogCompacted {
                requested: lo,
                first_available: first,
            });
        }
        if hi > last + 1 {
            return Err(RaftError::LogUnavailable(hi));
        }
        if lo >= hi {
            return Ok(vec![]);
        }

        let lo_offset = (lo - first) as usize;
        let hi_offset = (hi - first) as usize;
        let slice = &self.entries[lo_offset..hi_offset];

        if let Some(max) = max_size {
            let mut result = Vec::new();
            let mut total_bytes: u64 = 0;
            for entry in slice {
                let size = entry.data.len() as u64;
                if !result.is_empty() && total_bytes + size > max {
                    break;
                }
                total_bytes += size;
                result.push(entry.clone());
            }
            Ok(result)
        } else {
            Ok(slice.to_vec())
        }
    }

    fn append_entries(&mut self, entries: &[LogEntry]) -> Result<(), RaftError> {
        if entries.is_empty() {
            return Ok(());
        }
        let first = self.first_index();
        let last = self.last_index();
        let incoming_first = entries[0].index;

        // Discard any existing entries that conflict.
        if incoming_first <= last {
            let truncate_at = (incoming_first - first) as usize;
            if truncate_at < self.entries.len() {
                self.entries.truncate(truncate_at);
            }
        }

        self.entries.extend_from_slice(entries);
        Ok(())
    }
}

// ─── MemStorage ──────────────────────────────────────────────────────────────

/// Thread-safe, in-memory implementation of [`Storage`].
#[derive(Clone, Default)]
pub struct MemStorage {
    inner: Arc<RwLock<MemStorageInner>>,
}

impl MemStorage {
    /// Create a new empty in-memory store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a store pre-populated with a snapshot (useful in tests).
    pub fn with_snapshot(snapshot: Snapshot) -> Self {
        let mut inner = MemStorageInner::default();
        inner.snapshot = Some(snapshot);
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }
}

#[async_trait]
impl Storage for MemStorage {
    async fn save_hard_state(&self, hs: &HardState) -> Result<(), RaftError> {
        self.inner.write().hard_state = hs.clone();
        Ok(())
    }

    async fn load_hard_state(&self) -> Result<HardState, RaftError> {
        Ok(self.inner.read().hard_state.clone())
    }

    async fn first_index(&self) -> Result<LogIndex, RaftError> {
        Ok(self.inner.read().first_index())
    }

    async fn last_index(&self) -> Result<LogIndex, RaftError> {
        Ok(self.inner.read().last_index())
    }

    async fn term(&self, index: LogIndex) -> Result<Term, RaftError> {
        self.inner.read().term(index)
    }

    async fn entries(
        &self,
        lo: LogIndex,
        hi: LogIndex,
        max_size: Option<u64>,
    ) -> Result<Vec<LogEntry>, RaftError> {
        self.inner.read().entries(lo, hi, max_size)
    }

    async fn append_entries(&self, entries: &[LogEntry]) -> Result<(), RaftError> {
        self.inner.write().append_entries(entries)
    }

    async fn snapshot(&self) -> Result<Option<Snapshot>, RaftError> {
        Ok(self.inner.read().snapshot.clone())
    }

    async fn apply_snapshot(&self, snapshot: Snapshot) -> Result<(), RaftError> {
        let mut inner = self.inner.write();
        let last_included = snapshot.metadata.last_included_index;
        // Drop all log entries covered by the snapshot
        let first = inner.first_index();
        if last_included >= first {
            let drop_count = (last_included - first + 1) as usize;
            if drop_count >= inner.entries.len() {
                inner.entries.clear();
            } else {
                inner.entries.drain(0..drop_count);
            }
        }
        inner.snapshot = Some(snapshot);
        Ok(())
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use iceraft_core::{EntryType, SnapshotMeta};

    use super::*;

    fn make_entry(term: Term, index: LogIndex) -> LogEntry {
        LogEntry {
            term,
            index,
            data: vec![index as u8],
            entry_type: EntryType::Normal,
        }
    }

    #[tokio::test]
    async fn test_empty_storage() {
        let s = MemStorage::new();
        assert_eq!(s.first_index().await.unwrap(), 1);
        assert_eq!(s.last_index().await.unwrap(), 0);
        let hs = s.load_hard_state().await.unwrap();
        assert_eq!(hs, HardState::default());
        assert!(s.snapshot().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_append_and_read() {
        let s = MemStorage::new();
        let entries = vec![make_entry(1, 1), make_entry(1, 2), make_entry(2, 3)];
        s.append_entries(&entries).await.unwrap();

        assert_eq!(s.last_index().await.unwrap(), 3);
        assert_eq!(s.first_index().await.unwrap(), 1);
        assert_eq!(s.term(2).await.unwrap(), 1);
        assert_eq!(s.term(3).await.unwrap(), 2);

        let got = s.entries(1, 4, None).await.unwrap();
        assert_eq!(got.len(), 3);
    }

    #[tokio::test]
    async fn test_truncation_on_conflict() {
        let s = MemStorage::new();
        s.append_entries(&[make_entry(1, 1), make_entry(1, 2), make_entry(1, 3)])
            .await
            .unwrap();
        // Overwrite from index 2 with a different term
        s.append_entries(&[make_entry(2, 2), make_entry(2, 3)])
            .await
            .unwrap();

        assert_eq!(s.last_index().await.unwrap(), 3);
        assert_eq!(s.term(2).await.unwrap(), 2);
        assert_eq!(s.term(3).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_hard_state_persistence() {
        let s = MemStorage::new();
        let hs = HardState {
            term: 5,
            vote: 2,
            commit: 10,
        };
        s.save_hard_state(&hs).await.unwrap();
        assert_eq!(s.load_hard_state().await.unwrap(), hs);
    }

    #[tokio::test]
    async fn test_apply_snapshot() {
        let s = MemStorage::new();
        // First add some entries
        for i in 1u64..=5 {
            s.append_entries(&[make_entry(1, i)]).await.unwrap();
        }
        let snap = Snapshot {
            data: b"state-at-5".to_vec(),
            metadata: SnapshotMeta {
                last_included_index: 5,
                last_included_term: 1,
                conf_nodes: vec![1, 2, 3],
            },
        };
        s.apply_snapshot(snap).await.unwrap();
        assert_eq!(s.first_index().await.unwrap(), 6);
        assert_eq!(s.last_index().await.unwrap(), 5);
        assert!(s.snapshot().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_max_size_cap() {
        let s = MemStorage::new();
        for i in 1u64..=10 {
            s.append_entries(&[LogEntry {
                term: 1,
                index: i,
                data: vec![0u8; 100],
                entry_type: EntryType::Normal,
            }])
            .await
            .unwrap();
        }
        // Only allow 350 bytes – should get 3 entries
        let got = s.entries(1, 11, Some(350)).await.unwrap();
        assert_eq!(got.len(), 3);
    }
}
