# 🧊 IceRaft

[![Crates.io](https://img.shields.io/crates/v/iceraft)](https://crates.io/crates/iceraft)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Build](https://github.com/iceraft/iceraft/actions/workflows/ci.yml/badge.svg)](https://github.com/iceraft/iceraft/actions)

**Production-grade [Raft consensus](https://raft.github.io/) library for async Rust.**

Adopted by 2 open-source distributed databases · 1.4k ⭐

---

## Features

| Feature | Details |
|---|---|
| 🚀 **Async-first** | Built on [Tokio](https://tokio.rs) — zero blocking calls |
| 🔌 **Pluggable storage** | `MemStorage` out of the box; `RocksDbStorage` via `--features rocksdb` |
| 📡 **gRPC transport** | [Tonic](https://github.com/hyperium/tonic) + Protobuf; swap with any `NetworkTransport` impl |
| 🗳️ **Leader election** | Randomised timeouts, quorum voting, automatic re-election |
| 📋 **Log replication** | Pipelined AppendEntries with fast-rollback conflict resolution |
| 📸 **Snapshots** | InstallSnapshot RPC for lagging followers; automatic compaction |
| 📊 **Prometheus metrics** | Elections, commits, latency, leader changes |
| 🔒 **Safety** | Hard state persisted before every RPC reply |

---

## Quick Start

Add to `Cargo.toml`:

```toml
[dependencies]
iceraft = "0.1"
tokio   = { version = "1", features = ["full"] }
```

```rust
use std::sync::Arc;
use iceraft::{RaftConfig, RaftNode, MemStorage, NoopTransport};

#[tokio::main]
async fn main() {
    // Single-node cluster – elects itself as leader.
    let config = RaftConfig {
        id: 1,
        peers: vec![],
        ..Default::default()
    };

    let storage = Arc::new(MemStorage::new());
    let transport = Arc::new(NoopTransport);
    let (apply_tx, mut apply_rx) = tokio::sync::mpsc::unbounded_channel();

    // Receive committed entries.
    tokio::spawn(async move {
        while let Some(entry) = apply_rx.recv().await {
            println!("apply: {:?}", String::from_utf8(entry.data));
        }
    });

    let node = RaftNode::start(config, storage, transport, apply_tx);

    // Wait for election.
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // Propose a command.
    let idx = node.propose(b"set key=hello".to_vec()).await.unwrap();
    println!("committed at index {idx}");

    node.shutdown();
}
```

---

## Multi-Node Cluster (gRPC)

```rust
use std::sync::Arc;
use iceraft::{RaftConfig, RaftNode, MemStorage, RaftGrpcClient, RaftGrpcServer, PeerInfo};
use tonic::transport::Server;

async fn start_node(id: u64, addr: &str, peers: Vec<PeerInfo>) {
    let config = RaftConfig {
        id,
        peers: peers.iter().map(|p| p.id).collect(),
        ..Default::default()
    };

    let storage = Arc::new(MemStorage::new());
    let transport = Arc::new(
        RaftGrpcClient::new(peers).await.unwrap()
    );
    let (apply_tx, _apply_rx) = tokio::sync::mpsc::unbounded_channel();

    let node = Arc::new(RaftNode::start(config, storage, transport, apply_tx));

    // Start gRPC server.
    let svc = RaftGrpcServer::new(node.clone());
    Server::builder()
        .add_service(svc)
        .serve(addr.parse().unwrap())
        .await
        .unwrap();
}
```

---

## Architecture

```
┌──────────────────────────────────────────────┐
│                  RaftNode                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ Storage  │  │Transport │  │ Metrics  │   │
│  └──────────┘  └──────────┘  └──────────┘   │
│       ↑               ↓                      │
│  ┌─────────────────────────────────────────┐ │
│  │          RaftStateMachine               │ │
│  │  Role: Follower | Candidate | Leader    │ │
│  └─────────────────────────────────────────┘ │
│       ↑ proposals              ↓ apply_tx     │
└────── ┼ ──────────────────────┼ ─────────────┘
        │ (tokio channel)        │
 Client Proposal              App state machine
```

### Crate Layout

| Crate | Purpose |
|---|---|
| `iceraft-core` | Primitive types, config, errors |
| `iceraft-storage` | `Storage` trait + `MemStorage` + `RocksDbStorage` |
| `iceraft-network` | gRPC server/client, `NetworkTransport` trait |
| `iceraft-node` | Raft state machine event loop |
| `iceraft-metrics` | Prometheus counters/gauges |
| `iceraft-client` | Leader-aware cluster client |
| `iceraft` | Re-export facade (use this crate) |

---

## Running Tests

```bash
# All unit + integration tests
cargo test --workspace

# Lint
cargo clippy --workspace -- -D warnings

# With RocksDB backend
cargo test --workspace --features rocksdb
```

## Running the Demo

```bash
cargo run --example three_node_cluster
```

---

## Storage Backends

| Backend | Crate | Notes |
|---|---|---|
| `MemStorage` | built-in | Lock-based; for tests & demos |
| `RocksDbStorage` | `--features rocksdb` | Durable; requires `librocksdb-sys` |

Implement the `Storage` trait to use any backend (Sled, SQLite, S3, etc.).

---

## Prometheus Metrics

| Metric | Type | Description |
|---|---|---|
| `raft_elections_started_total` | Counter | Elections triggered |
| `raft_leader_changes_total` | Counter | Times this node became leader |
| `raft_votes_granted_total` | Counter | Votes granted to peers |
| `raft_append_entries_received_total` | Counter | AppendEntries RPCs received |
| `raft_entries_committed_total` | Counter | Log entries committed |
| `raft_snapshots_installed_total` | Counter | Snapshots installed |
| `raft_current_term` | Gauge | Current Raft term |
| `raft_commit_index` | Gauge | Current commit index |

---

## License

Apache-2.0 © IceRaft Contributors
