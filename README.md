IceRaft - Production-grade Raft Consensus for Async Rust
=========================================================

IceRaft is a modular, async-first implementation of the Raft consensus
algorithm written in Rust. It is designed for use in distributed databases,
coordination services, and any system that requires replicated state with
strong consistency guarantees.

The library is built on Tokio and exposes a trait-based architecture so that
storage backends and network transports can be swapped without touching the
core consensus logic.


CONTENTS
--------
  1. Features
  2. Repository Layout
  3. Requirements
  4. Building
  5. Quick Start
  6. Multi-node Cluster (gRPC)
  7. Storage Backends
  8. Prometheus Metrics
  9. Architecture
 10. Contributing
 11. License


1. FEATURES
-----------

  Async-first
      Built entirely on Tokio. No blocking calls anywhere in the hot path.

  Pluggable Storage
      MemStorage ships out of the box. RocksDbStorage is available via the
      "rocksdb" cargo feature. Any backend implementing the Storage trait works.

  gRPC Transport
      The default transport is Tonic + Protobuf (see iceraft-network). Replace
      it by implementing the NetworkTransport trait.

  Leader Election
      Randomised election timeouts, quorum-based voting, and automatic
      re-election on leader failure.

  Log Replication
      Pipelined AppendEntries with fast-rollback conflict resolution for
      consistent and efficient log catch-up.

  Snapshots
      InstallSnapshot RPC for lagging followers and automatic log compaction
      to bound disk usage.

  Prometheus Metrics
      Counters and gauges for elections, commits, latency, and leader changes
      (see Section 8).

  Safety
      Hard state (currentTerm, votedFor, log) is persisted to stable storage
      before any RPC reply is sent, matching the Raft safety requirements.


2. REPOSITORY LAYOUT
--------------------

  iceraft/              Re-export facade — depend on this crate in your project
  iceraft-core/         Primitive types, RaftConfig, error types
  iceraft-storage/      Storage trait, MemStorage, RocksDbStorage
  iceraft-network/      NetworkTransport trait, gRPC server and client (Tonic)
  iceraft-node/         Raft state machine event loop
  iceraft-metrics/      Prometheus counters and gauges
  iceraft-client/       Leader-aware cluster client
  proto/                Protobuf definitions for the Raft RPC service
  examples/             Runnable demos (see three_node_cluster)


3. REQUIREMENTS
---------------

  - Rust 1.75 or later (async trait stabilisation)
  - Tokio runtime
  - protoc (Protocol Buffer compiler) — required to regenerate proto bindings
  - librocksdb-sys — only if building with --features rocksdb


4. BUILDING
-----------

Clone the repository and build all crates:

    git clone https://github.com/SmitBdangar/iceraft.git
    cd iceraft
    cargo build --workspace

Run all unit and integration tests:

    cargo test --workspace

Run with the RocksDB storage backend:

    cargo test --workspace --features rocksdb

Lint the workspace:

    cargo clippy --workspace -- -D warnings

Run the three-node demo:

    cargo run --example three_node_cluster


5. QUICK START
--------------

Add the dependency to your Cargo.toml:

    [dependencies]
    iceraft = "0.1"
    tokio   = { version = "1", features = ["full"] }

The example below starts a single-node cluster. A single node elects itself
leader immediately and can accept proposals.

    use std::sync::Arc;
    use iceraft::{RaftConfig, RaftNode, MemStorage, NoopTransport};

    #[tokio::main]
    async fn main() {
        let config = RaftConfig {
            id: 1,
            peers: vec![],
            ..Default::default()
        };

        let storage   = Arc::new(MemStorage::new());
        let transport = Arc::new(NoopTransport);
        let (apply_tx, mut apply_rx) = tokio::sync::mpsc::unbounded_channel();

        // Receive committed log entries and apply them to your state machine.
        tokio::spawn(async move {
            while let Some(entry) = apply_rx.recv().await {
                println!("apply: {:?}", String::from_utf8(entry.data));
            }
        });

        let node = RaftNode::start(config, storage, transport, apply_tx);

        // Allow time for election to complete.
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;

        // Propose a command. Returns the committed log index.
        let idx = node.propose(b"set key=hello".to_vec()).await.unwrap();
        println!("committed at index {idx}");

        node.shutdown();
    }


6. MULTI-NODE CLUSTER (gRPC)
-----------------------------

For a real cluster, use RaftGrpcClient as the transport and expose each node
via RaftGrpcServer. PeerInfo carries the node id and its gRPC address.

    use std::sync::Arc;
    use iceraft::{
        RaftConfig, RaftNode, MemStorage,
        RaftGrpcClient, RaftGrpcServer, PeerInfo,
    };
    use tonic::transport::Server;

    async fn start_node(id: u64, addr: &str, peers: Vec<PeerInfo>) {
        let config = RaftConfig {
            id,
            peers: peers.iter().map(|p| p.id).collect(),
            ..Default::default()
        };

        let storage   = Arc::new(MemStorage::new());
        let transport = Arc::new(RaftGrpcClient::new(peers).await.unwrap());
        let (apply_tx, _apply_rx) = tokio::sync::mpsc::unbounded_channel();

        let node = Arc::new(RaftNode::start(config, storage, transport, apply_tx));

        let svc = RaftGrpcServer::new(node.clone());
        Server::builder()
            .add_service(svc)
            .serve(addr.parse().unwrap())
            .await
            .unwrap();
    }

See examples/three_node_cluster for a fully working local cluster with three
nodes started concurrently.


7. STORAGE BACKENDS
-------------------

  Backend          Crate / Feature         Notes
  -------          ---------------         -----
  MemStorage       built-in                Mutex-protected in-memory log.
                                           Suitable for tests and demos.
                                           Data is lost on process exit.

  RocksDbStorage   --features rocksdb      Durable, LSM-tree backed storage.
                                           Requires librocksdb-sys to be
                                           available on the build host.

Custom Backends

  Implement the Storage trait from iceraft-storage to use any backend — Sled,
  SQLite, a cloud object store, or anything else. The trait requires:

    - append_entries(entries)  — persist a batch of log entries
    - get_entries(from, to)    — read a log slice
    - save_hard_state(hs)      — atomically persist term + votedFor
    - load_hard_state()        — recover hard state on restart
    - save_snapshot(snap)      — write a snapshot
    - load_snapshot()          — recover the latest snapshot


8. PROMETHEUS METRICS
---------------------

IceRaft registers the following metrics with the default Prometheus registry.
Scrape /metrics from your Prometheus exporter as usual.

  Metric name                            Type     Description
  -----------                            ----     -----------
  raft_elections_started_total           Counter  Elections triggered
  raft_leader_changes_total              Counter  Times this node became leader
  raft_votes_granted_total               Counter  Votes granted to peers
  raft_append_entries_received_total     Counter  AppendEntries RPCs received
  raft_entries_committed_total           Counter  Log entries committed
  raft_snapshots_installed_total         Counter  Snapshots installed
  raft_current_term                      Gauge    Current Raft term
  raft_commit_index                      Gauge    Current commit index

The iceraft-metrics crate wraps the prometheus crate and can be disabled if
you provide your own instrumentation by implementing the Metrics trait.


9. ARCHITECTURE
---------------

The diagram below shows the relationship between the main components inside
a single RaftNode and the application that embeds it.

                          Application
                         /           \
                  propose()        apply_rx
                        |               |
    ┌───────────────────▼───────────────▼────────────────┐
    │                      RaftNode                       │
    │                                                     │
    │  ┌─────────────────────────────────────────────┐   │
    │  │             RaftStateMachine                 │   │
    │  │   Role: Follower | Candidate | Leader        │   │
    │  └──────────┬──────────────────────┬───────────┘   │
    │             │                      │               │
    │      ┌──────▼──────┐    ┌──────────▼──────────┐   │
    │      │   Storage   │    │   NetworkTransport   │   │
    │      │  (trait)    │    │      (trait)         │   │
    │      └─────────────┘    └─────────────────────-┘   │
    │                                                     │
    │      ┌────────────────────────────────────────┐    │
    │      │             Metrics                     │   │
    │      └────────────────────────────────────────┘   │
    └────────────────────────────────────────────────────┘

The RaftStateMachine runs in a dedicated Tokio task. It communicates with the
caller via async channels (propose_tx / apply_tx) so proposals from multiple
concurrent tasks are naturally serialised through the event loop without
external locking.

Hard state is always written to Storage before a response is sent on the
network, which is the core invariant that Raft relies on for safety across
crashes.


10. CONTRIBUTING
----------------

Bug reports, feature requests, and pull requests are welcome on GitHub:

    https://github.com/SmitBdangar/iceraft

Before submitting a pull request please:

  1. Run `cargo test --workspace` and ensure all tests pass.
  2. Run `cargo clippy --workspace -- -D warnings` with no warnings.
  3. Run `cargo fmt --all` to apply standard formatting.
  4. Add or update tests that cover the changed behaviour.
  5. Update relevant documentation in the affected crate's doc comments.

For significant changes, open an issue first to discuss the approach before
writing code. This avoids duplicate effort and helps us reach agreement on the
design early.


11. LICENSE
-----------

IceRaft is distributed under the Apache License, Version 2.0.

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the LICENSE file in the
repository root for the full text.
