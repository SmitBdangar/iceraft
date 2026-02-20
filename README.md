IceRaft
=======

Production-grade Raft consensus library for async Rust. Built on Tokio.
Storage backends and network transports are trait-based and fully swappable.


BUILDING
--------

    git clone https://github.com/SmitBdangar/iceraft.git
    cd iceraft
    cargo build --workspace
    cargo test --workspace
    cargo run --example three_node_cluster


USAGE
-----

    [dependencies]
    iceraft = "0.1"
    tokio   = { version = "1", features = ["full"] }

    use std::sync::Arc;
    use iceraft::{RaftConfig, RaftNode, MemStorage, NoopTransport};

    #[tokio::main]
    async fn main() {
        let (apply_tx, mut apply_rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(async move {
            while let Some(entry) = apply_rx.recv().await {
                println!("apply: {:?}", String::from_utf8(entry.data));
            }
        });

        let node = RaftNode::start(
            RaftConfig { id: 1, peers: vec![], ..Default::default() },
            Arc::new(MemStorage::new()),
            Arc::new(NoopTransport),
            apply_tx,
        );

        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        let idx = node.propose(b"set key=hello".to_vec()).await.unwrap();
        println!("committed at index {idx}");
        node.shutdown();
    }

For a multi-node gRPC cluster, swap NoopTransport for RaftGrpcClient and
expose each node with RaftGrpcServer. See examples/three_node_cluster.


STORAGE
-------

  MemStorage       built-in            In-memory. Tests and demos only.
  RocksDbStorage   --features rocksdb  Durable. Requires librocksdb-sys.

Bring your own backend by implementing the Storage trait in iceraft-storage.


CRATES
------

  iceraft            Public facade — the only crate you need to import
  iceraft-core       Types, config, errors
  iceraft-storage    Storage trait + MemStorage + RocksDbStorage
  iceraft-network    NetworkTransport trait, gRPC server/client (Tonic)
  iceraft-node       Raft state machine event loop
  iceraft-metrics    Prometheus counters and gauges
  iceraft-client     Leader-aware cluster client


LICENSE
-------

Apache 2.0. See LICENSE.
