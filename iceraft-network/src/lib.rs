//! # iceraft-network
//!
//! gRPC transport layer built on [tonic].
//!
//! ## Overview
//!
//! - [`RaftGrpcServer`] – tonic server that receives RPCs and forwards them
//!   to an inner [`RaftMessageHandler`].
//! - [`RaftGrpcClient`] – pooled client stubs, one per peer.
//! - [`NetworkTransport`] – trait used by `iceraft-node` to send outgoing RPCs.

pub mod client;
pub mod server;
pub mod transport;

// Generated protobuf / tonic code
pub mod proto {
    // Pre-generated from proto/raft.proto (avoids runtime protoc dependency).
    include!("gen/raft.rs");
}

pub use client::RaftGrpcClient;
pub use server::RaftGrpcServer;
pub use transport::{NetworkTransport, RaftMessageHandler};
