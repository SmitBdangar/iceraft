//! # iceraft-node
//!
//! Core Raft state machine: leader election, log replication, and snapshots.
//!
//! ## Architecture
//!
//! ```text
//!   ┌──────────────────────────────────────────────┐
//!   │                  RaftNode                    │
//!   │  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
//!   │  │ Storage  │  │Transport │  │ Metrics  │   │
//!   │  └──────────┘  └──────────┘  └──────────┘   │
//!   │       ↑               ↓                      │
//!   │  ┌─────────────────────────────────────────┐ │
//!   │  │          RaftStateMachine               │ │
//!   │  │  Role: Follower | Candidate | Leader    │ │
//!   │  └─────────────────────────────────────────┘ │
//!   │       ↑ proposals              ↓ apply_tx     │
//!   └────── ┼ ──────────────────────┼ ─────────────┘
//!           │ (Tokio channel)        │
//!    Client Proposal              App state machine
//! ```

pub mod message;
pub mod node;
pub mod state;

pub use message::RaftMessage;
pub use node::RaftNode;
pub use state::{RaftRole, RaftStateMachine};
