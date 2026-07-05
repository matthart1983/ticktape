//! raft-rs delegation backend (spec §9 open question) — feature-gated.
//!
//! Ticktape's default replication is its own epoch-lease election + fence over
//! the journal-as-system-of-record, which the deterministic simulator can
//! fault-inject end to end (the DST moat). This optional backend delegates
//! *log ordering* to [`raft`] (tikv/raft-rs) instead, while Ticktape keeps
//! owning the deterministic [`Service`] as the replicated state machine:
//! committed Raft entries are decoded and fed to `Service::apply`, so the
//! application logic and its invariants are unchanged regardless of which
//! algorithm ordered the log.
//!
//! **Why raft-rs and not a batteries-included framework:** raft-rs is a
//! *synchronous consensus module you drive yourself* — you own the storage,
//! the tick clock, and the `Ready` loop. That matches Ticktape's synchronous,
//! run-to-completion model and, crucially, means the whole thing can be
//! stepped deterministically (this crate's tests drive N nodes in-process with
//! no threads or wall-clock), so a Raft-backed deployment keeps far more of the
//! DST story than an async framework that owns its own event loop would.
//!
//! **Build requirement:** raft-rs generates its wire types from `.proto` at
//! build time, so building with `--features raft-backend` needs `protoc` on
//! PATH. raft-proto 0.7 vendors an older codegen that only parses the legacy
//! `libprotoc 3.x.y` version string, so use a 3.21-era protoc (e.g. macOS:
//! `brew install protobuf@21`). The default build (feature off) needs nothing.
//!
//! Enable with the `raft-backend` feature. [`ServiceNode`] is one node: it
//! wraps a `RawNode` + your `Service`, and you tick it, propose inputs on the
//! leader, and route the messages it emits to peers — a few lines, all under
//! your control (see the crate tests for a 3-node convergence harness).

#![cfg(feature = "raft-backend")]

mod node;

pub use node::{ServiceNode, StepError};
