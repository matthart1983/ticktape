//! Replication tiers and failover (M4) — the hardest correctness surface,
//! stated precisely rather than hand-waved (spec §9).
//!
//! Two concerns that naive single-sequencer designs conflate:
//!
//! 1. **Data replication** is deterministic replay: ship the ordered
//!    inputs, every replica computes bit-identical state. The transport
//!    layer (M3) already does this; no per-message voting needed.
//! 2. **Commit + leadership** cannot be done safely by a lone sequencer:
//!    two leaders assigning the same seq is split-brain, and preventing it
//!    requires a quorum *somewhere*.
//!
//! This crate provides that quorum machinery as **pure, transport-free
//! state machines** — the same discipline as the transport's
//! `Reassembler` — so every safety property is verified in deterministic
//! simulation (`tests/cluster_sim.rs`: leader kills, partitions, message
//! loss, dueling candidates):
//!
//! - [`Acceptor`] / [`Election`] — epoch leases by majority grant. An
//!   acceptor promises each epoch to at most one candidate (monotonic
//!   `promised`), so **at most one leader can ever hold a given epoch**.
//!   Grants carry the acceptor's journal high-water so a winner knows how
//!   far a majority has seen.
//! - [`EpochChange`] — the fence. The first frame of a new epoch names the
//!   epoch and its start seq; replicas reject stream messages from older
//!   epochs, and anything a dead leader sequenced past the fence point is
//!   discarded, not silently merged.
//! - [`CommitTracker`] — Tier 2 (quorum-committed): an input's outputs are
//!   withheld until a **majority** of replicas have durably journaled it.
//!   Combined with elections that sync to the max high-water of a granting
//!   majority, any two majorities intersect ⇒ **no committed input is ever
//!   lost on failover**.
//!
//! The tiers (chosen per deployment, spec §9):
//!
//! - **Tier 0** — single node (M0): durability = fsync policy; no HA.
//! - **Tier 1** — async hot standby: lowest latency; on failover, inputs
//!   sequenced-but-not-replicated in the final instants are lost — a
//!   *bounded, documented* window (the historical exchange tradeoff). The
//!   simulator asserts the loss is exactly that window, never more.
//! - **Tier 2** — quorum-committed: one replication round-trip on the
//!   commit path buys no-loss failover. The simulator asserts it.
//!
//! Not in this milestone, documented: the `openraft` delegation backend
//! (the builtin VSR-style quorum is what the simulator can prove; a
//! `raft-backend` feature remains open in the spec), acceptor-state
//! durability across acceptor restarts (acceptors must persist `promised`;
//! the simulator does not yet crash acceptors), and live lease renewal
//! timers (the simulator schedules suspicion adversarially instead, which
//! covers strictly more interleavings than any timer would).

pub mod commit;
pub mod election;
pub mod epoch;

pub use commit::CommitTracker;
pub use election::{Acceptor, Election, ElectionOutcome, VoteReply, VoteRequest};
pub use epoch::EpochChange;

/// Which durability tier a cluster runs (spec §9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// Async hot standby: outputs released immediately; failover may lose
    /// the unreplicated tail (bounded, documented).
    AsyncStandby,
    /// Quorum-committed: outputs released only once a majority has
    /// journaled the input; failover loses nothing committed.
    QuorumCommit,
}

/// Majority for a cluster of `n` voters.
pub fn majority(n: usize) -> usize {
    n / 2 + 1
}
