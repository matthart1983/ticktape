//! Reliable sequenced-stream transport (M3).
//!
//! Moves the sequencer's frame stream between processes and hosts with the
//! MoldUDP64/SoupBinTCP shape proven by exchange feeds:
//!
//! - **Packets over UDP** (unicast or multicast) or Unix datagram sockets,
//!   each carrying one or more sequenced frames. Delivery is unreliable and
//!   unordered by design.
//! - **A/B feed redundancy**: the publisher sends every packet on two
//!   independent channels; a receiver takes whichever copy of a given seq
//!   arrives first and discards the duplicate. Most "loss" costs nothing.
//! - **Gap-fill**: receivers order by `seq`, never by arrival. A missing
//!   seq is detected trivially and recovered with a unicast TCP range
//!   request to a [`Retransmitter`] (SoupBinTCP-style).
//! - **Heartbeats** advertise the publisher's high-water seq, so tail loss
//!   (nothing after the gap to reveal it) is detected too.
//!
//! The reliability core is [`Reassembler`] — a pure state machine with no
//! sockets, no clock, no threads — so the entire loss/reorder/duplication
//! space is fuzzed deterministically (see `tests/`). The socket layers are
//! thin shells around it.
//!
//! A [`Replica`] consumes the in-order stream into its own copy of your
//! `Service` — redundancy through determinism: ship the ordered inputs,
//! recompute identical state.
//!
//! Deferred (tracked in the spec): a shared-memory ring for same-box IPC
//! (Unix datagram sockets cover multi-process today; the shm ring is the
//! planned fast path), packet batching, and a journal-backed retransmit
//! store.

pub mod net;
pub mod reassembler;
pub mod replica;
#[cfg(feature = "shm")]
pub mod shm;
pub mod wire;

pub use net::{
    bind_udp, spawn_feed, ChainStore, FrameStore, JournalRewinder, MemStore, PacketSource,
    Publisher, PublisherConfig, Receiver, ReceiverConfig, Retransmitter,
};
pub use reassembler::Reassembler;
pub use replica::Replica;
#[cfg(feature = "shm")]
pub use shm::{ShmRing, ShmSource};
pub use wire::{Packet, MAX_PACKET_BYTES};

use std::fmt;
use ticktape_core::FrameError;

#[derive(Debug)]
pub enum TransportError {
    Io(std::io::Error),
    /// A packet failed structural validation (bad magic, CRC, layout).
    Corrupt(&'static str),
    /// A frame inside a packet failed its own validation.
    Frame(FrameError),
    /// Packet belongs to a different publisher session.
    SessionMismatch {
        expected: u64,
        got: u64,
    },
    /// A gap could not be filled (no retransmitter configured, or the
    /// retransmitter could not serve the range).
    GapUnrecoverable {
        from: u64,
        count: u64,
    },
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::Io(e) => write!(f, "transport I/O error: {e}"),
            TransportError::Corrupt(what) => write!(f, "corrupt packet: {what}"),
            TransportError::Frame(e) => write!(f, "corrupt frame in packet: {e}"),
            TransportError::SessionMismatch { expected, got } => {
                write!(f, "session mismatch: expected {expected}, got {got}")
            }
            TransportError::GapUnrecoverable { from, count } => {
                write!(f, "unrecoverable gap: {count} frames from seq {from}")
            }
        }
    }
}

impl std::error::Error for TransportError {}

impl From<std::io::Error> for TransportError {
    fn from(e: std::io::Error) -> Self {
        TransportError::Io(e)
    }
}

impl From<FrameError> for TransportError {
    fn from(e: FrameError) -> Self {
        TransportError::Frame(e)
    }
}
