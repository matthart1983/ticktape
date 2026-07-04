//! Ticktape — a framework for deterministic, replicated services on the
//! sequencer architecture.
//!
//! A single logical **sequencer** imposes a total order on all inputs and
//! writes them to a durable **journal**; one or more **deterministic state
//! machines** (your application) consume the identical stream and compute
//! bit-identical state. You write a [`Service`] — a pure
//! `(state, input) -> outputs` step function; Ticktape provides ordering,
//! durability, and (in later milestones) replication, transport, and
//! deterministic-simulation testing.
//!
//! This facade re-exports the common surface:
//!
//! ```
//! use ticktape::{Ctx, Decode, Encode, Node, NodeConfig, Seq, Service};
//!
//! struct Counter { value: i64 }
//!
//! #[derive(Encode, Decode)]
//! enum Cmd { Add(i64), Reset }
//!
//! #[derive(Encode, Decode)]
//! enum Evt { Value(i64) }
//!
//! impl Service for Counter {
//!     type Input = Cmd;
//!     type Output = Evt;
//!     type Snapshot = i64;
//!     type Config = ();
//!     fn genesis(_: &()) -> Self { Counter { value: 0 } }
//!     fn apply(&mut self, _seq: Seq, cmd: &Cmd, ctx: &mut Ctx<'_, Evt>) {
//!         match cmd { Cmd::Add(n) => self.value += n, Cmd::Reset => self.value = 0 }
//!         ctx.emit(Evt::Value(self.value));
//!     }
//!     fn snapshot(&self) -> i64 { self.value }
//!     fn restore(v: i64, _: &()) -> Self { Counter { value: v } }
//! }
//!
//! let dir = std::env::temp_dir().join("ticktape-doc-example");
//! let _ = std::fs::remove_dir_all(&dir);
//! let mut node: Node<Counter> = Node::open(NodeConfig::new(&dir), ()).unwrap();
//! let (seq, _outs) = node.submit(Cmd::Add(2)).unwrap();
//! assert_eq!(seq.as_u64(), 1);
//! assert_eq!(node.service().value, 2);
//! ```

pub use ticktape_core::{
    decode_all, encode_to_vec, CodecError, Ctx, Frame, FrameError, FrameKind, OutBuf, Seq, Service,
    Timestamp,
};
// Trait + derive macro under one name each (serde-style).
pub use ticktape_codec::{Decode, Encode};
pub use ticktape_journal::{FsyncPolicy, Journal, JournalConfig, JournalError, Recovered};
pub use ticktape_runtime::{
    InProcBus, ManualClock, Node, NodeConfig, NodeError, TimeSource, WallClock,
};
