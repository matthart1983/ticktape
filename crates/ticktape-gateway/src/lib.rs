//! The gateway (M5): where the deterministic core meets the messy outside
//! world. The gateway absorbs all edge nondeterminism — retries, network
//! flapping, client crashes — so the state machine only ever sees a clean,
//! sequenced input stream (spec §13).
//!
//! - **Sessions + dedup.** Each client session numbers its own commands
//!   with a monotonic `client_seq`. The gateway admits exactly the next
//!   expected number: duplicates (retries after a lost ack) are dropped,
//!   gaps are protocol errors. Result: exactly-once *effect* despite
//!   at-least-once delivery. The envelope ([`GatewayInput::Client`])
//!   carries `(session, client_seq)` into the journal, so dedup state is
//!   itself deterministic, journaled, and replicated — a restarted gateway
//!   reseeds its expectations from the recovered service.
//! - **Flow control.** A bounded per-session window of unacknowledged
//!   commands (the Island discipline at window = 1) ties client throughput
//!   to engine latency; a throttled command is refused without consuming
//!   the client seq, so the client simply retries it.
//! - **Cancel-on-disconnect.** A dropped connection injects
//!   [`GatewayInput::SessionClosed`] as a *sequenced input*, so the service
//!   reacts deterministically (e.g. pulls the session's resting orders) —
//!   identically on every replica and every replay.
//! - **Drop-copy.** Any number of independent observers may subscribe to a
//!   session's outcomes; they receive the same [`Addressed`] events the
//!   client does, derived from the same sequenced stream.
//!
//! The application opts in by shaping its `Service` around the envelopes:
//! `Input = GatewayInput<YourCmd>`, `Output = Addressed<YourEvt>` — the
//! service addresses each event to the session that must see it (an
//! exchange addresses a trade to both taker and maker). Everything else —
//! matching, ownership, cancel-on-disconnect behavior — is ordinary
//! deterministic service logic, which means the whole session layer is
//! exercised by the M1 simulator like any other input.

pub mod flow;
pub mod server;
pub mod wire;

pub use flow::{Admit, SessionFlow};
pub use server::{serve, ServeConfig};
pub use wire::{read_msg, write_msg, Addressed, ClientMsg, GatewayInput, RejectReason, ServerMsg};
