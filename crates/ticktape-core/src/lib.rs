//! Core types for Ticktape, a framework for deterministic, replicated
//! services on the sequencer architecture.
//!
//! This crate is dependency-free and holds everything the rest of the
//! workspace agrees on:
//!
//! - [`Seq`] / [`Timestamp`] — the sequence-number and sequenced-time types.
//! - [`Frame`] / [`FrameKind`] — the framework-owned wire/journal record.
//! - [`Encode`] / [`Decode`] — the canonical-byte codec contract (primitive
//!   impls live here too, because the orphan rule requires it; the
//!   `ticktape-codec` crate re-exports them alongside the derive macros).
//! - [`Service`] / [`Ctx`] — the one trait an application implements.
//!
//! # The determinism contract
//!
//! A [`Service`] is a pure function of its sequenced input stream. Same
//! inputs ⇒ bit-identical state and outputs on every replica and every
//! replay. [`Ctx`] is the only channel to the outside world and every
//! capability it exposes is deterministic: sequenced time, the current
//! sequence number, and output emission. There is deliberately no `spawn`,
//! `sleep`, `rand`, file, or socket access.

pub mod codec;
pub mod crc32c;
pub mod frame;
pub mod seq;
pub mod service;

pub use codec::{decode_all, encode_to_vec, CodecError, Decode, Encode};
pub use frame::{Frame, FrameError, FrameKind, FRAME_HEADER_LEN};
pub use seq::{Seq, Timestamp};
pub use service::{Ctx, OutBuf, Service};
