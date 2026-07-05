# Changelog

All notable changes to Ticktape are documented here. The format is loosely
[Keep a Changelog](https://keepachangelog.com/); this project follows semantic
versioning as of 1.0.0.

## [1.0.0] — 2026-07-05

First stable release. The public API follows semver from here; the wire format
is specified in [WIRE.md](WIRE.md).

### The framework

A deterministic sequencer-architecture framework for durable, replicated
services: a single logical sequencer imposes total order, a segmented journal
is the system of record, and deterministic state machines replay identical
inputs to bit-identical state.

- **Core spine (M0–M6):** the `Service`/`Ctx` contract, canonical fixed-layout
  codec (`#[derive(Encode, Decode)]`, now including generic types), CRC32C-
  checked frames, the journal with snapshots and crash recovery, the runtime
  `Node`, the deterministic simulator (VOPR/DST), reliable A/B UDP transport
  with TCP gap-fill, epoch-lease election + fencing, the TCP gateway, and
  benchmarks against the spec budgets.
- **24×7 operation:** snapshot pruning + journal compaction bound disk; no
  day-roll.
- **No single point of failure:** a packaged leader/follower server with
  **automatic failover** (a standby's failure detector promotes it with no
  operator action), preserving exact pre-failover state.
- **Durability tiers:** Tier 0/1, and Tier 2 quorum commit with no committed
  loss (outputs withheld until a majority has journaled the input).
- **Deterministic timers:** `ctx.set_timer` fires as a journaled `TimerFired`
  frame; the order book uses them for good-till-date orders.
- **Replayable gateway sessions:** per-session `event_seq` + bounded outbox;
  a reconnecting client or drop-copy observer is backfilled exactly what it
  missed.
- **Performance workstream:** group commit (`Node::submit_batch`), hardware
  CRC32C (ARMv8/SSE4.2), packet batching, streaming replay, and a reusable
  encode buffer on the hot paths. Two feature-gated backends: an `io_uring`
  journal `Storage` (`--features io-uring`, Linux) and a shared-memory
  `PacketSource` ring (`--features shm`).
- **Interop & alternatives:** `ticktape-sbe` (SBE-framed payloads over the
  canonical tier) and `ticktape-raft` (delegate log ordering to tikv/raft-rs
  while keeping the deterministic state machine) — both feature-gated.
- **Docs:** [docs/GUIDE.md](docs/GUIDE.md) teaching walkthrough and
  [WIRE.md](WIRE.md) byte-level spec.

### Verification

Dual-platform (macOS arm64 + Linux x86_64, io_uring on a real kernel). Safety
properties are checked under seeded fault injection: crash-recovery determinism,
Tier-2 no-committed-loss across kills/partitions, multi-node acceptor crash
safety, and the application's own invariants — mutation-tested where it counts.
