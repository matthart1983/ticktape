# Backlog — known gaps, honestly stated

The M0–M6 spine of the design is implemented and simulation-verified. This
file is the audit of what is *not* done: real gaps a user could hit, in
priority order, each with why it matters and what "done" means. Nothing
here is hidden in the code — there are no `TODO`s or stubs; everything
below works as documented but is incomplete as a product.

## P0 — correctness-adjacent traps (a real user hits these first)

### 1. Events to offline sessions are silently dropped
`gateway::server` routes an `Addressed` event only to live sinks; a client
that reconnects has missed everything in between, and drop-copy observers
cannot backfill. Real session protocols (SoupBinTCP) make the per-session
outbound stream itself sequenced and replayable.
**Done means:** each session's outbound events carry a per-session sequence
number; the gateway retains a bounded per-session outbox (or derives it
from the journal on demand); `Hello { session, from_event_seq }` replays
the gap on reconnect; drop-copy can join from any point. Covered by an e2e
test that kills a client, trades against its book, reconnects, and sees
the missed fills.

### 2. Unbounded growth in three stores
- `transport::MemStore` (retransmit) never evicts — a long-running feed
  leaks its entire history in memory. Fix: journal-backed store (serve
  ranges by reading segments) with the in-memory store as a bounded
  recent-window cache.
- Old snapshot files are never pruned (`purge_after` removes only
  *fenced* ones). Fix: keep the newest N valid snapshots, delete the rest
  at snapshot time.
- Journal segments accumulate forever. Fix: compaction — archive/delete
  segments wholly below the newest durable snapshot (this is the spec's
  §12 note, and it also unlocks true fast recovery: today recovery still
  reads and CRC-verifies every segment even when a snapshot skips the
  re-apply).

**Done means:** a week-long soak of the feed example holds steady RSS and
disk bounded by config; recovery time is bounded by snapshot cadence, not
journal age.

### 3. Gateway flow control cannot actually throttle
`SessionFlow` windows are correct and unit-tested, but the live server
acks synchronously inside `submit`, so `outstanding` never exceeds 1 and
`Throttled` is unreachable. The window becomes real only with deferred
acks (see P1.2). Until then this is a documented no-op — the risk is
someone reading the code and believing backpressure exists.
**Done means:** either deferred-ack mode lands (below), or the server
config states plainly that window > 1 has no effect in synchronous mode.

## P1 — the milestone-sized gap: machinery ⇒ product

### 1. A packaged clustered server (auto-failover, end to end)
Elections, fencing, and quorum commit are proven in the deterministic
cluster simulation — but nothing wires them to live sockets and timers.
Missing pieces, in dependency order:
- a small network protocol for `VoteRequest`/`VoteReply` (reuse the
  length-prefixed wire helpers);
- acceptor `promised` **persistence** (currently a documented embedder
  requirement with no library support — an acceptor that forgets a
  promise can elect two leaders for one epoch). A tiny journaled-value
  helper in `ticktape-cluster` closes it;
- a failure detector (missed-heartbeat count over the existing transport
  heartbeats) driving candidacy;
- standby promotion wiring: reconcile-with-fences (the rule the sim
  forced), adopt the feed publisher role, emit `EpochChange`, gateway
  fail-over of client connections.

**Done means:** `kill -9` the leader of a 3-node deployment of the
exchange example; a standby promotes within the heartbeat timeout; clients
reconnect and resume their sessions; the cluster sim's invariants hold on
the real deployment's journals afterwards.

### 2. Tier 2 in the runtime: deferred acks
`CommitTracker` exists; `Node` doesn't use it. Quorum commit requires
splitting `submit` into sequence/journal (returns pending handle) and a
commit watermark that releases outputs + acks. This is also what makes
gateway windows real (P0.3) and unlocks genuine group-commit batching
(P2.1) — three gaps, one architectural change.
**Done means:** a `Node` mode where outputs/acks release at the commit
watermark, exercised by the cluster sim invariants against the *runtime*
rather than the test harness's own bookkeeping.

## P2 — the named performance workstream (budgets currently missed)

Benchmarked gaps (see README Performance): synced-fsync p99 budget of
15 µs vs measured 200 µs (Linux) / ~4 ms (macOS); 2–3 M frames/s
throughput budget vs 0.8–1.7 M/s.

1. **Async group commit**: batch concurrent ingress into one
   `pwritev` + `fdatasync` per window with deferred acks (depends on
   P1.2). This is the single change that makes the synced budgets
   reachable.
2. **Packet batching** in the transport publisher (fill to
   `MAX_PACKET_BYTES` instead of one frame per packet).
3. **`io_uring` / `O_DIRECT`** journal path (Linux, feature-gated).
4. **Shared-memory IPC ring** for same-box fan-out (the spec's IpcShm).
5. **Streaming replay** — recovery iterates segments instead of
   materializing `Vec<Frame>` (memory ∝ journal size today).
6. Hardware CRC32C (SSE4.2 / ARMv8) behind the same function.

## P3 — polish, tooling, and honesty upkeep

- Simulator gaps: acceptor crash/restart faults (with persisted
  `promised`), directory-entry loss on crash, non-prefix page flushes,
  multi-node transport faults driven through the real `Reassembler`.
- Derive macros: generic type support (removes the hand-written codecs in
  `ticktape-gateway`).
- Test the `UnixDatagram` packet source (implemented, unused by tests).
- Feed example: backfill the retransmit store from the recovered journal
  so late joiners work across leader restarts (today: documented "wipe
  the journal").
- `openraft` delegation backend behind a `raft-backend` feature (spec §9
  open question — build only if someone actually wants it).
- Docs: a short book / teaching deck (spec M6 item, deferred).
- **Decisions pending**: crates.io publish (names unclaimed — cheap
  insurance, do early) and the `v1.0.0` tag (recommend: after P0 and
  P1.1, which is when "use this for something real" stops needing
  caveats).

---

*Rule of the house: when an item ships, move its line to the README/CHANGELOG;
when a new gap is found, it lands here — never silently.*
