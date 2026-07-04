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
  leaks its entire history in memory. Fix: split the retransmitter the
  way jimgreco/core splits `MoldRepeater`/`MoldRewinder` — a **repeater**
  serving live gap-fill from a bounded in-memory recent window, and a
  **rewinder** serving historical ranges by reading journal segments.
  (This also fixes the feed example's late-join-across-restart gap, P3.)
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

### 1. A packaged clustered server (end to end; manual promotion first)
Elections, fencing, and quorum commit are proven in the deterministic
cluster simulation — but nothing wires them to live sockets and timers.
**Ship in two stages.** Stage A: **manual promotion** — an operator runs
`promote`, which executes election + fence + reconcile using the machinery
the sim already proved. This is a legitimate production posture with
commercial precedent (CoralSequencer deliberately ships manual primary
failover), and it decouples the server from failure-detector work.
Stage B adds the failure detector for auto-failover.

Missing pieces, in dependency order:
- a small network protocol for `VoteRequest`/`VoteReply` (reuse the
  length-prefixed wire helpers);
- acceptor `promised` **persistence** (currently a documented embedder
  requirement with no library support — an acceptor that forgets a
  promise can elect two leaders for one epoch). A tiny journaled-value
  helper in `ticktape-cluster` closes it;
- a **command channel on the transport** (the jimgreco/core model:
  contributors carry an app id + per-app seq and publish commands on one
  channel; only the active sequencer listens and publishes events on the
  other). Our `SessionFlow` already implements the dedup half — this
  runs it over the bus instead of bespoke per-client TCP, makes N
  contributors (gateways, risk apps, drop-copy writers) uniform, and
  makes the sequencer swappable;
- an **activation-graph-lite** (core's `Activator` pattern: per-component
  ready/started/active states with dependency-ordered start/stop, and
  lifecycle events published into the stream) as the skeleton that wires
  journal + transport + gateway + election into one process;
- Stage B only: a failure detector (missed-heartbeat count over the
  existing transport heartbeats) driving candidacy;
- standby promotion wiring: reconcile-with-fences (the rule the sim
  forced), adopt the feed publisher role, emit `EpochChange`, gateway
  fail-over of client connections.

**Done means (Stage A):** operator-promoted failover of a 3-node exchange
deployment with no committed loss (Tier 2) and clients resuming sessions.
**Done means (Stage B):** `kill -9` the leader; a standby promotes within
the heartbeat timeout; the cluster sim's invariants hold on the real
deployment's journals afterwards.

### 2. Tier 2 in the runtime: deferred acks
`CommitTracker` exists; `Node` doesn't use it. Quorum commit requires
splitting `submit` into sequence/journal (returns pending handle) and a
commit watermark that releases outputs + acks. This is also what makes
gateway windows real (P0.3) and unlocks genuine group-commit batching
(P2.1) — three gaps, one architectural change.
**Done means:** a `Node` mode where outputs/acks release at the commit
watermark, exercised by the cluster sim invariants against the *runtime*
rather than the test harness's own bookkeeping.

### 3. Deterministic timers (capability gap — from the core/Coral review)
Services can read sequenced time but cannot schedule against it: there is
no way to express "cancel this order in 30s", GTD expiry, auction phases,
or deterministic timeouts — bread-and-butter exchange logic. jimgreco/core
treats timers as first-class: the sequencer owns a scheduler and timer
firings are *sequenced messages*, identical on every replica and replay.
**Design:** `ctx.set_timer(id, at)` / `ctx.cancel_timer(id)` emit timer
requests; the sequencer tracks pending deadlines (ordered by
`(at, seq, id)` for total determinism) and injects `TimerFired { id }` as
a sequenced input when sequenced time passes each deadline. Pending-timer
state is part of the snapshot; the simulator fuzzes firing under crashes
for free.
**Done means:** the order book example supports good-till-date orders that
expire identically on live, replay, and replica paths, verified under the
simulator; timer state survives snapshot-based recovery.

### 4. Admin/observability plane
ticktape components expose nothing — not even counters. Both reviewed
systems treat operability as a product feature (core: command shell with
`@Command`/`@Property` introspection over telnet/HTTP + metrics; Coral:
monitoring tooling). Minimal version: a stats struct per component
(sequencer seq + commit watermark, journal bytes + segment count, snapshot
age, session count + per-session seqs, retransmit window depth, gap-fill
counts) surfaced by the packaged server on one plain-text/HTTP endpoint.
**Done means:** an operator can answer "is it healthy, how far behind is
the standby, when was the last snapshot" with curl, no debugger.

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
6. **Allocation-free hot path** — jimgreco/core is obsessively
   zero-allocation (Agrona buffers, garbage-free collections); we allocate
   per frame (`Vec<u8>` payloads, `encode_to_vec` per message, clones per
   fan-out sink). Rust has no GC but allocation still caps throughput:
   introduce buffer reuse/arenas on the sequencer path and a borrowed
   `Frame<'a>` view type decoding directly out of segment/packet buffers.
7. Hardware CRC32C (SSE4.2 / ARMv8) behind the same function.

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
- **`WIRE.md`** — a standalone wire-format spec (frame, segment, snapshot,
  packet, retransmit-request layouts + CRC rules, currently documented
  only in Rust doc-comments). The wire is already language-neutral;
  Coral's C++-node interop shows the value of making that a documented
  feature so non-Rust nodes can join the stream.
- **Schema-version handshake** — core publishes application/schema
  definitions into the stream, making it self-describing. Cheap ticktape
  version: a schema-version field in the stream's first frame (and in
  `EpochChange`), so a replica rejects a mismatched `Input` schema
  explicitly instead of failing on decode.
- Docs: a short book / teaching deck (spec M6 item, deferred).
- **Decisions pending**: crates.io publish (names unclaimed — cheap
  insurance, do early) and the `v1.0.0` tag (recommend: after P0 and
  P1.1, which is when "use this for something real" stops needing
  caveats).

---

*2026-07-05: P0.2's repeater/rewinder design, P1.1's staging + command
channel + activation graph, P1.3 (timers), P1.4 (admin plane), P2.6
(zero-alloc), and the `WIRE.md`/schema-handshake items come from a
comparative review of jimgreco/core and CoralSequencer — two production
sequencer platforms whose deltas against ticktape were: timers, operability,
bus-level command ingress, and app-lifecycle activation. Nothing in either
system contradicted the architecture.*

*Rule of the house: when an item ships, move its line to the README/CHANGELOG;
when a new gap is found, it lands here — never silently.*
