# Backlog — known gaps, honestly stated

The M0–M6 spine of the design is implemented and simulation-verified. This
file is the audit of what is *not* done: real gaps a user could hit, in
priority order, each with why it matters and what "done" means. Nothing
here is hidden in the code — there are no `TODO`s or stubs; everything
below works as documented but is incomplete as a product.

**Priority order (revised 2026-07-05):** ranked by practitioner feedback
from a builder of a production core-derived platform (SBE/Aeron stack).
Their two disqualifiers for platforms in this space — *can it run
24×7/365 without filling a disk* and *is every component redundant* — are
P0. The first (24×7 operation) is **done as of v0.8.0**; the packaged
no-SPOF replicated deployment is now the top open item.

## P0 — platform viability (the practitioner's disqualifiers)

### ✅ 1. 24×7/365 continuous operation — DONE (v0.8.0)
Journal compaction (`compact_below`, `reseat_to`) + snapshot pruning
(`retain_snapshots`, default 2) bound disk; the transport's
`MoldRepeater`/`MoldRewinder`-style split — a bounded in-memory `MemStore`
repeater + a journal-backed `JournalRewinder`, composed by `ChainStore` —
bounds retransmit memory; recovery anchors on a snapshot when history is
compacted away (and reseats the journal when a synced snapshot outlives
its unsynced tail). All fault-injection-verified: compaction/prune/reseat
fire on nearly every VOPR seed, the sim's invariants are compaction-aware,
and the feed example ran to seq 1,400 on one segment + two snapshots with a
late joiner backfilling from disk. **A node now runs 24×7/365 with no
restart or day-roll.**

### ◑ 1. No single point of failure — Stage A DONE (v0.10.0), Stage B open
"The sequencer needs several running at the same time; the journal also
needs several running, replicated, so that all the single points of
failure are removed." In this architecture a *replicated journal* is not
a separate subsystem — every replica journals the stream it applies, and
Tier 2 commits only what a majority has durably journaled, so journal
redundancy falls out of deterministic replay; the cluster sim already
proves no-committed-loss across leader kills and partitions. What's
missing is the **packaged deployment** that makes it real on live sockets.

**Ship in two stages.** Stage A: **manual promotion** — an operator runs
`promote`, which executes election + fence + reconcile using the machinery
the sim already proved. This is a legitimate production posture with
commercial precedent (CoralSequencer deliberately ships manual primary
failover), and it decouples the server from failure-detector work.
Stage B adds the failure detector for auto-failover.

Missing pieces, in dependency order:
- ~~a small network protocol for `VoteRequest`/`VoteReply`~~ — **done
  (v0.9.0)**: `ticktape-cluster::net` (CRC'd TCP votes, `AcceptorServer`,
  `run_election`), tested over loopback;
- ~~acceptor `promised` **persistence**~~ — **done (v0.9.0)**:
  `PersistentAcceptor` fsyncs `promised` before every grant; corrupt
  record is a hard error, not a silent reset; 300-seed crash-injection
  test proves no epoch is granted twice across restart;
- a **command channel on the transport** (the jimgreco/core model:
  contributors carry an app id + per-app seq and publish commands on one
  channel; only the active sequencer listens and publishes events on the
  other). Our `SessionFlow` already implements the dedup half — this
  runs it over the bus instead of bespoke per-client TCP, makes N
  contributors (gateways, risk apps, drop-copy writers) uniform, and
  makes the sequencer swappable;
- an **activation-graph-lite** (core's `Activator` pattern: per-component
  ready/started/active states with dependency-ordered start/stop, and
  lifecycle events published into the stream — core runs every app as an
  activatable master/replica pair; that's the model);
- **auxiliary services redundant the same way**: any replica can serve
  repeater/rewinder gap-fill from its own journal, and snapshots are
  taken on replicas (spec §12), so no component of a deployment is
  unique;
- Stage B only: a failure detector (missed-heartbeat count over the
  existing transport heartbeats) driving candidacy;
- standby promotion wiring: reconcile-with-fences (the rule the sim
  forced), adopt the feed publisher role, emit `EpochChange`, gateway
  fail-over of client connections.

**Stage A — DONE (v0.10.0).** `ticktape-server::Server` (Leader/Follower
over real UDP feed + TCP votes/retransmit) with `promote()` = election +
reconcile + open Node on the local journal + fence + become leader.
Verified: a 3-node Bank deployment on loopback fails over on operator
promotion with the promoted leader holding the exact pre-failover state
(no committed loss) and the surviving follower tracking it; promotion
without a majority is refused. Acceptor `promised` is durable (v0.9.0).
Deferred within Stage A: a caught-up-shortfall winner fetching the gap
from a peer before leading (test keeps followers caught up); a packaged
multi-process binary + gateway wiring (the library composes; the demo
binary is a thin wrapper); full Tier-2 deferred-ack enforcement in the
runtime (P1.3) so "no committed loss" is enforced by the runtime, not
just held by caught-up replicas.

**Stage B — open.** A failure detector (missed-heartbeat count over the
existing transport heartbeats) that maintains `leader_hint` and triggers
promotion automatically. **Done means:** `kill -9` the leader; a standby
promotes within the heartbeat timeout with no operator action; the
cluster sim's invariants hold on the real deployment's journals.

## P1 — correctness traps and product machinery

### 1. Admin/observability plane
ticktape components expose nothing — not even counters. Both reviewed
systems treat operability as a product feature, and the practitioner
called it out specifically: core lets you telnet into any service node
for an interactive shell — inspect state, submit commands, JMX-style —
plus a web console. Minimal version first: a stats struct per component
(sequencer seq + commit watermark, journal bytes + segment count, snapshot
age, session count + per-session seqs, retransmit window depth, gap-fill
counts) surfaced by the packaged server on one plain-text/HTTP endpoint.
An interactive admin channel (pause / promote / snapshot-now / prune) is
the natural second step and shares plumbing with the no-SPOF server's Stage-A manual
promotion.
**Done means:** an operator can answer "is it healthy, how far behind is
the standby, when was the last snapshot" with curl, no debugger.

### 2. Events to offline sessions are silently dropped
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

### 3. Tier 2 in the runtime: deferred acks
`CommitTracker` exists; `Node` doesn't use it. Quorum commit requires
splitting `submit` into sequence/journal (returns pending handle) and a
commit watermark that releases outputs + acks. This is also what makes
gateway flow-control windows real (today `SessionFlow` windows are
correct and unit-tested, but the live server acks synchronously inside
`submit`, so `outstanding` never exceeds 1 and `Throttled` is unreachable
— a documented no-op until this lands) and unlocks genuine group-commit
batching (P2.1) — three gaps, one architectural change.
**Done means:** a `Node` mode where outputs/acks release at the commit
watermark, exercised by the cluster sim invariants against the *runtime*
rather than the test harness's own bookkeeping. Until then, the server
config states plainly that window > 1 has no effect in synchronous mode.

### 4. Deterministic timers (capability gap — from the core/Coral review)
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

## P2 — the named performance workstream (budgets currently missed)

Benchmarked gaps (see README Performance): synced-fsync p99 budget of
15 µs vs measured 200 µs (Linux) / ~4 ms (macOS); 2–3 M frames/s
throughput budget vs 0.8–1.7 M/s.

1. **Async group commit**: batch concurrent ingress into one
   `pwritev` + `fdatasync` per window with deferred acks (depends on
   P1.3). This is the single change that makes the synced budgets
   reachable.
2. **Packet batching** in the transport publisher (fill to
   `MAX_PACKET_BYTES` instead of one frame per packet).
3. **`io_uring` / `O_DIRECT`** journal path (Linux, feature-gated).
4. **Shared-memory IPC ring** for same-box fan-out (the spec's IpcShm).
5. **Streaming replay** — recovery iterates segments instead of
   materializing `Vec<Frame>` (memory ∝ journal size today; P0.1
   compaction bounds the size, this removes the materialization).
6. **Allocation-free hot path** — jimgreco/core is obsessively
   zero-allocation (Agrona buffers, garbage-free collections); we allocate
   per frame (`Vec<u8>` payloads, `encode_to_vec` per message, clones per
   fan-out sink). Rust has no GC but allocation still caps throughput:
   introduce buffer reuse/arenas on the sequencer path and a borrowed
   `Frame<'a>` view type decoding directly out of segment/packet buffers.
7. Hardware CRC32C (SSE4.2 / ARMv8) behind the same function.

## P3 — polish, tooling, and honesty upkeep

- **SBE codec adapter** (spec §6 tier 3) — practitioner platforms in this
  space serialize with SBE; an adapter makes ticktape services interoperable
  with that ecosystem without abandoning the canonical `fixed` tier.
- **Transport is deliberately swappable** — practitioner consensus is that
  the sequencer needs only plain UDP multicast or TCP (Aeron is "just
  reliable A→B transport"), which is what M3 built. Keep `PacketSource`/
  publisher seams clean so an Aeron-backed (or QUIC-backed) transport could
  slot in for users who want it; owning the *journal*, by contrast, stays a
  non-negotiable — an outsourced archive (Aeron Archiver) cannot be
  fault-injected deterministically inside the simulator, and DST is the
  moat.
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
- Simulator gaps: acceptor crash/restart faults (with persisted
  `promised`), directory-entry loss on crash, non-prefix page flushes,
  multi-node transport faults driven through the real `Reassembler`.
- Derive macros: generic type support (removes the hand-written codecs in
  `ticktape-gateway`).
- Test the `UnixDatagram` packet source (implemented, unused by tests).
- ~~Feed example: backfill late joiners from the journal across restarts~~
  — **done in v0.8.0** (the feed leader runs on a repeater/rewinder chain).
- `openraft` delegation backend behind a `raft-backend` feature (spec §9
  open question — build only if someone actually wants it).
- Docs: a short book / teaching deck (spec M6 item, deferred).
- **Decisions pending**: crates.io publish (names unclaimed — cheap
  insurance, do early) and the `v1.0.0` tag (recommend: after P0, which
  is when "use this for something real" stops needing caveats).

---

*2026-07-05 (revision 2): priorities re-ranked around direct feedback from
a practitioner who built a production platform on the jimgreco/core model
(SBE serialization, Aeron transport, Aeron Archiver journal). Their
disqualifiers — no 24×7/365 operation without snapshot+prune, and any
remaining single point of failure — are now P0.1 and P0.2. Their
operability bar (telnet shell into any node, web console, master/replica
activation pairs) drives P1.1 and the activation-graph item. Earlier
provenance: the 2026-07-05 comparative review of jimgreco/core and
CoralSequencer contributed the timers, repeater/rewinder, command-channel,
zero-alloc, WIRE.md, and schema-handshake items. Nothing in either system
or the practitioner feedback contradicted the architecture.*

*Rule of the house: when an item ships, move its line to the README/CHANGELOG;
when a new gap is found, it lands here — never silently.*
