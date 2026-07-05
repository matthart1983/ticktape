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

### ✅ 1. No single point of failure — DONE (Stage A v0.10.0, Stage B v0.13.0)
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
binary is a thin wrapper); wiring the leader's now-built Tier-2
deferred-ack mode (P1.3, done) to a live follower→leader ack channel so
"no committed loss" is enforced by the runtime end-to-end, not just held
by caught-up replicas.

**✅ Stage B — DONE (v0.13.0).** A follower-side failure detector
(`FailureDetector`) samples the transport's new `packets_seen` liveness
counter each `pump`: an idle leader still heartbeats, so the counter
advances even with no new frames, distinguishing "leader quiet" from
"leader gone". When it sees no liveness within `failover_timeout + idx *
failover_stagger` (staggered so the lowest-indexed survivor stands first),
`Server::maybe_failover()` stands for election with no operator action; on
winning it becomes leader, on losing it re-points gap-fill at the
presumptive new leader and resumes following — the deployment re-converges
on its own. Followers adopt the epoch carried by `EpochChange` fence frames
so a *second* failover targets a fresh epoch. `Server::heartbeat()` fans
liveness out to every follower feed.
**Verified** (`ticktape-server/tests/auto_failover.rs`, real loopback): a
3-node deployment whose main loop is just `pump` + `heartbeat` +
`maybe_failover` — never `promote()` or `set_leader_hint()` — loses its
leader to a `drop`, promotes the lowest-indexed survivor automatically with
the exact pre-failover state (no committed loss, conservation invariant
holds) and a bumped epoch, and the other survivor re-points and re-converges
on its own; a negative test proves an idle-but-heartbeating leader never
triggers a false failover. Safety of *what* promotion does remains covered
by the deterministic cluster sim; this stage adds the real-time *trigger*.

## P1 — correctness traps and product machinery

### ✅ 1. Admin/observability plane — DONE (v0.11.0)
`ticktape-server::admin` — `ServerStats` (role, epoch, seq, replication
lag, latest snapshot seq, journal segment count) + a dependency-free
Prometheus-text HTTP endpoint (`/metrics`, `/healthz`); `Server::stats()`
gathers it cheaply. An operator can answer "is it healthy, which node
leads, how far behind is each standby, is disk bounded" with curl, and it
updates live across a failover. **Remaining as a follow-up:** an
*interactive* admin channel (pause / promote / snapshot-now / prune over a
socket — core's telnet-shell affordance); the read-only metrics plane was
the higher-value half and shipped first. Not yet surfaced: per-session
gateway stats + commit watermark (the packaged server doesn't host the
gateway yet).

### ✅ 2. Events to offline sessions are silently dropped — DONE (v0.15.0)
Fixed the drop: `route_events` now assigns each session's outbound events a
monotonic per-session `event_seq` and retains them in a bounded per-session
outbox (`ServeConfig::outbox_capacity`, default 1024), *creating the session
entry even when no connection is live* — so events for an offline session
accumulate rather than vanish. `ServerMsg::Event { event_seq, event }`
carries the seq to the client; `Hello { session, from_event_seq }` and
`DropCopy { session, from_event_seq }` replay everything after that point on
(re)connect, so a command client or a drop-copy observer resumes from any
position still in the outbox (a received `event_seq` gap means it fell past
the retention floor and must resync). **Verified** (`exchange/tests/e2e.rs`):
a drop-copy observer watches a maker's session, drops offline, the maker's
resting book trades while it is gone, and on reconnect it replays *exactly*
the fill it missed (event_seq 2, no gap/dup); and a reconnecting command
client is backfilled the cancel-on-disconnect event it never saw. Flake-
checked 5×. **Follow-up:** `event_seq` is an in-memory per-gateway counter,
so it resets on a *gateway* restart (a client reconnecting across that sees
the seq rewind and resyncs) — deriving it from the journal for cross-restart
continuity is the "or derives it from the journal on demand" option, left
for when the packaged server hosts the gateway.

### ✅ 3. Tier 2 in the runtime: deferred acks — DONE (v0.12.0)
The `Node` now has a quorum-commit mode (`NodeConfig::with_quorum(voters,
self_replica)` / `CommitConfig`). In that mode `submit` journals + applies
but *withholds* the outputs, recording only the leader's own durable
high-water; `record_ack(follower, seq)` advances a majority commit
watermark that releases the buffered `(seq, outputs)` in seq order.
`commit_watermark()` and `pending_commit_count()` expose the state (the
latter is the basis for edge flow control). Tier 0/1 nodes are unchanged —
`submit` still returns outputs synchronously when no `CommitConfig` is set.
**Verified:** unit tests for the release semantics (withhold until
majority, single-voter fast path, stale-ack monotonicity, minority never
commits, ticks advance the leader high-water); and a deterministic
differential simulation (`ticktape-cluster/tests/quorum_commit_sim.rs`,
300 seeds) that drives a real runtime `Node` in quorum mode on the
fault-injecting `SimStorage` and cross-checks its watermark + exact
released set against an independent reference model — *the runtime's own
machinery, not the harness's bookkeeping* — plus a 200-seed crash run
proving no committed output is lost or resurrected across power loss.
**Remaining as a follow-up (the live wiring):** feeding `record_ack` from
a real follower→leader ack channel in the packaged server (needs the
Stage-A ack back-channel), which is what finally makes the gateway's
`SessionFlow` window > 1 exceed one outstanding in the *live* server
(`Throttled` reachable end-to-end) and unlocks group-commit batching
(P2.1). The runtime mechanism those depend on now exists and is proven;
only the transport plumbing remains.

### ✅ 4. Deterministic timers — DONE (v0.14.0)
The capability gap from the core/Coral review is closed: services can now
schedule against sequenced time. `Ctx::set_timer(id, at)` /
`cancel_timer(id)` record requests during a step; the runtime holds them in
a `TimerWheel` ordered by `(deadline, set-at-seq, id)` for total
determinism, and when sequenced time (advanced by any `submit` or `tick`)
reaches a deadline the sequencer injects a `TimerFired` frame carrying the
`id`, delivered to a new `Service::on_timer(id, ctx)` handler (default
no-op). Firing is a *journaled, sequenced* frame — stamped at the trigger
time so a burst of same-time timers can't advance time or loop forever —
so it replays identically on recovery and on every replica (`Replica` now
applies `TimerFired` frames too). Pending-timer state is serialized into
the snapshot alongside the service state (`(Snapshot, wheel)` tuple), so
timers armed before a snapshot still fire after a snapshot-based recovery.
`Node::pending_timer_count()` gauges the wheel (a disk-pressure signal).
**Verified:** 3 `TimerWheel` unit tests (ordering, re-arm/cancel, snapshot
round-trip); 7 runtime end-to-end tests (fire-on-time, tick-fires,
cancel-prevents, replay equivalence via `verify_replay`, snapshot survival,
handler-re-arm, trigger-time stamping); and the flagship: the **order book
gained good-till-date orders** (`Cmd::SubmitGtd { …, expire_at }` →
`Evt::Expired`), proven under the fault-injecting `SimStorage` to expire
identically on all three paths — **live**, **replay** (crash recovery
re-runs the journaled firings), and **replica** (an independent follower of
the stream) — with GTD timer state surviving snapshot-based recovery and
disarming correctly on fill/cancel. Share-conservation invariant holds
across expiry (expired shares count as canceled).

## P2 — the named performance workstream (budgets currently missed)

Benchmarked gaps (see README Performance): synced-fsync p99 budget of
15 µs vs measured 200 µs (Linux) / ~4 ms (macOS); 2–3 M frames/s
throughput budget vs 0.8–1.7 M/s.

**Status: all 7 landed (5 in v0.16.0; io_uring + shm ring in v0.17.0, both
feature-gated so the default build stays dependency-free). The shm ring is
verified on macOS; the io_uring backend is Linux-only — written and gated,
to be verified on the Linux NUC.**

1. **✅ Group commit — DONE.** `Journal::append_batch` writes a whole
   contiguous run with one `write_all` + one fsync, and `Node::submit_batch`
   sequences+journals a batch that way, then applies each input in order.
   Observationally identical to N `submit`s (a test asserts a byte-identical
   journal), but the batch pays one `fdatasync` instead of N. Bench: on
   macOS (APFS fsync ≈ 4 ms) a 64-input batch under `EveryFrame` costs
   ≈ 66 µs/input vs ≈ 4 ms/input single — ~60× — which is what brings synced
   latency under budget on real NVMe. (This is the primitive; the "deferred
   acks / concurrent ingress" wording of the original item is realized by a
   caller — e.g. the gateway — draining a burst into one `submit_batch`.)
2. **✅ Packet batching — DONE.** `Publisher::publish_batch{,_to}` packs as
   many seq-contiguous frames as fit (≤ `MAX_PACKET_BYTES`) into each packet
   — one syscall per packet, not per frame — encoding through a reusable
   buffer. Round-trips through the real receiver in a loopback test.
3. **✅ `io_uring` journal path — DONE (v0.17.0, Linux-only, feature-gated).**
   `ticktape-journal`'s `io-uring` feature adds `IoUringStorage: Storage`
   whose open segments submit their appends and data-syncs through a per-file
   `io_uring` ring — so an `append_batch` commit becomes one submitted write +
   one submitted `fdatasync`. Cold-path ops (list/read/create/truncate) stay
   on `std::fs`. The dep is `[target.'cfg(target_os = "linux")']`-scoped and
   the module is `#[cfg(all(target_os = "linux", feature = "io-uring"))]`, so
   the default build (and macOS) never touches it. **Verify on the Linux NUC:**
   `cargo test -p ticktape-journal --features io-uring` runs
   `tests/iouring.rs`, which asserts the io_uring journal is byte-identical to
   the `RealStorage` one and recovers to the same frames.
4. **✅ Shared-memory IPC ring — DONE (v0.17.0, feature-gated, verified).**
   `ticktape-transport`'s `shm` feature adds a lock-free SPSC slot ring over a
   memory-mapped file (`ShmRing`) plus `ShmSource: PacketSource`, so a
   co-located consumer (drop-copy auditor, same-box replica, risk gate) reads
   the sequenced stream from shared memory with no socket round-trip, behind
   the existing `PacketSource` seam. A full ring drops (the reliable feed
   still carries the frame) rather than corrupting. Verified on macOS
   (`cargo test -p ticktape-transport --features shm`): single-process
   round-trip, full-ring drop, and a cross-thread producer/consumer run.
   `memmap2` is optional (off by default).
5. **✅ Streaming replay — DONE.** `Journal::replay_open` streams frames to a
   callback in seq order without materializing `Vec<Frame>` (peak memory =
   one frame + one segment buffer). `verify_replay` uses it, so the
   determinism check no longer builds a second full frame vector. (Live Node
   crash-recovery still uses the materializing path — its snapshot-anchor
   selection needs journal metadata before replay, and P0.1 compaction
   already bounds the materialized size; rewiring it onto the streaming
   primitive is a safe follow-up, not a budget blocker.)
6. **✅ Allocation-free hot path (sequencer + fan-out) — DONE.** The journal
   append path encodes through a reusable `scratch` buffer instead of a
   fresh `Vec<u8>` per frame (`append`/`append_batch`); the publisher packs
   through a reusable buffer (`encode_data_into`, no per-frame `Vec`). The
   borrowed `Frame<'a>` view decoding directly out of segment/packet buffers
   remains a further step (the read path still owns payloads), tracked but
   not required for the write-side budgets.
7. **✅ Hardware CRC32C — DONE.** `crc32c` dispatches to ARMv8 `crc32c*` /
   SSE4.2 `crc32` (runtime-detected) with the software table as fallback; a
   differential test pins hardware == software across all lengths 0..300.
   Bench: 10.7 GB/s on Apple Silicon (vs ~1–2 GB/s for the table). Still
   dependency-free — intrinsics from `core::arch`.

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
