# ticktape

[![CI](https://github.com/matthart1983/ticktape/actions/workflows/ci.yml/badge.svg)](https://github.com/matthart1983/ticktape/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
![Rust: stable](https://img.shields.io/badge/rust-stable-orange.svg)

**A Rust framework for deterministic, replicated services on the sequencer
architecture** — the pattern behind Island/INET, Nasdaq, Jane Street, and
LMAX — **with deterministic simulation testing built into the runtime.**

You write a `Service`: a pure `(state, input) → outputs` step function.
ticktape provides total ordering, durable journaling, replay recovery, and a
seeded fault-injecting simulator that hammers *your* state machine with
crashes and torn writes while checking durability, ordering, determinism,
and your own invariants. A reliable sequenced UDP transport (A/B feeds +
gap-fill) streams the total order to follower replicas that compute
bit-identical state, the failover machinery — epoch-lease elections,
fencing, quorum commit — is verified under a multi-node deterministic
simulation with leader kills, partitions, and dueling candidates, and a
TCP gateway puts real clients in front of it all: session dedup
(exactly-once effect despite retries), flow control, cancel-on-disconnect,
and drop-copy.

```rust
use ticktape::{Ctx, Decode, Encode, Node, NodeConfig, Seq, Service};

struct Counter { value: i64 }

#[derive(Encode, Decode)]
enum Cmd { Add(i64), Reset }

#[derive(Encode, Decode)]
enum Evt { Value(i64) }

impl Service for Counter {
    type Input = Cmd;
    type Output = Evt;
    type Snapshot = i64;
    type Config = ();

    fn genesis(_: &()) -> Self { Counter { value: 0 } }

    fn apply(&mut self, _seq: Seq, cmd: &Cmd, ctx: &mut Ctx<'_, Evt>) {
        match cmd { Cmd::Add(n) => self.value += n, Cmd::Reset => self.value = 0 }
        ctx.emit(Evt::Value(self.value));
    }

    fn snapshot(&self) -> i64 { self.value }
    fn restore(v: i64, _: &()) -> Self { Counter { value: v } }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node: Node<Counter> = Node::open(NodeConfig::new("journal"), ())?;
    let (seq, outputs) = node.submit(Cmd::Add(42))?;
    println!("applied at seq {seq}: {outputs:?}");
    Ok(())
}
```

That's the entire application. No networking code, no ordering code, no
durability code, no recovery code — and the same struct, unmodified, runs
inside the fault-injecting simulator today and is designed to run replicated
with hot standby in later milestones.

---

## The idea in sixty seconds

A single logical **sequencer** imposes a total order on all inputs, stamping
each with a gapless `u64` sequence number, and appends them to a durable
**journal**. That ordered stream is the system of record. Deterministic
state machines — your application — consume the identical stream and compute
**bit-identical state**, which makes the hard things fall out of the design
instead of being bolted on:

- **Recovery** is `genesis + replay(journal)`. There is no separate
  "recovery code path" to get wrong — it's the same `apply` that ran live.
- **Replication** (roadmap) is shipping the ordered *inputs*, not mutated
  state; every replica computes the same bytes.
- **Debugging** is replaying the journal. Any past state is reproducible
  exactly.
- **Testing** is running the whole system on virtual time and a seeded RNG,
  injecting faults, and asserting replicas/replays never diverge.

```text
                  unsequenced inputs                     sequenced stream (seq-stamped)
 ┌──────────┐  (client commands)     ┌────────────────┐
 │ clients  │ ──────────────────────▶│   SEQUENCER    │────────┬─────────────┬──────────┐
 └──────────┘                        │  - assigns seq │        ▼             ▼          ▼
      ▲                              │  - appends to  │  ┌───────────┐ ┌──────────┐ ┌────────┐
      │        per-client responses  │    JOURNAL     │  │  standby  │ │ audit /  │ │  ...   │
      └──────────────────────────────│  - runs the    │  │ (replay)  │ │ drop-copy│ │        │
                                     │    SERVICE     │  └───────────┘ └──────────┘ └────────┘
                                     └───────┬────────┘
                                             ▼
                                        ┌─────────┐     every box is "just another
                                        │ JOURNAL │     deterministic consumer of
                                        └─────────┘     the sequenced stream"
```

Two rules make it work, and the framework enforces both:

1. **Determinism.** `apply` is a pure function of `(state, input, ctx)`.
   Same inputs ⇒ bit-identical state and outputs, on every replica, every
   replay, every machine.
2. **Time is data.** Wall-clock time enters the system only as sequenced
   timestamps assigned by the sequencer. A service reads `ctx.now()` —
   there is no API to reach the OS clock from inside `apply`.

## Try it

```sh
git clone https://github.com/matthart1983/ticktape
cd ticktape
cargo test --workspace     # everything, incl. seeded fault-injection fuzz
```

Every example invocation is a cold start that rebuilds state by replaying
its journal — so "crash recovery" is just… running it again:

```text
$ cargo run -p counter -- add 5
recovered: value=0 at seq 0 (replayed from ./counter-journal)
applied at seq 1: [Value(5)]

$ cargo run -p counter -- add 37
recovered: value=5 at seq 1 (replayed from ./counter-journal)
applied at seq 2: [Value(42)]

$ cargo run -p counter -- show
recovered: value=42 at seq 2 (replayed from ./counter-journal)
```

The `kv` example is a durable key-value store in ~90 lines of service code:

```text
$ cargo run -p kv -- put name ada
$ cargo run -p kv -- get name
seq 2: [Value(Some("ada"))]
$ cargo run -p kv -- verify        # replay-equivalence self-check
replay equivalence: OK
```

The flagship example is a **price-time-priority limit order book** —
journaled, snapshotted, and fuzzed under fault injection with
exchange-grade invariants (never a crossed book; every accepted share is
exactly one of traded, canceled, or resting):

```text
$ cargo run -p orderbook -- sell 100 102
$ cargo run -p orderbook -- sell 50 101
$ cargo run -p orderbook -- buy 120 102
seq 3 (order id 3):
  Accepted { id: 3 }
  Trade { taker: 3, maker: 2, price: 101, qty: 50 }   # best price first,
  Trade { taker: 3, maker: 1, price: 102, qty: 70 }   # at the maker's price
$ cargo run -p orderbook -- book
  BID qty@px | ASK qty@px
             | 30@102
$ cargo run -p orderbook -- verify
invariants: OK · replay equivalence: OK
```

The **exchange demo** puts the order book behind the gateway — run
`exchange serve`, connect interactive `exchange client` sessions and an
`exchange watch` drop-copy observer, then kill a client mid-session and
watch its resting orders get pulled deterministically.

And the **multi-process feed demo**: a leader publishes its sequenced stream
over UDP; followers in other processes replicate bit-identical state live,
gap-filling anything lost from the retransmitter:

```text
# terminal 1
$ cargo run -p feed -- sub --bind 127.0.0.1:7101 --retx 127.0.0.1:7110
follower: seq 100 · balances [2145, 2418, ...] · invariants OK

# terminal 2
$ cargo run -p feed -- pub --to 127.0.0.1:7101 --retx-port 7110
leader: seq 100
```

## Deterministic simulation testing

The reason to build on a framework like this is trusting it under failure —
so the simulator is a first-class deliverable, not an afterthought. Storage
I/O and time sit behind traits; `ticktape-sim` swaps in an in-memory disk
and a virtual clock, and runs an entire node — journal, crashes, recovery —
in one thread, on one seed. **Same seed ⇒ same run, exactly. A failing seed
is the reproduction.**

One simulated run drives your service with a seeded workload while
interleaving journal syncs, sequenced ticks, and **power-loss crashes** in
which each file keeps only a seeded prefix of its unsynced bytes — possibly
with a bit-flipped torn sector — and all pre-crash file handles are fenced.
After every crash + recovery, the harness checks:

1. **Recovery succeeds** — a crash may lose the unsynced tail, never the
   ability to restart.
2. **Durability** — every frame synced before the crash is still there.
3. **Total order** — surviving frames are a byte-exact, gapless prefix of
   what was submitted.
4. **Determinism** — recovered state byte-matches an independent
   `genesis + replay` of the surviving frames. With snapshots enabled the
   node recovers via `restore(snapshot) + replay(tail)`, so this check also
   proves your `restore` is exact — and snapshot files take crash faults
   like any other file (torn snapshots must fall back, never lie).
5. **Your invariants** — whatever must always hold about *your* state.

Wiring a service in is one trait and one closure:

```rust,ignore
use ticktape_sim::{vopr, InvariantViolation, Invariants, Rng, SimConfig};

impl Invariants for Bank {
    fn check(&self) -> Result<(), InvariantViolation> {
        if self.balances.iter().sum::<i64>() != TOTAL {
            return Err(InvariantViolation::new("money not conserved"));
        }
        Ok(())
    }
}

// Fuzz 1000 seeds; on failure, shrink to the failing step and verify the repro.
vopr::<Bank>(&SimConfig::new(0), 0..1000, (), gen_transfer)?;
```

A failure report is a complete, deterministic reproduction recipe:

```text
seed 217 failed at step 143: negative balance: -58 (reproduce with seed=217 steps=144)
```

The harness's own acceptance tests prove it catches real bug classes: a
service that reads a process-global inside `apply` (ambient state → replay
divergence), a bank that allows overdrafts (invariant violation, shrunk and
reproduced), an off-by-one `restore` (caught only when recovery goes
through a snapshot — and provably clean with snapshots off), and bit rot
injected into synced journal bytes (must always be a loud error or a
validated truncation — never silently wrong state). It catches real bugs,
not just planted ones: seed 0 of the snapshot milestone's first fuzz run
found a stale-snapshot-poisoning bug (a snapshot outliving a journal
truncation described a history that no longer existed), which is why
recovery now purges snapshots past the surviving tail. There is also a
standalone fuzzer:

```sh
cargo run --release -p ticktape-sim --bin vopr                  # fuzz forever
cargo run --release -p ticktape-sim --bin vopr -- --runs 1000
cargo run --release -p ticktape-sim --bin vopr -- --seed 42     # reproduce one seed
```

CI runs 2000 fresh seeds on every push.

## What's in the box

| Crate | What it is |
|---|---|
| `ticktape` | Facade re-exporting the common surface. Start here. |
| `ticktape-core` | `Frame` (CRC32C-checked wire/journal record), `Seq`, sequenced `Timestamp`, the `Service`/`Ctx` contract, canonical `Encode`/`Decode` traits. Dependency-free. |
| `ticktape-codec` + `ticktape-macros` | The "fixed" codec: `#[derive(Encode, Decode)]` producing little-endian, declaration-order, canonical bytes. |
| `ticktape-journal` | Segmented append-only log: per-frame CRCs, fsync policy (per-frame / time-window group commit / never), torn-tail detection + truncation, gapless-seq validation. Plus the CRC-checked snapshot store (stale snapshots purged on recovery). All I/O behind a `Storage` trait. |
| `ticktape-runtime` | Single-node `Node`: sequence → journal → apply, crash recovery from `snapshot + replay(tail)` with fallback to full replay, cadence snapshotting + `SnapshotMark` frames, sequenced tick time, monotonic-clamped timestamps, in-proc stream fan-out, `verify_replay()`. |
| `ticktape-sim` | The deterministic simulator: seeded RNG (SplitMix64, no `rand` dependency — archived seeds must never rot), simulated disk with crash semantics, virtual clock, `Invariants`, the VOPR loop, and the `vopr` binary. |
| `ticktape-transport` | Reliable sequenced-stream transport, MoldUDP64/SoupBinTCP-shaped: A/B UDP feed redundancy, heartbeat high-water marks, gap detection by seq, unicast TCP range retransmission, late-join catch-up, and `Replica` — a follower that recomputes the service from the ordered stream. The reliability core (`Reassembler`) is a pure state machine, fuzzed across 200 seeds of loss/duplication/reordering. |
| `ticktape-cluster` | The failover machinery as pure state machines: epoch-lease election (Paxos-phase-1-shaped — provably at most one leader per epoch), the `EpochChange` fence, and Tier 2 quorum-commit tracking. Verified in a multi-node deterministic simulation: leader kills, partitions, zombie leaders, dueling candidates, lagging replicas — asserting split-brain safety, Tier 2 no-committed-loss, Tier 1 bounded loss, and bit-identical convergence. A negative test proves disabled fencing is *detected*. |
| `ticktape-gateway` | The edge: per-session monotonic-seq dedup (exactly-once effect under retries), windowed flow control (window 1 = the classic single-outstanding discipline), gap rejection, cancel-on-disconnect injected as a *sequenced input*, drop-copy observers, and a threaded TCP server hosting any session-aware `Service`. Session envelopes go through the journal, so dedup state is deterministic, replicated, and survives restarts. |
| `examples/counter`, `examples/kv`, `examples/orderbook`, `examples/feed`, `examples/exchange` | The hello world; the smallest real service; the flagship price-time-priority CLOB; the multi-process leader/follower feed demo; and the exchange — the order book behind the TCP gateway, driven by real clients in `cargo test` and fuzzed with session traffic under fault injection. |

## How determinism is enforced (not hoped for)

- **`Ctx` is the only door.** Inside `apply` you can read `ctx.now()`
  (sequenced time), `ctx.seq()`, and call `ctx.emit(...)`. There is
  deliberately no spawn, no sleep, no randomness, no file, no socket.
  External interactions use the split-phase pattern: emit a request event;
  the response re-enters later as a sequenced input.
- **The codec rejects nondeterminism at compile time.** `HashMap`/`HashSet`
  (iteration order) and bare floats (NaN, `-0.0`) have no `Encode`/`Decode`
  impls, so they cannot appear in inputs or snapshots. Use `BTreeMap` and
  integer/fixed-point numerics.
- **Replay equivalence is continuously tested.** `Node::verify_replay()`
  re-runs `genesis + replay(journal)` and byte-compares snapshots; the
  simulator performs the same check after every simulated crash.
- **Timestamps are monotonically clamped** at the sequencer, so a stepping
  wall clock (NTP, VM migration) can never make sequenced time run
  backwards.

<details>
<summary><b>On-disk format</b> (click to expand)</summary>

Every sequenced record is a `Frame` — fixed little-endian header, opaque
app-encoded payload, CRC32C over each:

```text
 offset size field
   0     8   seq           u64  monotonic global sequence number
   8     8   timestamp     u64  sequencer-assigned nanos (the ONLY time source)
  16     2   stream_id     u16  logical stream/topic
  18     2   kind          u16  Input | Output | Tick | SessionOpen/Close |
                                SnapshotMark | EpochChange | Heartbeat
  20     4   payload_len   u32
  24     4   header_crc    u32  CRC32C of bytes [0,24)
  28   ...   payload            app-encoded Input/Output
  ..     4   payload_crc   u32  CRC32C of payload
```

The journal is a directory of segments named by the first seq they contain
(`00000000000000000001.seg`), each starting with a CRC'd 28-byte header
(magic `TKTJ`, format version, first seq, epoch). Only *inputs* are
journaled — outputs are deterministically recomputable (the LMAX
discipline). On recovery, a torn tail in the final segment is truncated to
the last intact frame; corruption anywhere else is a loud error, never
silently-wrong data. The frame layout is framework-owned and stable, so app
schema evolution never touches framework wire stability.

</details>

## Status & roadmap

**Today (M0–M6):** a usable, durable, deterministic-service library with
snapshot-accelerated recovery, a fused simulation-testing harness, a
reliable sequenced transport feeding cross-process follower replicas,
simulation-verified failover machinery (elections, fencing, quorum
commit), a TCP gateway with sessions, dedup, flow control,
cancel-on-disconnect, and drop-copy, and benchmarks in CI against the
spec's budgets. **The spine of the spec is walked.** Still open, and named
rather than hidden: the async group-commit/`io_uring` performance
workstream (see Performance), a packaged clustered-server binary wiring
cluster + transport + gateway to live timers end-to-end, journal
compaction, the shared-memory IPC ring, the `openraft` delegation backend,
acceptor-crash faults in the simulator. The API will still move.

| Milestone | Scope | Status |
|---|---|---|
| M0 — Core + single node | `Service`/`Ctx`, codec + derives, segmented journal, replay recovery | ✅ |
| M1 — Determinism harness | `ticktape-sim`: seeded storage faults, invariant checks, VOPR loop + shrinking | ✅ |
| M2 — Snapshotting + flagship example | Snapshot store, `SnapshotMark`, fast recovery; the order book with no-crossed-book / share-conservation invariants under simulation | ✅ |
| M3 — Transport | Reliable sequenced UDP (MoldUDP64-style A/B feeds), TCP gap-fill retransmitter, follower `Replica`; shm IPC ring deferred to the perf pass | ✅ |
| M4 — Replication + failover | Epoch-lease elections + fencing (Tier 1, the classic exchange mode) and VSR-shaped quorum commit (Tier 2); leader kills, partitions, and dueling candidates in the simulator | ✅ |
| M5 — Gateways | Client sessions: dedup, flow control, cancel-on-disconnect, drop-copy; external clients drive the order book end-to-end over TCP | ✅ |
| M6 — Hardening | Benchmarks in CI against the spec budgets; compute paths beat budget, fsync paths measured honestly (async group-commit + `io_uring` named as the follow-on perf workstream) | ✅ |

## Performance

The spec sets design budgets; `cargo run --release -p ticktape-bench`
measures against them (CI runs it report-only — runner hardware is too
noisy to gate). Apple-silicon laptop / Linux CI runner:

| Path | Measured (macOS / Linux) | Budget | |
|---|---|---|---|
| `apply` step (Bank service) | 24 / 21 ns/op | < 200 ns | ✅ |
| submit, `fsync=never` | p50 1.1 µs / **535 ns** · p99 3.0 / 1.8 µs | p50 < 1 µs · p99 < 5 µs | ✅ on Linux |
| submit, group-commit 50 µs | p50 ≈ 4 ms / **1.0 µs** · p99 — / 278 µs | p50 < 1 µs · p99 < 5 µs | p50 ✅ on Linux |
| submit, fsync every frame | p50 ≈ 4 ms / 200 µs | p99 < 15 µs (NVMe) | ❌ see below |
| cold recovery (read + replay) | 12.4 / 7.6 M frames/s | < 1 s / day of data | ✅ |
| reassembler (transport core) | 28 / 21.5 M frames/s | supports < 2 µs fan-out | ✅ |
| simulator speed | ~45,000× / ~25,000× wall-clock | ≥ 1000× | ✅ |

The synced fsync tails are the honest gap, and they're architectural, not
mysterious: a synchronous single-caller `submit` pays the full fsync
whenever a window closes, so time-window group commit degenerates toward
fsync-per-frame under serial load — milliseconds on macOS barrier-fsync,
hundreds of µs on the CI runner's disk. Hitting the sub-15 µs synced
budget requires the real exchange design: batched group commit across
*concurrent* ingress with deferred acks, on NVMe with
`io_uring`/`O_DIRECT`. That pipeline (with packet batching on the
transport, which the same single-frame-per-packet simplification caps at
~0.8 M frames/s vs the 2–3 M/s budget) is the named performance workstream
— until it lands, run `fsync=never` + Tier 1/2 replication for durability,
which is the historical exchange configuration anyway.

## Relation to prior art

This pattern is proven and the ecosystem is crowded; ticktape is an
integration-and-rigor play, not a new-primitive play.

- **[Aeron Cluster]** is the closest incumbent — and is JVM-centric, uses
  full Raft for the log, and has no fused deterministic-simulation harness.
  ticktape is native Rust, MIT/Apache, single-sequencer with explicit
  durability tiers, and ships the simulator in the box.
- **[LMAX Disruptor]** is an intra-process ring buffer — a building block,
  not a durable/replicated system. The vocabulary (and the split-phase
  discipline) comes from there.
- **[TigerBeetle]** sets the bar for simulation rigor (VOPR) — as a fixed
  double-entry-accounting state machine in Zig. ticktape aims that rigor at
  *arbitrary* user-written services in Rust.
- **[raft-rs] / [openraft] / [omnipaxos]** are consensus libraries: they
  order a log but bring no deterministic execution runtime, codec
  discipline, or "write a deterministic service" ergonomics.
- **Kafka / Redpanda / NATS** solve a different problem: ms-class brokered
  messaging with per-partition order and non-deterministic consumers.

[Aeron Cluster]: https://github.com/aeron-io/aeron
[LMAX Disruptor]: https://github.com/LMAX-Exchange/disruptor
[TigerBeetle]: https://github.com/tigerbeetle/tigerbeetle
[raft-rs]: https://github.com/tikv/raft-rs
[openraft]: https://github.com/databendlabs/openraft
[omnipaxos]: https://github.com/haraldng/omnipaxos

Recommended background: Martin Fowler's
[The LMAX Architecture](https://martinfowler.com/articles/lmax.html) and
Brian Nigito's [How to Build an Exchange](https://www.youtube.com/watch?v=b1e4t2k2KJY).

## Design notes & honest limitations

- **One logical sequencer caps throughput.** The eventual mitigation is
  sharding by `stream_id` (independent sequencers, no global order across
  shards) — an explicit tradeoff, not a bug.
- **A lone sequencer cannot be split-brain-safe by itself.** M4 is explicit
  about this: leadership is an epoch lease granted by a majority of
  acceptors (at most one leader per epoch, ever), the fast tier is
  bounded-loss-on-failover (the historical exchange tradeoff), and the safe
  tier pays a quorum round-trip for no-loss. The simulator drove two real
  design rules during development: election winners must reconcile their
  journal against the quorum's fences before leading, and replica acks are
  epoch-scoped (a fenced-off suffix vouches for nothing).
- **Acceptor state must be durable.** An acceptor that forgets a promise
  can elect two leaders for one epoch; embedders must persist `promised`
  before granting. The simulator does not yet crash acceptors.
- **Deriving `Encode`/`Decode` requires `ticktape-core` in your
  dependencies** (trait/derive split, as with serde).
- **Recovery still reads (and CRC-verifies) the full journal**; snapshots
  skip re-*applying* old inputs, and segment skip-scan/compaction lands with
  journal compaction.
- **The transport's socket layer is thin by design** — one frame per packet
  (batching is a planned optimization), blocking gap-fill, and Unix
  datagram sockets instead of the planned shared-memory ring for same-box
  IPC. The reliability *logic* is the fuzzed part.
- The simulator does not yet model directory-entry loss on crash, reordered
  (non-prefix) page flushes, or multi-node faults (those arrive with M4).

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Contributions are welcome under
the same terms; the API is early and moving fast, so opening an issue before
a large PR is kind to everyone.
