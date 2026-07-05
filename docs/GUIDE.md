# Ticktape: a guided tour

A teaching walkthrough of the sequencer architecture and how to build on it.
The [README](../README.md) is the pitch and the [WIRE.md](../WIRE.md) is the
byte-level reference; this is the *learning path* — read top to bottom.

---

## 1. The one idea

A **single logical sequencer** stamps every input with a gapless number and a
timestamp, writes it to a durable **journal**, and hands it to a
**deterministic state machine**. That's the whole architecture. Everything
else — replication, recovery, failover, the gateway — is a consequence.

```
   inputs ──▶ [ sequencer ] ──▶ journal (system of record)
                   │                 │
                   ▼                 ▼
             assign seq + ts    replay on restart
                   │
                   ▼
            [ your Service ] ── deterministic apply, in order
```

Two properties fall out of this and carry the entire design:

1. **The journal is the system of record.** State is a *projection* of the
   journal. Lose the state, replay the journal, get the identical state back.
2. **`apply` is a pure function of the input stream.** Same inputs ⇒
   bit-identical state, on every replica, every replay, every machine. This is
   what makes replication "ship the ordered inputs, recompute" instead of
   "copy the mutations."

If you internalize only one thing: **determinism is not a nice-to-have here,
it is the mechanism.** Recovery, replication, and the simulator all rely on it.

---

## 2. The `Service` contract

Your application is one trait:

```rust
trait Service {
    type Input;      // a command (canonical-encoded)
    type Output;     // an event
    type Snapshot;   // serializable state
    type Config;

    fn genesis(config: &Self::Config) -> Self;
    fn apply(&mut self, seq: Seq, input: &Self::Input, ctx: &mut Ctx<'_, Self::Output>);
    fn on_timer(&mut self, id: u64, ctx: &mut Ctx<'_, Self::Output>) { /* default: ignore */ }
    fn snapshot(&self) -> Self::Snapshot;
    fn restore(snap: Self::Snapshot, config: &Self::Config) -> Self;
}
```

`apply` is the whole application. Inside it you may:

- read **`ctx.now()`** — sequenced time, *not* the wall clock (identical on
  every replica);
- read **`ctx.seq()`**;
- **`ctx.emit(output)`** — put an event on the stream;
- **`ctx.set_timer(id, at)` / `cancel_timer(id)`** — schedule deterministically
  (fires as a journaled `TimerFired`, so it replays identically).

You may **not**: read the wall clock, use randomness, do I/O, spawn threads, or
iterate a `HashMap` (nondeterministic order — the codec won't even let you
encode one). Those are the determinism rules, and they are enforced: `HashMap`
and floats have no `Encode`/`Decode` impls, so a nondeterministic field fails
to compile.

> **The split-phase rule.** Need to call the outside world (a pricing service,
> an email)? You can't do it in `apply`. Instead *emit a request event*; an
> adapter outside the state machine performs the effect and feeds the response
> back in as a future sequenced input. The state machine only ever sees a clean,
> ordered stream.

---

## 3. Your first service

A counter, in full:

```rust
use ticktape::{Ctx, Seq, Service};
use ticktape_codec::{Encode, Decode};

struct Counter { total: i64 }

#[derive(Encode, Decode)]
enum Cmd { Add(i64), Reset }

#[derive(Encode, Decode)]
struct Snap { total: i64 }

impl Service for Counter {
    type Input = Cmd; type Output = i64; type Snapshot = Snap; type Config = ();
    fn genesis(_: &()) -> Self { Counter { total: 0 } }
    fn apply(&mut self, _: Seq, cmd: &Cmd, ctx: &mut Ctx<'_, i64>) {
        match cmd { Cmd::Add(n) => self.total += n, Cmd::Reset => self.total = 0 }
        ctx.emit(self.total);
    }
    fn snapshot(&self) -> Snap { Snap { total: self.total } }
    fn restore(s: Snap, _: &()) -> Self { Counter { total: s.total } }
}
```

Run it on a node:

```rust
let mut node: Node<Counter> = Node::open(NodeConfig::new("./journal"), ())?;
let (seq, outputs) = node.submit(Cmd::Add(5))?;   // journaled, then applied
assert_eq!(outputs, vec![5]);
```

`submit` journals the input *before* it applies it: an input either exists
durably in the total order, or it never happened. Kill the process and reopen —
the counter is 5 again, rebuilt from the journal.

That is the entire loop. Everything below is about doing it durably, at scale,
and without losing sleep.

---

## 4. Durability, in tiers

You choose how much a crash may cost:

| Tier | What it means | How |
|---|---|---|
| **0** | Single node, journal-backed | `Node`, one machine |
| **1** | Async standby, bounded loss | replicas tail the stream; a failover loses only the leader's unreplicated tail |
| **2** | Quorum commit, **no committed loss** | an input's outputs are withheld until a majority has journaled it |

The knob for the fsync/latency tradeoff is separate:

- `FsyncPolicy::EveryFrame` — safest, one `fdatasync` per input (ms on a
  barrier-fsync disk).
- `FsyncPolicy::Micros(n)` — time-windowed group commit.
- `Node::submit_batch(&[input])` — **group commit**: one write + one fsync for a
  whole batch, byte-identical to N submits. This is how you reach the synced
  budget under load.
- `FsyncPolicy::Never` — durability comes from replication instead (the classic
  exchange configuration).

---

## 5. Recovery is just startup

There is no separate "recovery mode." `Node::open` always does the same thing:

```
restore(newest valid snapshot)  +  replay(journal tail since that snapshot)
```

A fresh journal replays from genesis. A crashed one replays what survived. A
torn final frame is detected by its CRC and truncated. A corrupt snapshot is
skipped in favor of an older one, or full replay. **Snapshots are an
optimization, never the system of record** — you can delete every `.snap` file
and lose nothing but startup time.

For 24×7 operation, snapshots + journal compaction bound disk: old segments
covered by a retained snapshot are deleted, so the log is a bounded tail, not a
day-roll.

---

## 6. Why you can trust `apply` — the simulator

The determinism rules aren't honor-system. `ticktape-sim` runs your service
under a **deterministic simulation** (DST): a seeded RNG drives thousands of
inputs against a *fault-injecting* in-memory disk that models power-loss
crashes (synced bytes survive, unsynced tails are lost, sectors can tear).
After every simulated crash it asserts:

- the synced prefix survived;
- the journal is a byte-exact contiguous log;
- **an independent genesis-replay of the inputs byte-matches the recovered
  state** (this is the determinism check — an ambient global, a `HashMap`, a
  clock leak all break it);
- your own `Invariants::check` holds.

A seed that fails is a bug you can replay exactly. The order-book example ships
with exchange-grade invariants (never a crossed book; every share is traded,
canceled, or resting) checked this way across thousands of crashes. *This is the
moat* — the reason the whole framework is worth more than its parts.

---

## 7. Going multi-node

A `Service` that follows the determinism rules replicates for free:

- **Transport** (`ticktape-transport`): MoldUDP64-style A/B UDP feeds with a
  TCP gap-fill retransmitter. A `Replica` consumes the ordered stream into its
  own copy of your service — redundancy through recomputation.
- **Election + fencing** (`ticktape-cluster`): epoch-lease leader election
  (provably at most one leader per epoch) and the `EpochChange` fence that
  makes a deposed leader harmless. All verified in a multi-node deterministic
  simulation of kills, partitions, zombie leaders, and dueling candidates.
- **Packaged server** (`ticktape-server`): a leader + follower deployment with
  **automatic failover** — a standby's failure detector promotes it with no
  operator action, preserving the exact pre-failover state.
- **Gateway** (`ticktape-gateway`): the edge — per-session dedup (exactly-once
  effect under retries), flow control, cancel-on-disconnect as a *sequenced*
  input, drop-copy observers, and a per-session replayable outbox so a
  reconnecting client is backfilled exactly what it missed.

The point that keeps recurring: because state is a pure function of the ordered
stream, "the whole session layer is exercised by the simulator like any other
input." You don't test replication separately from logic — it's all one input
stream.

---

## 8. Where to go next

- **Build a real service:** read `examples/orderbook` — a price-time-priority
  limit order book with good-till-date orders (deterministic timers), fuzzed
  under fault injection.
- **Put it behind clients:** `examples/exchange` runs that book behind the TCP
  gateway, driven by real clients in `cargo test`.
- **The wire:** [WIRE.md](../WIRE.md) if you're writing a non-Rust node.
- **Interop / alternatives:** `ticktape-sbe` (SBE-framed payloads) and
  `ticktape-raft` (delegate log ordering to Raft) are opt-in, feature-gated.

The mental model never changes: **order the inputs, journal them, recompute
deterministically.** Everything is a variation on that one move.
