//! The [`Service`] trait — the entire user-facing application contract —
//! and [`Ctx`], the only channel through which a service touches the world.

use crate::codec::{Decode, Encode};
use crate::seq::{Seq, Timestamp};

/// Buffer of outputs emitted during one `apply` step.
///
/// A thin wrapper so the emission surface can grow (e.g. per-output stream
/// routing) without breaking the `Ctx::emit` signature.
#[derive(Debug)]
pub struct OutBuf<O> {
    items: Vec<O>,
}

impl<O> OutBuf<O> {
    pub fn new() -> Self {
        OutBuf { items: Vec::new() }
    }

    pub fn push(&mut self, out: O) {
        self.items.push(out);
    }

    pub fn drain(&mut self) -> Vec<O> {
        core::mem::take(&mut self.items)
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl<O> Default for OutBuf<O> {
    fn default() -> Self {
        Self::new()
    }
}

/// A timer scheduling request recorded during an apply step. The runtime
/// drains these into its timer wheel after each step; applications never
/// construct them directly (they use [`Ctx::set_timer`] /
/// [`Ctx::cancel_timer`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerReq {
    /// Arm (or re-arm) timer `id` to fire when sequenced time reaches `at`.
    Set { id: u64, at: Timestamp },
    /// Cancel timer `id` if it is pending.
    Cancel { id: u64 },
}

/// The deterministic execution context passed to [`Service::apply`].
///
/// Every capability here is deterministic: sequenced time, the current
/// sequence number, output emission, and timer scheduling. There is
/// deliberately no `spawn`, `sleep`, `rand`, file, or socket access —
/// external interactions use the split-phase pattern (emit a request event;
/// the response re-enters as a future sequenced input).
pub struct Ctx<'a, O> {
    seq: Seq,
    now: Timestamp,
    outputs: &'a mut OutBuf<O>,
    timers: &'a mut Vec<TimerReq>,
}

impl<'a, O> Ctx<'a, O> {
    /// Construct a context for one apply step. Called by the runtime (and
    /// the simulator); applications never build a `Ctx` themselves. The
    /// `timers` buffer collects scheduling requests for the runtime to
    /// apply; contexts that cannot schedule (a follower mirroring journaled
    /// firings) pass a throwaway buffer whose contents are ignored.
    pub fn new(
        seq: Seq,
        now: Timestamp,
        outputs: &'a mut OutBuf<O>,
        timers: &'a mut Vec<TimerReq>,
    ) -> Self {
        Ctx {
            seq,
            now,
            outputs,
            timers,
        }
    }

    /// Deterministic time: the sequencer-assigned timestamp of the current
    /// frame, NOT the OS clock. Identical on every replica and every replay.
    pub fn now(&self) -> Timestamp {
        self.now
    }

    /// The sequence number of the input being applied.
    pub fn seq(&self) -> Seq {
        self.seq
    }

    /// Emit an output event onto the sequenced stream.
    pub fn emit(&mut self, out: O) {
        self.outputs.push(out);
    }

    /// Arm a deterministic timer: when sequenced time reaches `at`, the
    /// sequencer injects a `TimerFired` frame carrying `id`, delivered to
    /// [`Service::on_timer`]. Re-arming an existing `id` replaces its
    /// deadline. `at` in the past fires at the next sequenced frame (a
    /// `tick` suffices) — never retroactively. Because firing is a sequenced
    /// frame, it is journaled and replays identically on every replica.
    pub fn set_timer(&mut self, id: u64, at: Timestamp) {
        self.timers.push(TimerReq::Set { id, at });
    }

    /// Cancel a pending timer. A no-op if `id` is not armed (including if it
    /// already fired).
    pub fn cancel_timer(&mut self, id: u64) {
        self.timers.push(TimerReq::Cancel { id });
    }
}

/// A deterministic replicated state machine.
///
/// # Contract
///
/// `apply` MUST be a pure function of (`&mut self`, `input`, `ctx`). It may
/// read the deterministic clock via `ctx.now()`, emit outputs via `ctx`, and
/// mutate `self`. It MUST NOT read the wall clock, use randomness, perform
/// I/O, spawn threads, or iterate collections in nondeterministic order
/// (use `BTreeMap`/`BTreeSet`, never `HashMap`/`HashSet`).
///
/// Same inputs ⇒ bit-identical state and outputs, on every replica, every
/// replay, every machine. The runtime replays the journal through `apply`
/// to recover state, and the simulator asserts replica equivalence — a
/// violation of this contract is what those checks exist to catch.
pub trait Service: Sized {
    /// The application command type (already decoded from the wire).
    type Input: Encode + Decode;
    /// The application output/event type.
    type Output: Encode;
    /// Serializable snapshot of all state needed to resume.
    type Snapshot: Encode + Decode;
    /// Static configuration. Must be identical on every replica; it is part
    /// of the deterministic inputs.
    type Config;

    /// The application's input-schema version. A leader stamps it into the
    /// `EpochChange` fence; a replica rejects a stream whose version differs
    /// from its own (an explicit "schema mismatch" instead of a confusing
    /// mid-replay decode error). Bump it whenever `Input`'s encoding changes
    /// incompatibly. Defaults to `0` — services that never evolve their
    /// schema, or don't replicate, can ignore it.
    const SCHEMA_VERSION: u32 = 0;

    /// Construct empty initial state (seq 0, no inputs applied).
    fn genesis(config: &Self::Config) -> Self;

    /// The step function. Called exactly once per sequenced input, in seq
    /// order, on every replica. Deterministic. This is the whole
    /// application.
    fn apply(&mut self, seq: Seq, input: &Self::Input, ctx: &mut Ctx<'_, Self::Output>);

    /// Handle a timer that reached its deadline (armed via
    /// [`Ctx::set_timer`]). `id` is the value passed when arming. Called at
    /// the sequenced time the timer fired, exactly once, on every replica
    /// and replay — the firing is a journaled frame. Like `apply`, it may
    /// read `ctx.now()`, emit outputs, and arm/cancel further timers.
    /// Default: ignore, for services that never use timers.
    fn on_timer(&mut self, _id: u64, _ctx: &mut Ctx<'_, Self::Output>) {
        let _ = _id;
    }

    /// Serialize state at the current seq for snapshotting.
    fn snapshot(&self) -> Self::Snapshot;

    /// Restore from a snapshot; the runtime then replays the journal from
    /// the snapshot's seq.
    fn restore(snap: Self::Snapshot, config: &Self::Config) -> Self;
}
