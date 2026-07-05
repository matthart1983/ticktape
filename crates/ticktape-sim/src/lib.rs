//! Deterministic simulation testing (DST) for Ticktape services.
//!
//! Because a [`Service`] is a pure function of its sequenced input stream
//! and every source of nondeterminism (time, storage, randomness) is behind
//! a trait, an entire node — journal, crashes, recovery — runs inside one
//! thread on one seed, deterministically. Same seed ⇒ same run, exactly;
//! a failing seed *is* the reproduction.
//!
//! What one simulated run does: drive a service with seeded random inputs
//! over a [`SimStorage`] disk, randomly interleaving syncs, sequenced
//! ticks, and **crashes** (power loss: unsynced tails partially survive,
//! possibly bit-flipped). After every crash + recovery it checks the safety
//! invariants:
//!
//! 1. **Recovery succeeds** — a crash may lose the unsynced tail, never the
//!    ability to restart.
//! 2. **Durability** — every frame synced before the crash is still there.
//! 3. **Total order** — surviving frames are exactly a gapless prefix of
//!    what was submitted, byte-for-byte.
//! 4. **Determinism** — the recovered node's state byte-matches an
//!    independent `genesis + replay` of the surviving frames.
//! 5. **Application invariants** — your [`Invariants::check`] holds.
//!
//! The [`vopr`] loop runs many seeds and, on failure, truncates the
//! schedule to the failing step (seed-preserving shrinking) and verifies
//! the reproduction. `cargo run -p ticktape-sim --bin vopr` fuzzes
//! continuously.

pub mod demo;
pub mod rng;
pub mod storage;

pub use rng::Rng;
pub use storage::SimStorage;

use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use ticktape_core::{decode_all, encode_to_vec, Ctx, FrameKind, OutBuf, Seq, Service, Timestamp};
use ticktape_journal::{FsyncPolicy, Journal};
use ticktape_runtime::{Node, NodeConfig, TimeSource};

/// A safety property of *your* state machine, checked by the simulator
/// after every applied input and every recovery (spec §11).
pub trait Invariants: Service {
    fn check(&self) -> Result<(), InvariantViolation>;
}

/// A broken safety property. The message should say what held false.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvariantViolation {
    pub what: String,
}

impl InvariantViolation {
    pub fn new(what: impl Into<String>) -> Self {
        InvariantViolation { what: what.into() }
    }
}

impl fmt::Display for InvariantViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.what)
    }
}

/// Virtual time, advanced only by the simulator. Implements the runtime's
/// [`TimeSource`], so sequenced timestamps derive from simulated time.
#[derive(Clone, Default)]
pub struct SimClock {
    nanos: Arc<AtomicU64>,
}

impl SimClock {
    pub fn advance(&self, nanos: u64) {
        self.nanos.fetch_add(nanos, Ordering::Relaxed);
    }
}

impl TimeSource for SimClock {
    fn now(&mut self) -> Timestamp {
        Timestamp(self.nanos.load(Ordering::Relaxed))
    }
}

/// Knobs for one simulated run. Per-mille dials are per-step probabilities;
/// the remainder of the probability mass submits an application input.
#[derive(Debug, Clone)]
pub struct SimConfig {
    pub seed: u64,
    /// Number of simulated operations.
    pub steps: u32,
    /// Probability (‰ per step) of a power-loss crash + recovery.
    pub crash_per_mille: u32,
    /// Probability (‰ per step) of an explicit journal sync.
    pub sync_per_mille: u32,
    /// Probability (‰ per step) of a sequenced tick.
    pub tick_per_mille: u32,
    /// Small on purpose: forces frequent segment rolls under fuzz.
    pub segment_bytes: u64,
    /// Whether surviving crash tails may take a bit flip (torn sector).
    pub torn_tails: bool,
    /// Node snapshot cadence. `Some(n)` exercises the snapshot/restore
    /// recovery path (and snapshot files take crash faults like any other
    /// file); `None` forces full replay from genesis.
    pub snapshot_every: Option<u64>,
}

impl SimConfig {
    pub fn new(seed: u64) -> Self {
        SimConfig {
            seed,
            steps: 400,
            crash_per_mille: 30,
            sync_per_mille: 100,
            tick_per_mille: 80,
            segment_bytes: 512,
            torn_tails: true,
            snapshot_every: Some(20),
        }
    }
}

/// A clean run's tallies (also proof of what the run exercised).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SimStats {
    pub inputs: u64,
    pub ticks: u64,
    pub syncs: u64,
    pub crashes: u64,
}

/// A failed run: the seed + step are the complete reproduction recipe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimFailure {
    pub seed: u64,
    /// 0-based step at which the violation was detected. Re-running the
    /// same seed with `steps = step + 1` reproduces this failure.
    pub step: u32,
    pub violation: InvariantViolation,
}

impl fmt::Display for SimFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {} failed at step {}: {}",
            self.seed, self.step, self.violation
        )
    }
}

/// What the simulator expects to find in the journal for one sequenced frame.
enum Expected {
    Input(Vec<u8>),
    Tick,
    /// A framework-generated `SnapshotMark` (the node appends one right
    /// after a frame that hits the snapshot cadence).
    Mark,
}

/// Run one seeded simulation of `S` under storage faults.
///
/// `gen_input` produces the workload; it must derive inputs only from the
/// given [`Rng`] (any other entropy source breaks seed reproducibility).
pub fn simulate<S>(
    config: &SimConfig,
    service_config: S::Config,
    mut gen_input: impl FnMut(&mut Rng) -> S::Input,
) -> Result<SimStats, SimFailure>
where
    S: Service + Invariants,
    S::Config: Clone,
{
    let mut rng = Rng::new(config.seed);
    let storage = SimStorage::new();
    let clock = SimClock::default();
    clock.advance(1_000_000_000); // start at t=1s, not the epoch

    let node_config = || {
        let mut nc = NodeConfig::new(PathBuf::from("/sim/journal"));
        nc.journal.segment_bytes = config.segment_bytes;
        // Durability is driven explicitly by the sim's `sync` op, so lost
        // unsynced tails are actually possible.
        nc.journal.fsync = FsyncPolicy::Never;
        nc.snapshot_every = config.snapshot_every;
        nc
    };

    let fail = |step: u32, violation: InvariantViolation| SimFailure {
        seed: config.seed,
        step,
        violation,
    };

    let mut node: Node<S, SimClock, SimStorage> = Node::open_with(
        node_config(),
        service_config.clone(),
        clock.clone(),
        storage.clone(),
    )
    .map_err(|e| {
        fail(
            0,
            InvariantViolation::new(format!("initial open failed: {e}")),
        )
    })?;

    let mut expected: Vec<Expected> = Vec::new();
    let mut synced: usize = 0; // frames guaranteed durable
    let mut stats = SimStats::default();

    for step in 0..config.steps {
        clock.advance(rng.below(1_000_000_000));
        let dice = rng.below(1000) as u32;

        if dice < config.crash_per_mille {
            stats.crashes += 1;
            storage.crash(&mut rng, config.torn_tails);
            node = recover_and_check::<S>(
                step,
                config,
                &service_config,
                &storage,
                &clock,
                node_config(),
                &mut expected,
                &mut synced,
                node,
            )?;
        } else if dice < config.crash_per_mille + config.sync_per_mille {
            stats.syncs += 1;
            node.sync()
                .map_err(|e| fail(step, InvariantViolation::new(format!("sync failed: {e}"))))?;
            synced = expected.len();
        } else if dice < config.crash_per_mille + config.sync_per_mille + config.tick_per_mille {
            stats.ticks += 1;
            let before = node.seq();
            node.tick()
                .map_err(|e| fail(step, InvariantViolation::new(format!("tick failed: {e}"))))?;
            expected.push(Expected::Tick);
            push_marks(&mut expected, before, node.seq());
        } else {
            stats.inputs += 1;
            let input = gen_input(&mut rng);
            let bytes = encode_to_vec(&input);
            let before = node.seq();
            node.submit(input)
                .map_err(|e| fail(step, InvariantViolation::new(format!("submit failed: {e}"))))?;
            expected.push(Expected::Input(bytes));
            push_marks(&mut expected, before, node.seq());
            node.service().check().map_err(|v| fail(step, v))?;
        }
    }

    // Final crash + recovery so every run ends with the full checklist.
    storage.crash(&mut rng, config.torn_tails);
    stats.crashes += 1;
    recover_and_check::<S>(
        config.steps.saturating_sub(1),
        config,
        &service_config,
        &storage,
        &clock,
        node_config(),
        &mut expected,
        &mut synced,
        node,
    )?;

    Ok(stats)
}

/// One op should consume one seq; any extra seqs the node consumed are
/// framework-appended `SnapshotMark` frames.
fn push_marks(expected: &mut Vec<Expected>, before: Seq, after: Seq) {
    for _ in 1..(after.0 - before.0) {
        expected.push(Expected::Mark);
    }
}

/// Reopen the node after a crash and run the full invariant checklist.
#[allow(clippy::too_many_arguments)]
fn recover_and_check<S>(
    step: u32,
    config: &SimConfig,
    service_config: &S::Config,
    storage: &SimStorage,
    clock: &SimClock,
    node_config: NodeConfig,
    expected: &mut Vec<Expected>,
    synced: &mut usize,
    old_node: Node<S, SimClock, SimStorage>,
) -> Result<Node<S, SimClock, SimStorage>, SimFailure>
where
    S: Service + Invariants,
    S::Config: Clone,
{
    let fail = |violation: InvariantViolation| SimFailure {
        seed: config.seed,
        step,
        violation,
    };
    let journal_config = node_config.journal.clone();

    // The crashed process is gone; its drop-time sync lands on fenced
    // handles and is ignored.
    drop(old_node);

    // Invariant 1: recovery succeeds.
    let node: Node<S, SimClock, SimStorage> = Node::open_with(
        node_config,
        service_config.clone(),
        clock.clone(),
        storage.clone(),
    )
    .map_err(|e| {
        fail(InvariantViolation::new(format!(
            "recovery failed after crash: {e}"
        )))
    })?;

    let recovered = node.seq().as_u64() as usize;

    // Invariant 2: durability — synced frames survive.
    if recovered < *synced {
        return Err(fail(InvariantViolation::new(format!(
            "durability violated: {synced} frames were synced but only {recovered} survived"
        ))));
    }
    // Total order can't contain frames that were never submitted.
    if recovered > expected.len() {
        return Err(fail(InvariantViolation::new(format!(
            "phantom frames: {} submitted, {recovered} recovered",
            expected.len()
        ))));
    }

    // Invariant 3: the journal holds a contiguous, byte-exact suffix of the
    // submitted stream. With compaction (snapshot pruning bounds disk for
    // 24×7 operation) the journal keeps only [first_seq..recovered]; the
    // shadow `expected` still holds the full history, so we check the
    // surviving tail against the corresponding slice.
    let re = Journal::open_with(journal_config, storage.clone()).map_err(|e| {
        fail(InvariantViolation::new(format!(
            "journal re-read failed: {e}"
        )))
    })?;
    let survivors = re.frames;
    let first = re.first_seq.as_u64();
    // The journal holds a contiguous run [first .. journal_last], each frame
    // byte-matching the shadow. journal_last may be < recovered when a synced
    // snapshot anchored the node beyond the journal tail (the crash lost the
    // journal's unsynced tail but the snapshot file survived) — legitimate,
    // and exercised here by compaction + retention.
    for (i, frame) in survivors.iter().enumerate() {
        let seq = first + i as u64;
        if frame.seq.0 != seq {
            return Err(fail(InvariantViolation::new(format!(
                "total order broken: journal frame {i} has seq {} (expected {seq})",
                frame.seq
            ))));
        }
        if seq > recovered as u64 {
            return Err(fail(InvariantViolation::new(format!(
                "journal frame at seq {seq} exceeds recovered tip {recovered}"
            ))));
        }
        let matches = match &expected[seq as usize - 1] {
            Expected::Input(bytes) => frame.kind == FrameKind::Input && &frame.payload == bytes,
            Expected::Tick => frame.kind == FrameKind::Tick,
            Expected::Mark => frame.kind == FrameKind::SnapshotMark,
        };
        if !matches {
            return Err(fail(InvariantViolation::new(format!(
                "journal frame at seq {} does not match the submitted input",
                frame.seq
            ))));
        }
    }
    // If the recovered tip is beyond the journal, a snapshot must have
    // anchored it — otherwise state was fabricated from nowhere.
    let journal_last = survivors
        .last()
        .map_or(first.saturating_sub(1), |f| f.seq.0);
    if recovered as u64 > journal_last {
        match node.recovery_info().snapshot_seq {
            Some(s) if s.0 >= journal_last => {}
            other => {
                return Err(fail(InvariantViolation::new(format!(
                    "recovered tip {recovered} exceeds journal_last {journal_last}                      with no anchoring snapshot (recovery snapshot_seq {other:?})"
                ))));
            }
        }
    }
    // The lost tail is permanently gone; it is no longer expected.
    expected.truncate(recovered);
    // Everything that survived a power loss is durable by definition.
    *synced = recovered;

    // Invariant 4: determinism — an independent replay of the *full* history
    // (from the shadow, which compaction never touches) must byte-match the
    // recovered node's state. Stronger than replaying the journal: it proves
    // snapshot+tail recovery equals a genesis replay, even when the journal
    // no longer holds genesis. (Replay uses zero sequenced time; the sim
    // services here are time-independent, and the time-dependent replay path
    // is covered by the runtime's real-timestamp recovery tests.)
    let mut fresh = S::genesis(service_config);
    let mut outputs = OutBuf::new();
    let mut timer_ops = Vec::new();
    for (i, exp) in expected.iter().enumerate() {
        if let Expected::Input(bytes) = exp {
            let seq = Seq(i as u64 + 1);
            let input: S::Input = decode_all(bytes).map_err(|e| {
                fail(InvariantViolation::new(format!(
                    "shadow input no longer decodes at seq {seq}: {e}"
                )))
            })?;
            let mut ctx = Ctx::new(seq, Timestamp::ZERO, &mut outputs, &mut timer_ops);
            fresh.apply(seq, &input, &mut ctx);
            outputs.drain();
        }
    }
    let live = encode_to_vec(&node.service().snapshot());
    let replayed = encode_to_vec(&fresh.snapshot());
    if live != replayed {
        return Err(fail(InvariantViolation::new(
            "determinism violated: recovered state does not byte-match an independent replay \
             of the same inputs (ambient state, nondeterministic iteration, or clock leak?)",
        )));
    }

    // Invariant 5: the application's own safety properties.
    node.service().check().map_err(fail)?;

    Ok(node)
}

/// A failure found by [`vopr`], shrunk to the minimal seed-preserving
/// schedule and verified to reproduce.
#[derive(Debug, Clone)]
pub struct ShrunkFailure {
    pub failure: SimFailure,
    /// Re-run recipe: same config, `seed = failure.seed`,
    /// `steps = min_steps` reproduces the identical violation.
    pub min_steps: u32,
}

impl fmt::Display for ShrunkFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} (reproduce with seed={} steps={})",
            self.failure, self.failure.seed, self.min_steps
        )
    }
}

/// The VOPR loop: run `simulate` across many seeds; on the first failure,
/// shrink the schedule to the failing step (seed-preserving) and verify the
/// reproduction before reporting.
pub fn vopr<S>(
    base: &SimConfig,
    seeds: impl IntoIterator<Item = u64>,
    service_config: S::Config,
    gen_input: impl Fn(&mut Rng) -> S::Input,
) -> Result<SimStats, ShrunkFailure>
where
    S: Service + Invariants,
    S::Config: Clone,
{
    let mut totals = SimStats::default();
    for seed in seeds {
        let config = SimConfig {
            seed,
            ..base.clone()
        };
        match simulate::<S>(&config, service_config.clone(), &gen_input) {
            Ok(stats) => {
                totals.inputs += stats.inputs;
                totals.ticks += stats.ticks;
                totals.syncs += stats.syncs;
                totals.crashes += stats.crashes;
            }
            Err(failure) => {
                // Seed-preserving shrink: the decision stream depends only
                // on the seed, so cutting the schedule at the failing step
                // reproduces the run's prefix exactly.
                let min_steps = failure.step + 1;
                let shrunk_config = SimConfig {
                    steps: min_steps,
                    ..config.clone()
                };
                let reproduced = simulate::<S>(&shrunk_config, service_config.clone(), &gen_input);
                let confirmed = match reproduced {
                    Err(ref again) => again.violation == failure.violation,
                    Ok(_) => false,
                };
                // If the shrunk run doesn't reproduce (e.g. the violation
                // surfaced at the end-of-run crash), fall back to the full
                // schedule, which is still deterministic.
                let min_steps = if confirmed { min_steps } else { config.steps };
                return Err(ShrunkFailure { failure, min_steps });
            }
        }
    }
    Ok(totals)
}
