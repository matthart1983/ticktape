//! Deterministic simulation of Tier-2 deferred acks driven against the
//! **runtime** `Node`'s own quorum-commit machinery (P1.3 done-criterion:
//! "exercised by the cluster sim invariants against the runtime rather than
//! the test harness's own bookkeeping").
//!
//! The simulator plays the replication transport: it submits inputs to a
//! real leader [`Node`] running in quorum mode on the fault-injecting
//! [`SimStorage`], and feeds follower acks back in adversarial orders
//! (reordered, stale, stalled). Every step it cross-checks the *runtime's*
//! decisions — its commit watermark and the exact `(seq, outputs)` it
//! releases — against an independent reference model computed a different
//! way (a sorted-vector majority instead of the Node's `BTreeMap`
//! split-off). A divergence means the runtime released outputs no quorum
//! held, released them out of order, released one twice, or lost a
//! committed output across a crash.
//!
//! Two properties, both asserted continuously across many seeds:
//!
//! - **Safety**: the runtime never releases a seq's outputs before a
//!   majority (leader included) durably holds it, and releases each exactly
//!   once, in seq order.
//! - **Durability across crashes**: a released output is always within the
//!   recovered journal — power loss never resurrects an uncommitted output
//!   nor forgets a committed one.

use ticktape_core::{FrameKind, Seq, Timestamp};
use ticktape_journal::FsyncPolicy;
use ticktape_runtime::{ManualClock, Node, NodeConfig};
use ticktape_sim::demo::{gen_transfer, Bank};
use ticktape_sim::{Rng, SimStorage};

const VOTERS: usize = 5;
const FOLLOWERS: usize = VOTERS - 1;
const LEADER_ID: u32 = 0;

fn majority(n: usize) -> usize {
    n / 2 + 1
}

/// The reference commit watermark, computed independently of the Node:
/// the majority-th highest high-water among the leader and the followers
/// that have acked at least once — mirroring the Node's rule (an
/// un-acked replica is simply absent, not a zero) via a different data
/// structure so the two implementations can disagree.
fn reference_watermark(leader_tip: u64, follower_highs: &[Option<u64>]) -> u64 {
    let mut waters: Vec<u64> = vec![leader_tip];
    waters.extend(follower_highs.iter().flatten().copied());
    if waters.len() < majority(VOTERS) {
        return 0;
    }
    waters.sort_unstable_by(|a, b| b.cmp(a));
    waters[majority(VOTERS) - 1]
}

fn open_leader(storage: SimStorage, dir: &str) -> Node<Bank, ManualClock, SimStorage> {
    let mut config = NodeConfig::new(dir).with_quorum(VOTERS, LEADER_ID);
    config.journal.fsync = FsyncPolicy::EveryFrame;
    Node::open_with(config, (), ManualClock(Timestamp(1)), storage).unwrap()
}

/// One seeded run. Returns nothing; panics on any invariant violation.
fn run(seed: u64, steps: u32, inject_crashes: bool) {
    let storage = SimStorage::new();
    let dir = "/j";
    let mut node = open_leader(storage.clone(), dir);

    let mut rng = Rng::new(seed);
    // Per-follower durable high-water (None = never acked).
    let mut follower_highs: Vec<Option<u64>> = vec![None; FOLLOWERS];
    // Kind of the frame at each seq (index = seq-1): Input frames carry
    // outputs, control frames (ticks) do not.
    let mut kinds: Vec<FrameKind> = Vec::new();
    // Every seq the runtime has released outputs for, in the order it did.
    let mut released: Vec<u64> = Vec::new();
    let mut last_watermark = 0u64;

    for _ in 0..steps {
        match rng.below(100) {
            // Submit an input. With >2 voters the leader alone is never a
            // majority, so this must never release anything on its own.
            0..=59 => {
                let (seq, out) = node.submit(gen_transfer(&mut rng)).unwrap();
                kinds.push(FrameKind::Input);
                assert_eq!(seq.0 as usize, kinds.len());
                assert!(
                    out.is_empty(),
                    "seed {seed}: submit released with only the leader's ack (1/{VOTERS})"
                );
            }
            // A sequenced tick: advances the leader's high-water but carries
            // no output, so it must never appear in the released set.
            60..=64 => {
                let seq = node.tick().unwrap();
                kinds.push(FrameKind::Tick);
                assert_eq!(seq.0 as usize, kinds.len());
            }
            // A follower reports progress — possibly reordered or stale.
            // Its target is any seq up to the leader's tip (0 models a
            // straggler that has journaled nothing yet).
            _ => {
                let tip = node.seq().0;
                let f = rng.below(FOLLOWERS as u64) as usize;
                let target = rng.below(tip + 1);
                let pairs = node.record_ack(f as u32 + 1, Seq(target));

                // The Node keeps high-waters monotonic; the reference must
                // too, or a stale ack would appear to lower the watermark.
                let prev = follower_highs[f].unwrap_or(0);
                follower_highs[f] = Some(prev.max(target));

                for (seq, _outputs) in pairs {
                    assert_eq!(
                        kinds[seq.0 as usize - 1],
                        FrameKind::Input,
                        "seed {seed}: released outputs for a non-input frame at {seq}"
                    );
                    if let Some(&last) = released.last() {
                        assert!(
                            seq.0 > last,
                            "seed {seed}: release out of order / duplicated: {} after {last}",
                            seq.0
                        );
                    }
                    released.push(seq.0);
                }
            }
        }

        // The runtime's watermark must never regress, and — in the
        // crash-free run, where the leader's commit baseline is never
        // reseeded — must equal the independently-derived reference. (After
        // a crash the reopened leader floors its watermark at its recovered
        // tip, which legitimately diverges from the live majority-of-
        // followers view; that path asserts durability instead, below.)
        let wm = node.commit_watermark().0;
        if !inject_crashes {
            assert_eq!(
                wm,
                reference_watermark(node.seq().0, &follower_highs),
                "seed {seed}: runtime watermark disagrees with the reference model"
            );
        }
        assert!(wm >= last_watermark, "seed {seed}: watermark regressed");
        last_watermark = wm;

        // A released output is always within the durable journal: the
        // runtime never releases ahead of what it has sequenced.
        assert!(
            released.last().copied().unwrap_or(0) <= node.seq().0,
            "seed {seed}: released a seq the journal does not hold"
        );

        // Occasionally lose power. Under EveryFrame fsync the whole journal
        // is durable, so recovery must find the exact same tip — and a
        // committed output is never forgotten (max released <= recovered
        // tip) nor an uncommitted one resurrected.
        if inject_crashes && rng.chance(1, 40) {
            let tip_before = node.seq().0;
            let max_released_before = released.last().copied().unwrap_or(0);
            storage.crash(&mut rng, false);
            node = open_leader(storage.clone(), dir);
            assert_eq!(
                node.seq().0,
                tip_before,
                "seed {seed}: synced journal lost frames across a crash"
            );
            assert!(
                max_released_before <= node.seq().0,
                "seed {seed}: a committed output was lost across a crash"
            );
            // The reopened leader floors its commit watermark at its
            // recovered tip (re-releasing recovered outputs is meaningless),
            // so realign the monotonic baseline to match.
            last_watermark = node.commit_watermark().0;
            assert_eq!(last_watermark, tip_before, "seed {seed}: reseed baseline");
        }
    }

    // Final reconciliation: the runtime released outputs for *exactly* the
    // Input-kind seqs at or below its final watermark, in order — no gaps,
    // no extras. (Skipped for crash runs, where the baseline resets shift
    // accounting; those runs assert durability continuously above.)
    if !inject_crashes {
        let wm = node.commit_watermark().0;
        let expected: Vec<u64> = (1..=wm)
            .filter(|&s| kinds[s as usize - 1] == FrameKind::Input)
            .collect();
        assert_eq!(
            released, expected,
            "seed {seed}: released set is not exactly the committed inputs"
        );
    }
}

#[test]
fn runtime_quorum_release_matches_reference_across_seeds() {
    for seed in 0..300 {
        run(seed, 500, false);
    }
}

#[test]
fn runtime_quorum_survives_crashes_without_losing_commits() {
    for seed in 0..200 {
        run(seed, 500, true);
    }
}
