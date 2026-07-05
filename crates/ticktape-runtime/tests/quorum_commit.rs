//! Tier-2 deferred-ack semantics on a single leader [`Node`] (P1.3).
//!
//! These exercise the commit-watermark contract in isolation — no sockets,
//! no cluster — by having the test play the role of the replication
//! transport: it `submit`s on the leader and feeds follower acks back with
//! `record_ack`, asserting that outputs are released *exactly* when a
//! majority holds the input, never before.

use ticktape_codec::{Decode, Encode};
use ticktape_core::{Ctx, Seq, Service, Timestamp};
use ticktape_journal::FsyncPolicy;
use ticktape_runtime::{ManualClock, Node, NodeConfig};

/// Minimal service: each `Push(n)` emits `Ack(n)` so we can see exactly
/// which inputs' outputs have been released.
struct Log {
    count: u64,
}

#[derive(Encode, Decode, PartialEq, Debug)]
struct Push(u64);

#[derive(Encode, Decode, PartialEq, Debug)]
struct Ack(u64);

#[derive(Encode, Decode, PartialEq, Debug)]
struct LogSnap {
    count: u64,
}

impl Service for Log {
    type Input = Push;
    type Output = Ack;
    type Snapshot = LogSnap;
    type Config = ();

    fn genesis(_: &()) -> Self {
        Log { count: 0 }
    }

    fn apply(&mut self, _seq: Seq, input: &Push, ctx: &mut Ctx<'_, Ack>) {
        self.count += 1;
        ctx.emit(Ack(input.0));
    }

    fn snapshot(&self) -> LogSnap {
        LogSnap { count: self.count }
    }

    fn restore(snap: LogSnap, _: &()) -> Self {
        Log { count: snap.count }
    }
}

/// A quorum leader: 3 voters, this leader is replica 0, real fsync (Tier 2
/// only means anything if the leader's own copy is durable).
fn leader(dir: &std::path::Path, voters: usize) -> Node<Log, ManualClock> {
    let mut config = NodeConfig::new(dir).with_quorum(voters, 0);
    config.journal.fsync = FsyncPolicy::EveryFrame;
    Node::open_with_clock(config, (), ManualClock(Timestamp(1))).unwrap()
}

#[test]
fn outputs_are_withheld_until_a_majority_acks() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader(dir.path(), 3);

    // Submit three inputs. The leader is only 1 of 3 voters, so nothing
    // commits yet — every submit returns an empty release set.
    for i in 1..=3 {
        let (seq, released) = node.submit(Push(i)).unwrap();
        assert_eq!(seq, Seq(i));
        assert!(
            released.is_empty(),
            "seq {i} released with only the leader's own ack (1/3)"
        );
    }
    assert_eq!(node.commit_watermark(), Seq::GENESIS);
    assert_eq!(node.pending_commit_count(), 3);

    // Follower 1 acks up to seq 2. Now {leader@3, f1@2} → majority (2/3)
    // holds everything through seq 2, so seqs 1 and 2 commit and release.
    let released = node.record_ack(1, Seq(2));
    assert_eq!(
        released,
        vec![(Seq(1), vec![Ack(1)]), (Seq(2), vec![Ack(2)])],
        "a 2/3 majority through seq 2 must release seqs 1 and 2 in order"
    );
    assert_eq!(node.commit_watermark(), Seq(2));
    assert_eq!(node.pending_commit_count(), 1); // seq 3 still pending

    // A second, slower follower acking an already-committed seq changes
    // nothing.
    assert!(node.record_ack(2, Seq(1)).is_empty());
    assert_eq!(node.commit_watermark(), Seq(2));

    // Follower 1 catches up to the tip → seq 3 commits.
    let released = node.record_ack(1, Seq(3));
    assert_eq!(released, vec![(Seq(3), vec![Ack(3)])]);
    assert_eq!(node.commit_watermark(), Seq(3));
    assert_eq!(node.pending_commit_count(), 0);
}

#[test]
fn single_voter_commits_immediately() {
    // voters = 1: the leader is a majority of itself, so Tier 2 collapses to
    // Tier-0 latency — outputs come straight back from submit.
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader(dir.path(), 1);

    let (seq, released) = node.submit(Push(42)).unwrap();
    assert_eq!(seq, Seq(1));
    assert_eq!(released, vec![Ack(42)]);
    assert_eq!(node.commit_watermark(), Seq(1));
    assert_eq!(node.pending_commit_count(), 0);
}

#[test]
fn stale_acks_never_regress_the_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader(dir.path(), 3);
    for i in 1..=5 {
        node.submit(Push(i)).unwrap();
    }

    // Follower 1 jumps to the tip: watermark = min(leader@5, f1@5) = 5.
    let released = node.record_ack(1, Seq(5));
    assert_eq!(released.len(), 5);
    assert_eq!(node.commit_watermark(), Seq(5));

    // A delayed, out-of-order ack from the same follower at an older seq is
    // ignored — high-waters are monotonic and nothing re-releases.
    assert!(node.record_ack(1, Seq(2)).is_empty());
    assert_eq!(node.commit_watermark(), Seq(5));
}

#[test]
fn a_minority_never_commits() {
    // 5 voters need 3 copies. Leader + one follower is only 2 — no commit.
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader(dir.path(), 5);
    for i in 1..=4 {
        node.submit(Push(i)).unwrap();
    }
    assert!(node.record_ack(1, Seq(4)).is_empty());
    assert_eq!(node.commit_watermark(), Seq::GENESIS);
    // A third voter tips it over: {leader, f1, f2} = 3/5.
    let released = node.record_ack(2, Seq(4));
    assert_eq!(released.len(), 4);
    assert_eq!(node.commit_watermark(), Seq(4));
}

#[test]
fn ticks_advance_the_leader_high_water_so_commit_tracks_the_tip() {
    // A control frame (tick) between inputs must not strand later inputs
    // below the watermark: the leader's self-high-water has to move past the
    // tick, so a follower acking the tip still commits the inputs after it.
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader(dir.path(), 3);

    node.submit(Push(1)).unwrap(); // seq 1
    node.tick().unwrap(); // seq 2 (no output)
    node.submit(Push(2)).unwrap(); // seq 3

    // Follower acks the tip (seq 3). Majority through 3 → both inputs
    // release; the tick contributes no output.
    let released = node.record_ack(1, Seq(3));
    assert_eq!(released, vec![(Seq(1), vec![Ack(1)]), (Seq(3), vec![Ack(2)])]);
    assert_eq!(node.commit_watermark(), Seq(3));
}
