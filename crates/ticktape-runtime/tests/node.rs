//! End-to-end Node tests: sequencing, crash recovery, replay equivalence,
//! deterministic time, and the in-proc sequenced stream.

use ticktape_codec::{Decode, Encode};
use ticktape_core::{Ctx, FrameKind, Seq, Service, Timestamp};
use ticktape_journal::FsyncPolicy;
use ticktape_runtime::{ManualClock, Node, NodeConfig};

/// A service exercising state, outputs, and deterministic time: it records
/// the timestamp of the last `Bump` and a running total.
struct Meter {
    total: i64,
    last_bump_at: Timestamp,
}

#[derive(Encode, Decode, PartialEq, Debug)]
enum Cmd {
    Bump(i64),
    Zero,
}

#[derive(Encode, Decode, PartialEq, Debug)]
enum Evt {
    Total(i64),
    At(u64),
}

#[derive(Encode, Decode, PartialEq, Debug)]
struct MeterSnapshot {
    total: i64,
    last_bump_at: u64,
}

impl Service for Meter {
    type Input = Cmd;
    type Output = Evt;
    type Snapshot = MeterSnapshot;
    type Config = ();

    fn genesis(_: &()) -> Self {
        Meter {
            total: 0,
            last_bump_at: Timestamp::ZERO,
        }
    }

    fn apply(&mut self, _seq: Seq, input: &Cmd, ctx: &mut Ctx<'_, Evt>) {
        match input {
            Cmd::Bump(n) => {
                self.total += n;
                self.last_bump_at = ctx.now();
                ctx.emit(Evt::At(ctx.now().as_nanos()));
            }
            Cmd::Zero => self.total = 0,
        }
        ctx.emit(Evt::Total(self.total));
    }

    fn snapshot(&self) -> MeterSnapshot {
        MeterSnapshot {
            total: self.total,
            last_bump_at: self.last_bump_at.as_nanos(),
        }
    }

    fn restore(snap: MeterSnapshot, _: &()) -> Self {
        Meter {
            total: snap.total,
            last_bump_at: Timestamp(snap.last_bump_at),
        }
    }
}

fn config(dir: &std::path::Path) -> NodeConfig {
    let mut config = NodeConfig::new(dir);
    config.journal.fsync = FsyncPolicy::EveryFrame;
    config
}

#[test]
fn sequences_and_emits() {
    let dir = tempfile::tempdir().unwrap();
    let mut node: Node<Meter, _> =
        Node::open_with_clock(config(dir.path()), (), ManualClock(Timestamp(100))).unwrap();

    let (seq, outs) = node.submit(Cmd::Bump(5)).unwrap();
    assert_eq!(seq, Seq(1));
    assert_eq!(outs, vec![Evt::At(100), Evt::Total(5)]);

    let (seq, outs) = node.submit(Cmd::Bump(-2)).unwrap();
    assert_eq!(seq, Seq(2));
    assert_eq!(outs, vec![Evt::At(100), Evt::Total(3)]);
    assert_eq!(node.service().total, 3);
}

#[test]
fn crash_recovery_rebuilds_identical_state() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut node: Node<Meter, _> =
            Node::open_with_clock(config(dir.path()), (), ManualClock(Timestamp(7))).unwrap();
        for i in 1..=20 {
            node.submit(Cmd::Bump(i)).unwrap();
        }
        node.submit(Cmd::Zero).unwrap();
        node.submit(Cmd::Bump(41)).unwrap();
        // No clean shutdown: drop simulates a crash (journal already fsynced
        // frame-by-frame).
    }
    let mut node: Node<Meter, _> =
        Node::open_with_clock(config(dir.path()), (), ManualClock(Timestamp(9))).unwrap();
    assert_eq!(node.seq(), Seq(22));
    assert_eq!(node.service().total, 41);
    assert_eq!(
        node.service().last_bump_at,
        Timestamp(7),
        "time must replay from the journal"
    );

    // New inputs continue the total order.
    let (seq, _) = node.submit(Cmd::Bump(1)).unwrap();
    assert_eq!(seq, Seq(23));
    assert_eq!(node.service().total, 42);
}

#[test]
fn replay_equivalence_holds() {
    let dir = tempfile::tempdir().unwrap();
    let mut node: Node<Meter, _> =
        Node::open_with_clock(config(dir.path()), (), ManualClock(Timestamp(50))).unwrap();
    for i in 0..100 {
        node.submit(if i % 7 == 0 { Cmd::Zero } else { Cmd::Bump(i) })
            .unwrap();
    }
    assert!(node.verify_replay().unwrap());
}

#[test]
fn ticks_advance_time_without_inputs() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut clock = ManualClock(Timestamp(10));
        clock.0 = Timestamp(10);
        let mut node: Node<Meter, _> =
            Node::open_with_clock(config(dir.path()), (), clock).unwrap();
        node.tick().unwrap(); // seq 1 at t=10
        node.submit(Cmd::Bump(1)).unwrap(); // seq 2
    }
    // Recovery must count the tick in the seq space.
    let node: Node<Meter, _> =
        Node::open_with_clock(config(dir.path()), (), ManualClock(Timestamp(99))).unwrap();
    assert_eq!(node.seq(), Seq(2));
    assert_eq!(node.service().total, 1);
}

#[test]
fn sequenced_time_never_goes_backwards() {
    let dir = tempfile::tempdir().unwrap();
    let mut node: Node<Meter, _> =
        Node::open_with_clock(config(dir.path()), (), ManualClock(Timestamp(100))).unwrap();
    node.submit(Cmd::Bump(1)).unwrap();
    // Wall clock steps backwards (NTP, VM migration...): sequenced time
    // must clamp, not regress.
    // Reopen-style access to the clock isn't exposed; use a fresh node on
    // the same journal with an earlier clock instead.
    drop(node);
    let mut node: Node<Meter, _> =
        Node::open_with_clock(config(dir.path()), (), ManualClock(Timestamp(30))).unwrap();
    node.submit(Cmd::Bump(2)).unwrap();
    assert_eq!(
        node.service().last_bump_at,
        Timestamp(100),
        "clamped to the journal's high-water timestamp"
    );
}

#[test]
fn bus_delivers_sequenced_frames_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut node: Node<Meter, _> =
        Node::open_with_clock(config(dir.path()), (), ManualClock(Timestamp(5))).unwrap();
    let rx = node.subscribe();
    node.submit(Cmd::Bump(1)).unwrap();
    node.tick().unwrap();
    node.submit(Cmd::Zero).unwrap();

    let frames: Vec<_> = rx.try_iter().collect();
    assert_eq!(frames.len(), 3);
    assert_eq!(frames[0].seq, Seq(1));
    assert_eq!(frames[0].kind, FrameKind::Input);
    assert_eq!(frames[1].kind, FrameKind::Tick);
    assert_eq!(frames[2].seq, Seq(3));
}

fn snap_config(dir: &std::path::Path, every: u64) -> NodeConfig {
    let mut config = config(dir);
    config.snapshot_every = Some(every);
    config
}

#[test]
fn snapshots_enable_fast_recovery() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut node: Node<Meter, _> =
            Node::open_with_clock(snap_config(dir.path(), 10), (), ManualClock(Timestamp(7)))
                .unwrap();
        for i in 1..=25 {
            node.submit(Cmd::Bump(i)).unwrap();
        }
        // 25 inputs + interleaved SnapshotMark frames; crash (drop).
    }
    let node: Node<Meter, _> =
        Node::open_with_clock(snap_config(dir.path(), 10), (), ManualClock(Timestamp(9))).unwrap();
    let info = node.recovery_info();
    let snap_seq = info.snapshot_seq.expect("must recover from a snapshot");
    assert!(snap_seq.as_u64() >= 20, "stale snapshot used: {info:?}");
    assert!(
        info.inputs_replayed < 25,
        "full replay despite snapshot: {info:?}"
    );
    // State identical to what full replay computes.
    assert_eq!(node.service().total, (1..=25).sum::<i64>());
    assert_eq!(node.service().last_bump_at, Timestamp(7));
}

#[test]
fn corrupt_snapshot_falls_back_and_state_is_still_right() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut node: Node<Meter, _> =
            Node::open_with_clock(snap_config(dir.path(), 5), (), ManualClock(Timestamp(3)))
                .unwrap();
        for i in 1..=12 {
            node.submit(Cmd::Bump(i)).unwrap();
        }
    }
    // Corrupt every snapshot file on disk.
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) == Some("snap") {
            let mut bytes = std::fs::read(&path).unwrap();
            let idx = bytes.len() / 2;
            bytes[idx] ^= 0xFF;
            std::fs::write(&path, &bytes).unwrap();
        }
    }
    let node: Node<Meter, _> =
        Node::open_with_clock(snap_config(dir.path(), 5), (), ManualClock(Timestamp(9))).unwrap();
    assert_eq!(
        node.recovery_info().snapshot_seq,
        None,
        "corrupt snapshots must be skipped"
    );
    assert_eq!(node.service().total, (1..=12).sum::<i64>());
}

#[test]
fn snapshot_marks_appear_in_the_stream() {
    let dir = tempfile::tempdir().unwrap();
    let mut node: Node<Meter, _> =
        Node::open_with_clock(snap_config(dir.path(), 3), (), ManualClock(Timestamp(1))).unwrap();
    let rx = node.subscribe();
    for i in 1..=3 {
        node.submit(Cmd::Bump(i)).unwrap();
    }
    let kinds: Vec<_> = rx.try_iter().map(|f| f.kind).collect();
    assert_eq!(
        kinds,
        vec![
            FrameKind::Input,
            FrameKind::Input,
            FrameKind::Input,
            FrameKind::SnapshotMark
        ],
        "mark must follow the frame that hit the cadence"
    );
    // verify_replay still holds with marks in the journal.
    let mut node = node;
    assert!(node.verify_replay().unwrap());
}
