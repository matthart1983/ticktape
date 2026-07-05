//! Deterministic timers end-to-end through the Node (P1.4): firing on time
//! advance, replay-after-recovery equivalence, snapshot survival, and
//! cancellation — all via journaled `TimerFired` frames.

use ticktape_codec::{Decode, Encode};
use ticktape_core::{Ctx, FrameKind, Seq, Service, Timestamp};
use ticktape_journal::FsyncPolicy;
use ticktape_runtime::{ManualClock, Node, NodeConfig};

/// A service that arms/cancels timers on command and records every timer
/// that fires (in order). Timer firings emit `Fired(id)`; commands emit
/// nothing, so the output stream is exactly the firing history.
#[derive(Encode, Decode, PartialEq, Debug)]
enum Cmd {
    Arm {
        id: u64,
        at: u64,
    },
    Cancel {
        id: u64,
    },
    /// Arm `id` to fire at `at`, and when it fires, re-arm it once at `again`.
    ArmRepeating {
        id: u64,
        at: u64,
        again: u64,
    },
}

#[derive(Encode, Decode, PartialEq, Debug)]
struct Fired(u64);

#[derive(Encode, Decode, PartialEq, Debug, Default)]
struct Snap {
    fired: Vec<u64>,
    // repeating timers still awaiting their re-arm: id -> next deadline
    repeat: Vec<(u64, u64)>,
}

// Full state needs the repeating map too; keep it on the struct.
#[derive(Default)]
struct TimersFull {
    fired: Vec<u64>,
    repeat: std::collections::BTreeMap<u64, u64>,
}

impl Service for TimersFull {
    type Input = Cmd;
    type Output = Fired;
    type Snapshot = Snap;
    type Config = ();

    fn genesis(_: &()) -> Self {
        TimersFull::default()
    }

    fn apply(&mut self, _seq: Seq, input: &Cmd, ctx: &mut Ctx<'_, Fired>) {
        match *input {
            Cmd::Arm { id, at } => ctx.set_timer(id, Timestamp(at)),
            Cmd::Cancel { id } => ctx.cancel_timer(id),
            Cmd::ArmRepeating { id, at, again } => {
                self.repeat.insert(id, again);
                ctx.set_timer(id, Timestamp(at));
            }
        }
    }

    fn on_timer(&mut self, id: u64, ctx: &mut Ctx<'_, Fired>) {
        self.fired.push(id);
        ctx.emit(Fired(id));
        // A repeating timer re-arms itself once, from inside the handler.
        if let Some(again) = self.repeat.remove(&id) {
            ctx.set_timer(id, Timestamp(again));
        }
    }

    fn snapshot(&self) -> Snap {
        Snap {
            fired: self.fired.clone(),
            repeat: self.repeat.iter().map(|(&k, &v)| (k, v)).collect(),
        }
    }

    fn restore(snap: Snap, _: &()) -> Self {
        TimersFull {
            fired: snap.fired,
            repeat: snap.repeat.into_iter().collect(),
        }
    }
}

fn config(dir: &std::path::Path) -> NodeConfig {
    let mut c = NodeConfig::new(dir);
    c.journal.fsync = FsyncPolicy::EveryFrame;
    c
}

#[test]
fn timer_fires_when_sequenced_time_reaches_its_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock(Timestamp(100));
    let mut node: Node<TimersFull, _> =
        Node::open_with_clock(config(dir.path()), (), clock).unwrap();

    // Arm a timer for t=500. Nothing fires yet (clock at 100).
    let (_seq, out) = node.submit(Cmd::Arm { id: 42, at: 500 }).unwrap();
    assert!(out.is_empty(), "timer fired before its deadline");
    assert_eq!(node.pending_timer_count(), 1);

    // Advance time past the deadline via a submit stamped at t=600; the
    // timer fires as part of that step and its output rides back.
    node.clock_mut().0 = Timestamp(600);
    let (_seq, out) = node.submit(Cmd::Arm { id: 99, at: 10_000 }).unwrap();
    assert_eq!(
        out,
        vec![Fired(42)],
        "deadline-crossing did not fire timer 42"
    );
    assert_eq!(node.pending_timer_count(), 1, "only timer 99 remains armed");
    assert_eq!(node.service().fired, vec![42]);
}

#[test]
fn a_tick_alone_fires_due_timers() {
    let dir = tempfile::tempdir().unwrap();
    let mut node: Node<TimersFull, _> =
        Node::open_with_clock(config(dir.path()), (), ManualClock(Timestamp(1))).unwrap();
    node.submit(Cmd::Arm { id: 1, at: 50 }).unwrap();
    node.submit(Cmd::Arm { id: 2, at: 50 }).unwrap();
    node.submit(Cmd::Arm { id: 3, at: 90 }).unwrap();

    // A tick at t=50 fires 1 and 2 (id order), not 3.
    node.clock_mut().0 = Timestamp(50);
    node.tick().unwrap();
    assert_eq!(node.service().fired, vec![1, 2]);

    node.clock_mut().0 = Timestamp(90);
    node.tick().unwrap();
    assert_eq!(node.service().fired, vec![1, 2, 3]);
}

#[test]
fn cancel_prevents_firing() {
    let dir = tempfile::tempdir().unwrap();
    let mut node: Node<TimersFull, _> =
        Node::open_with_clock(config(dir.path()), (), ManualClock(Timestamp(1))).unwrap();
    node.submit(Cmd::Arm { id: 7, at: 100 }).unwrap();
    node.submit(Cmd::Cancel { id: 7 }).unwrap();
    assert_eq!(node.pending_timer_count(), 0);
    node.clock_mut().0 = Timestamp(1_000);
    node.tick().unwrap();
    assert!(
        node.service().fired.is_empty(),
        "cancelled timer still fired"
    );
}

#[test]
fn firing_is_journaled_and_replays_identically() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut node: Node<TimersFull, _> =
            Node::open_with_clock(config(dir.path()), (), ManualClock(Timestamp(1))).unwrap();
        node.submit(Cmd::Arm { id: 10, at: 200 }).unwrap();
        node.submit(Cmd::Arm { id: 20, at: 300 }).unwrap();
        node.clock_mut().0 = Timestamp(250);
        node.tick().unwrap(); // fires 10
        node.clock_mut().0 = Timestamp(350);
        node.tick().unwrap(); // fires 20
        assert_eq!(node.service().fired, vec![10, 20]);
        // A replay of the on-disk journal must reproduce the exact state.
        assert!(node.verify_replay().unwrap(), "replay diverged from live");
    }
    // Reopen from the journal: recovery replays the journaled TimerFired
    // frames, so the recovered service holds the identical firing history.
    let node: Node<TimersFull, _> =
        Node::open_with_clock(config(dir.path()), (), ManualClock(Timestamp(400))).unwrap();
    assert_eq!(
        node.service().fired,
        vec![10, 20],
        "recovery lost timer firings"
    );
    assert_eq!(node.pending_timer_count(), 0);
}

#[test]
fn timers_armed_before_a_snapshot_still_fire_after_recovery() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut c = config(dir.path());
        c.snapshot_every = Some(2); // snapshot frequently
        let mut node: Node<TimersFull, _> =
            Node::open_with_clock(c, (), ManualClock(Timestamp(1))).unwrap();
        // Arm a far-future timer, then submit enough to trigger snapshots so
        // the timer's arming frame is compacted below the snapshot.
        node.submit(Cmd::Arm { id: 55, at: 9_000 }).unwrap();
        for i in 0..10 {
            node.submit(Cmd::Arm {
                id: 1000 + i,
                at: 8_000,
            })
            .unwrap();
        }
        node.sync().unwrap();
        // The wheel is part of the snapshot; timer 55 is still armed.
        assert!(node.pending_timer_count() >= 1);
    }
    // Recover (from a snapshot that post-dates timer 55's arming), then fire.
    let mut node: Node<TimersFull, _> =
        Node::open_with_clock(config(dir.path()), (), ManualClock(Timestamp(9_500))).unwrap();
    // Timer 55 survived in the snapshot's wheel; a tick past 9000 fires it.
    node.tick().unwrap();
    assert!(
        node.service().fired.contains(&55),
        "a timer armed before the snapshot was lost across recovery"
    );
}

#[test]
fn repeating_timer_rearms_from_within_its_handler() {
    let dir = tempfile::tempdir().unwrap();
    let mut node: Node<TimersFull, _> =
        Node::open_with_clock(config(dir.path()), (), ManualClock(Timestamp(1))).unwrap();
    node.submit(Cmd::ArmRepeating {
        id: 5,
        at: 100,
        again: 200,
    })
    .unwrap();
    node.clock_mut().0 = Timestamp(150);
    node.tick().unwrap(); // fires at 100, re-arms for 200
    assert_eq!(node.service().fired, vec![5]);
    assert_eq!(node.pending_timer_count(), 1, "did not re-arm");
    node.clock_mut().0 = Timestamp(250);
    node.tick().unwrap(); // fires the re-armed instance
    assert_eq!(node.service().fired, vec![5, 5]);
    assert_eq!(
        node.pending_timer_count(),
        0,
        "re-armed timer should not repeat again"
    );

    // And this whole re-arm dance replays deterministically.
    assert!(node.verify_replay().unwrap());
}

#[test]
fn timer_fired_frames_carry_the_trigger_time() {
    // A burst of same-deadline timers must all fire at that deadline without
    // advancing sequenced time (else the loop wouldn't terminate).
    let dir = tempfile::tempdir().unwrap();
    let mut node: Node<TimersFull, _> =
        Node::open_with_clock(config(dir.path()), (), ManualClock(Timestamp(1))).unwrap();
    let sub = node.subscribe();
    for id in 0..5 {
        node.submit(Cmd::Arm { id, at: 100 }).unwrap();
    }
    node.clock_mut().0 = Timestamp(100);
    node.tick().unwrap();
    assert_eq!(node.service().fired, vec![0, 1, 2, 3, 4]);
    // Every TimerFired frame is stamped at t=100 (the trigger), not later.
    let fired_stamps: Vec<u64> = sub
        .try_iter()
        .filter(|f| f.kind == FrameKind::TimerFired)
        .map(|f| f.timestamp.as_nanos())
        .collect();
    assert_eq!(fired_stamps, vec![100, 100, 100, 100, 100]);
}
