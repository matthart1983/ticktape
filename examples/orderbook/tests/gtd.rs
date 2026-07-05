//! Good-till-date orders under the deterministic simulator (P1.4 done-
//! criterion): GTD orders expire *identically* on the three paths the spec
//! names — **live** (the running node), **replay** (crash recovery re-runs
//! the journaled `TimerFired` frames), and **replica** (a follower applying
//! the same stream) — and their pending-timer state survives a
//! snapshot-based recovery, all under the fault-injecting `SimStorage`.

use orderbook::{Cmd, Evt, OrderBook, Side};
use ticktape::{Journal, JournalConfig, Node, NodeConfig, Replica, Seq, Service};
use ticktape_sim::{Invariants, Rng, SimClock, SimStorage};

type SimNode = Node<OrderBook, SimClock, SimStorage>;

const DIR: &str = "/gtd/journal";

fn open(storage: &SimStorage, clock: &SimClock, snapshot_every: Option<u64>) -> SimNode {
    let mut c = NodeConfig::new(DIR);
    c.snapshot_every = snapshot_every;
    Node::open_with(c, (), clock.clone(), storage.clone()).unwrap()
}

/// The **replica path**: rebuild an independent `Replica` by applying every
/// journaled frame (Input *and* TimerFired) from genesis, and return its
/// state bytes. Valid only when the journal was never compacted (snapshots
/// off), so the replayable range starts at genesis — which is how a
/// stream-follower that joined at the start sees the world.
fn replica_state_from_genesis(storage: &SimStorage) -> Vec<u8> {
    let re = Journal::open_with(JournalConfig::new(DIR), storage.clone()).unwrap();
    assert_eq!(
        re.first_seq,
        Seq(1),
        "replica-path check needs an uncompacted journal"
    );
    let mut replica: Replica<OrderBook> = Replica::new(&());
    for frame in &re.frames {
        replica.apply(frame).expect("replica applies in order");
    }
    ticktape::encode_to_vec(&replica.service().snapshot())
}

#[test]
fn gtd_expiry_is_identical_on_live_replay_and_replica_paths() {
    // Snapshots OFF: the journal keeps every frame from genesis, so the
    // replica path can replay the whole stream.
    let storage = SimStorage::new();
    let clock = SimClock::default();
    clock.advance(1_000);
    let mut node = open(&storage, &clock, None);

    // Three GTD sells at distinct prices (no bids ⇒ all rest), staggered
    // expiries, plus a plain never-expiring sell.
    for (id, price, exp) in [(1u64, 100u32, 2_000u64), (2, 101, 3_000), (3, 102, 9_000)] {
        node.submit(Cmd::SubmitGtd {
            id,
            side: Side::Sell,
            price,
            qty: 5,
            expire_at: exp,
        })
        .unwrap();
    }
    node.submit(Cmd::Submit {
        id: 4,
        side: Side::Sell,
        price: 103,
        qty: 5,
    })
    .unwrap();
    assert_eq!(node.pending_timer_count(), 3);

    // t=2_500: order 1 expires.
    clock.advance(1_500);
    node.tick().unwrap();
    assert!(!node.service().has_order(1) && node.service().has_order(2));

    // Cancel order 2 → its timer disarms (no phantom expiry later).
    node.submit(Cmd::Cancel { id: 2 }).unwrap();
    assert_eq!(node.pending_timer_count(), 1);

    // t past 9_000: order 3 expires; order 4 (plain) never does.
    clock.advance(10_000);
    node.tick().unwrap();
    assert!(!node.service().has_order(3) && node.service().has_order(4));
    assert_eq!(node.pending_timer_count(), 0);

    // Live invariants.
    node.service().check().unwrap();
    // Replay path: recovery re-runs the journaled TimerFired frames.
    assert!(node.verify_replay().unwrap(), "replay path diverged");
    // Replica path: an independent follower of the same stream agrees.
    let live = ticktape::encode_to_vec(&node.service().snapshot());
    assert_eq!(
        replica_state_from_genesis(&storage),
        live,
        "replica path diverged"
    );
    // Only the plain order survived.
    assert_eq!(node.service().resting_orders(), 1);
}

#[test]
fn gtd_timer_state_survives_snapshot_based_recovery_under_crashes() {
    // Snapshots ON and frequent: GTD arming frames get compacted below a
    // snapshot, so pending timers must be restored *from the snapshot's
    // serialized wheel*, not from journal replay.
    let storage = SimStorage::new();
    let clock = SimClock::default();
    clock.advance(1_000);
    let mut node = open(&storage, &clock, Some(6));

    // Arm a far-future GTD order, then churn enough orders to drive several
    // snapshots (and journal compaction) past its arming frame.
    node.submit(Cmd::SubmitGtd {
        id: 500,
        side: Side::Sell,
        price: 200,
        qty: 9,
        expire_at: 100_000,
    })
    .unwrap();
    for i in 0..30u64 {
        // Buys well below the ask so they rest then get cancelled — churn
        // that advances the snapshot cadence without touching order 500.
        node.submit(Cmd::Submit {
            id: 1_000 + i,
            side: Side::Buy,
            price: 100,
            qty: 1,
        })
        .unwrap();
        node.submit(Cmd::Cancel { id: 1_000 + i }).unwrap();
    }
    node.sync().unwrap();
    assert!(node.pending_timer_count() >= 1, "order 500 still armed");

    // 💀 Crash and recover; the wheel (hence order 500's timer) must come back
    // from the snapshot, since its arming frame was compacted away.
    storage.crash(&mut Rng::new(7), false);
    let mut node = open(&storage, &clock, Some(6));
    assert!(
        node.service().has_order(500),
        "GTD order lost across recovery"
    );
    assert_eq!(
        node.pending_timer_count(),
        1,
        "GTD timer must survive snapshot-based recovery"
    );
    node.service().check().unwrap();
    assert!(node.verify_replay().unwrap());

    // Now advance past its deadline: it expires, identically post-recovery.
    clock.advance(200_000);
    node.tick().unwrap();
    assert!(
        !node.service().has_order(500),
        "GTD order should have expired after recovery"
    );
    assert_eq!(node.pending_timer_count(), 0);

    // Final crash: the *synced* expiry is durable and replays (a torn crash
    // only mangles unsynced tails).
    node.sync().unwrap();
    storage.crash(&mut Rng::new(8), true);
    let mut node = open(&storage, &clock, Some(6));
    assert!(!node.service().has_order(500));
    node.service().check().unwrap();
    assert!(node.verify_replay().unwrap());
}

#[test]
fn a_gtd_order_that_fills_before_expiry_never_fires_its_timer() {
    let storage = SimStorage::new();
    let clock = SimClock::default();
    clock.advance(1_000);
    let mut node = open(&storage, &clock, None);

    node.submit(Cmd::SubmitGtd {
        id: 10,
        side: Side::Buy,
        price: 100,
        qty: 5,
        expire_at: 5_000,
    })
    .unwrap();
    assert_eq!(node.pending_timer_count(), 1);
    let (_seq, evts) = node
        .submit(Cmd::Submit {
            id: 11,
            side: Side::Sell,
            price: 100,
            qty: 5,
        })
        .unwrap();
    assert!(
        evts.iter().any(|e| matches!(e, Evt::Trade { .. })),
        "should trade"
    );
    assert!(!node.service().has_order(10));
    assert_eq!(
        node.pending_timer_count(),
        0,
        "a filled GTD order must disarm its expiry timer"
    );

    // Past the old deadline: no phantom expiry.
    clock.advance(10_000);
    node.tick().unwrap();
    node.service().check().unwrap();
    assert!(node.verify_replay().unwrap());
    assert_eq!(node.service().resting_orders(), 0);
    assert_eq!(
        replica_state_from_genesis(&storage),
        ticktape::encode_to_vec(&node.service().snapshot())
    );
}
