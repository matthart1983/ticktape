//! The M2 acceptance test: the order book survives seeded fault injection —
//! power-loss crashes, torn tails, snapshot/restore recovery — with
//! exchange-grade invariants (no crossed book, share conservation) checked
//! after every input and every recovery.

use orderbook::{gen_cmd, Cmd, OrderBook, Side};
use ticktape::{FsyncPolicy, Node, NodeConfig};
use ticktape_sim::{vopr, SimConfig};

#[test]
fn orderbook_survives_seeded_fault_injection() {
    let base = SimConfig::new(0); // snapshots on by default (every 20 frames)
    let stats = vopr::<OrderBook>(&base, 0..40, (), gen_cmd)
        .unwrap_or_else(|failure| panic!("order book failed under faults: {failure}"));
    assert!(stats.inputs > 5_000, "fuzz too shallow: {stats:?}");
    assert!(stats.crashes > 40, "fuzz too gentle: {stats:?}");
}

#[test]
fn orderbook_crash_recovery_via_snapshot_on_real_fs() {
    let dir = tempfile::tempdir().unwrap();
    let config = || {
        let mut c = NodeConfig::new(dir.path());
        c.journal.fsync = FsyncPolicy::EveryFrame;
        c.snapshot_every = Some(25);
        c
    };
    {
        let mut node: Node<OrderBook> = Node::open(config(), ()).unwrap();
        for i in 0..120u64 {
            let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
            node.submit(Cmd::Submit {
                id: i,
                side,
                price: 95 + (i % 11) as u32,
                qty: 1 + (i % 9) as u32,
            })
            .unwrap();
        }
        // Crash (drop without clean shutdown).
    }
    let mut node: Node<OrderBook> = Node::open(config(), ()).unwrap();
    let info = node.recovery_info();
    assert!(info.snapshot_seq.is_some(), "must recover via snapshot");
    assert!(info.inputs_replayed < 120, "must not replay from genesis");
    use ticktape_sim::Invariants;
    node.service().check().unwrap();
    assert!(
        node.verify_replay().unwrap(),
        "snapshot recovery must equal full replay"
    );
}
