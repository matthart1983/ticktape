//! The P0.2 Stage-A acceptance test: a 3-node replicated deployment over
//! real loopback sockets, an operator-driven failover, and the two
//! properties that matter — **no committed loss** and **continuity**
//! (a promoted follower resumes as leader with the exact replicated state
//! and can keep sequencing).
//!
//! Drives the deployment synchronously (submit on the leader, pump on the
//! followers) so the test is deterministic despite real UDP/TCP.

use std::net::{Ipv4Addr, TcpListener, UdpSocket};
use std::time::Duration;

use ticktape_core::Service;
use ticktape_server::{ClusterConfig, PeerAddrs, Role, Server};
use ticktape_sim::demo::{gen_transfer, Bank};
use ticktape_sim::{Invariants, Rng};

/// Allocate `n` distinct addresses of each kind by binding all probe
/// sockets *simultaneously* (holding them alive), reading their addresses,
/// then dropping them together — otherwise the OS reuses a just-freed
/// ephemeral port and two nodes collide on one address.
fn cluster(n: usize) -> ClusterConfig {
    let tcp: Vec<TcpListener> = (0..2 * n)
        .map(|_| TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap())
        .collect();
    let udp: Vec<UdpSocket> = (0..n)
        .map(|_| UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap())
        .collect();
    let peers = (0..n)
        .map(|i| PeerAddrs {
            acceptor: tcp[i].local_addr().unwrap(),
            retx: tcp[n + i].local_addr().unwrap(),
            feed: udp[i].local_addr().unwrap(),
        })
        .collect();
    // Sockets drop here, freeing the distinct ports for the servers to bind.
    ClusterConfig::new(peers, 0xF33D)
}

fn bank_state(server: &Server<Bank>) -> Vec<u8> {
    server.snapshot_bytes()
}

/// Pump the followers until they've caught up to `target` (or a deadline).
fn drain_followers(followers: &mut [&mut Server<Bank>], target: ticktape_core::Seq) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut all_caught = true;
        for f in followers.iter_mut() {
            f.pump(Duration::from_millis(50)).unwrap();
            if f.seq() < target {
                all_caught = false;
            }
        }
        if all_caught || std::time::Instant::now() >= deadline {
            break;
        }
    }
}

#[test]
fn operator_promoted_failover_preserves_state_and_continues() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let config = cluster(3);

    // Three followers; node 0 becomes the initial leader.
    let mut s0: Server<Bank> =
        Server::open_follower(0, config.clone(), (), dirs[0].path()).unwrap();
    let mut s1: Server<Bank> =
        Server::open_follower(1, config.clone(), (), dirs[1].path()).unwrap();
    let mut s2: Server<Bank> =
        Server::open_follower(2, config.clone(), (), dirs[2].path()).unwrap();

    s0.start_as_leader().unwrap();
    assert_eq!(s0.role(), Role::Leader);

    // Sequence 100 transfers on the leader; followers replicate them.
    let mut rng = Rng::new(7);
    for _ in 0..100 {
        s0.submit(gen_transfer(&mut rng)).unwrap();
    }
    let leader_seq = s0.seq();
    drain_followers(&mut [&mut s1, &mut s2], leader_seq);

    // Everyone converged to the leader's state (Bank conserves money).
    let leader_state = bank_state(&s0);
    assert_eq!(bank_state(&s1), leader_state, "follower 1 diverged");
    assert_eq!(bank_state(&s2), leader_state, "follower 2 diverged");

    // 💀 The leader dies. The operator promotes follower 1.
    drop(s0);
    s1.promote()
        .expect("follower 1 should win with 2/3 majority");
    assert_eq!(s1.role(), Role::Leader);
    assert!(s1.epoch() > 1, "promotion must bump the epoch");

    // No committed loss: the promoted leader holds the exact pre-failover
    // state (money conserved, same balances).
    let promoted_state = bank_state(&s1);
    // Strip the fence frame's effect: fencing is a control frame, so state
    // is unchanged — the promoted leader's Bank equals the old leader's.
    assert_eq!(
        promoted_state, leader_state,
        "promotion lost committed state"
    );
    // Reconstruct a Bank from the snapshot and check the invariant.
    let restored = Bank::restore(ticktape_core::decode_all(&promoted_state).unwrap(), &());
    restored
        .check()
        .expect("conservation invariant after failover");

    // Continuity: the new leader keeps sequencing, and the surviving
    // follower keeps replicating from it — after the operator points it at
    // the new leader (node 1) for gap-fill.
    for _ in 0..50 {
        s1.submit(gen_transfer(&mut rng)).unwrap();
    }
    let new_leader_seq = s1.seq();
    // Point follower 2 at the new leader and drain.
    point_at_new_leader(&mut s2, 1);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while s2.seq() < new_leader_seq && std::time::Instant::now() < deadline {
        s2.pump(Duration::from_millis(50)).unwrap();
    }
    assert_eq!(
        bank_state(&s2),
        bank_state(&s1),
        "follower 2 did not track the promoted leader"
    );
}

/// Point a follower's gap-fill at a new leader index (Stage-A operator step;
/// Stage B's failure detector maintains this automatically).
fn point_at_new_leader(server: &mut Server<Bank>, leader_idx: usize) {
    server.set_leader_hint(leader_idx);
}

#[test]
fn promotion_without_majority_is_refused() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let config = cluster(3);
    // Only one server exists; its two peers are unreachable.
    let mut lone: Server<Bank> =
        Server::open_follower(0, config.clone(), (), dirs[0].path()).unwrap();
    // No leader, no peers up — promotion can't reach a majority.
    let _ = &dirs; // keep dirs alive
    assert!(
        lone.promote().is_err(),
        "promotion must fail without a majority"
    );
}
