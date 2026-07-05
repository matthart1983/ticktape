//! P0.2 Stage-B acceptance: **automatic** failover. Same 3-node loopback
//! deployment as `failover.rs`, but no operator ever calls `promote()` or
//! `set_leader_hint()`. The leader is killed; a standby's failure detector
//! notices the silence and stands for election on its own; the survivor
//! re-points at the new leader on its own. The properties are unchanged —
//! no committed loss, continuity — but the trigger is the deployment's own
//! main loop.
//!
//! Drives real UDP/TCP with real wall-clock timeouts (a liveness policy is
//! inherently timing-based); the timeouts are shrunk so the test is quick.

use std::net::{Ipv4Addr, TcpListener, UdpSocket};
use std::time::{Duration, Instant};

use ticktape_core::Service;
use ticktape_server::{ClusterConfig, PeerAddrs, Role, Server};
use ticktape_sim::demo::{gen_transfer, Bank};
use ticktape_sim::{Invariants, Rng};

/// Distinct addresses of each kind, allocated by holding all probe sockets
/// alive simultaneously (see `failover.rs` for the why). Timeouts are shrunk
/// to keep the test fast while leaving the heartbeat interval comfortably
/// below the failover timeout.
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
    let mut config = ClusterConfig::new(peers, 0xB0B);
    config.failover_timeout = Duration::from_millis(200);
    config.failover_stagger = Duration::from_millis(120);
    config.heartbeat_interval = Duration::from_millis(30);
    config
}

/// One iteration of a server's main loop: a leader advertises liveness; a
/// follower pumps the feed and, if it now suspects the leader, fails over
/// on its own. Exactly the loop a real deployment runs.
fn tick(server: &mut Server<Bank>) {
    match server.role() {
        Role::Leader => {
            let _ = server.heartbeat();
        }
        Role::Follower => {
            let _ = server.pump(Duration::from_millis(20));
            let _ = server.maybe_failover();
        }
    }
}

#[test]
fn standby_promotes_automatically_when_the_leader_dies() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let config = cluster(3);

    let mut s0: Server<Bank> =
        Server::open_follower(0, config.clone(), (), dirs[0].path()).unwrap();
    let mut s1: Server<Bank> =
        Server::open_follower(1, config.clone(), (), dirs[1].path()).unwrap();
    let mut s2: Server<Bank> =
        Server::open_follower(2, config.clone(), (), dirs[2].path()).unwrap();

    s0.start_as_leader().unwrap();

    // Replicate 100 transfers; drain both followers to the leader's tip.
    let mut rng = Rng::new(11);
    for _ in 0..100 {
        s0.submit(gen_transfer(&mut rng)).unwrap();
    }
    let leader_seq = s0.seq();
    let drain_deadline = Instant::now() + Duration::from_secs(10);
    while (s1.seq() < leader_seq || s2.seq() < leader_seq)
        && Instant::now() < drain_deadline
    {
        let _ = s1.pump(Duration::from_millis(20));
        let _ = s2.pump(Duration::from_millis(20));
    }
    let pre_state = s0.snapshot_bytes();
    assert_eq!(s1.snapshot_bytes(), pre_state, "follower 1 diverged pre-kill");
    assert_eq!(s2.snapshot_bytes(), pre_state, "follower 2 diverged pre-kill");

    // 💀 Kill the leader. No operator touches the survivors from here.
    drop(s0);

    // Phase 1: drive both survivors' main loops until one promotes itself.
    let deadline = Instant::now() + Duration::from_secs(10);
    let promoted = loop {
        assert!(
            Instant::now() < deadline,
            "no standby promoted itself within the deadline"
        );
        tick(&mut s1);
        tick(&mut s2);
        if s1.role() == Role::Leader {
            break 1;
        }
        if s2.role() == Role::Leader {
            break 2;
        }
    };

    // The lowest-indexed survivor (node 1, shortest staggered timeout) is
    // the expected winner; the epoch election would make either safe.
    assert_eq!(promoted, 1, "expected the lowest-indexed survivor to win");

    let (leader, follower): (&mut Server<Bank>, &mut Server<Bank>) = if promoted == 1 {
        (&mut s1, &mut s2)
    } else {
        (&mut s2, &mut s1)
    };

    // No committed loss: the self-promoted leader holds the exact
    // pre-failover state, at a bumped epoch.
    assert_eq!(
        leader.snapshot_bytes(),
        pre_state,
        "automatic promotion lost committed state"
    );
    assert!(leader.epoch() > 1, "promotion must bump the epoch");
    let restored = Bank::restore(
        ticktape_core::decode_all(&leader.snapshot_bytes()).unwrap(),
        &(),
    );
    restored
        .check()
        .expect("conservation invariant after automatic failover");

    // Continuity: the new leader keeps sequencing, and the surviving
    // follower re-points at it and catches up — all on its own (the loop
    // never calls set_leader_hint).
    for _ in 0..50 {
        leader.submit(gen_transfer(&mut rng)).unwrap();
    }
    let target = leader.seq();
    let conv_deadline = Instant::now() + Duration::from_secs(10);
    while follower.seq() < target && Instant::now() < conv_deadline {
        tick(leader);
        tick(follower);
    }
    assert_eq!(
        follower.snapshot_bytes(),
        leader.snapshot_bytes(),
        "surviving follower did not re-converge on the new leader automatically"
    );
    // And it stayed a follower — it found the leader rather than dueling
    // forever.
    assert_eq!(follower.role(), Role::Follower);
}

#[test]
fn a_quiet_leader_is_not_mistaken_for_a_dead_one() {
    // The detector must treat heartbeats as liveness: a leader with no
    // inputs to sequence still sends heartbeats, and a follower fed only
    // those must never stand for election.
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let config = cluster(3);
    let mut s0: Server<Bank> =
        Server::open_follower(0, config.clone(), (), dirs[0].path()).unwrap();
    let mut s1: Server<Bank> =
        Server::open_follower(1, config.clone(), (), dirs[1].path()).unwrap();
    let mut s2: Server<Bank> =
        Server::open_follower(2, config.clone(), (), dirs[2].path()).unwrap();
    s0.start_as_leader().unwrap();

    // No submits at all — only heartbeats. Drive well past the failover
    // timeout; the followers must stay followers.
    let deadline = Instant::now() + Duration::from_millis(900);
    while Instant::now() < deadline {
        let _ = s0.heartbeat();
        tick(&mut s1);
        tick(&mut s2);
    }
    assert_eq!(s1.role(), Role::Follower, "idle leader triggered a false failover");
    assert_eq!(s2.role(), Role::Follower, "idle leader triggered a false failover");
    assert!(!s1.leader_suspected());
    assert!(!s2.leader_suspected());
}
