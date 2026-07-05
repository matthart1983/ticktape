//! The admin/observability plane over a live deployment: stats reflect role
//! and replication lag, the Prometheus endpoint serves a real server, and
//! the picture updates across an operator-driven failover — the exact
//! workflow of operating the manual-failover deployment.

use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream, UdpSocket};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ticktape_server::{bind_metrics, serve_metrics, ClusterConfig, PeerAddrs, Server};
use ticktape_sim::demo::{gen_transfer, Bank};
use ticktape_sim::Rng;

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
    ClusterConfig::new(peers, 0xF33D)
}

#[test]
fn stats_reflect_role_lag_and_survive_failover() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let config = cluster(3);
    let mut s0: Server<Bank> =
        Server::open_follower(0, config.clone(), (), dirs[0].path()).unwrap();
    let mut s1: Server<Bank> =
        Server::open_follower(1, config.clone(), (), dirs[1].path()).unwrap();
    let mut s2: Server<Bank> =
        Server::open_follower(2, config.clone(), (), dirs[2].path()).unwrap();

    s0.start_as_leader().unwrap();
    let mut rng = Rng::new(3);
    for _ in 0..60 {
        s0.submit(gen_transfer(&mut rng)).unwrap();
    }

    // Before followers pump: leader is ahead, so a pumped follower reports lag.
    let ls = s0.stats();
    assert_eq!(ls.role, "leader");
    assert_eq!(ls.lag, 0);
    assert!(ls.seq >= 60);

    // Drain followers; they catch up and lag falls to zero.
    let target = s0.seq();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while (s1.seq() < target || s2.seq() < target) && std::time::Instant::now() < deadline {
        s1.pump(Duration::from_millis(40)).unwrap();
        s2.pump(Duration::from_millis(40)).unwrap();
    }
    let f1 = s1.stats();
    assert_eq!(f1.role, "follower");
    assert_eq!(f1.node, 1);
    assert_eq!(f1.lag, 0, "caught-up follower should report zero lag");
    assert_eq!(f1.seq, ls.seq);

    // The metrics endpoint serves a live server (leader).
    let stats_src = Arc::new(Mutex::new(s0.stats()));
    let src = stats_src.clone();
    let (listener, addr) = bind_metrics((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    std::thread::spawn(move || serve_metrics(listener, move || src.lock().unwrap().clone()));
    let mut conn = TcpStream::connect(addr).unwrap();
    conn.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
        .unwrap();
    let mut body = String::new();
    conn.read_to_string(&mut body).unwrap();
    assert!(
        body.contains("ticktape_role{node=\"0\"} 1"),
        "leader role metric: {body}"
    );
    assert!(body.contains(&format!("ticktape_seq{{node=\"0\"}} {}", ls.seq)));

    // Failover: kill the leader, promote follower 1, stats reflect the new
    // leader with a bumped epoch.
    drop(s0);
    s1.promote().unwrap();
    let after = s1.stats();
    assert_eq!(after.role, "leader");
    assert_eq!(after.node, 1);
    assert!(after.epoch > ls.epoch, "epoch must advance on promotion");
    assert_eq!(after.lag, 0);
}
