//! Real-socket integration: A/B redundancy over UDP loopback, gap-fill
//! from the TCP retransmitter, and the end-to-end story — a leader Node
//! feeding a follower Replica that computes bit-identical state.

use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::time::Duration;
use ticktape_core::Timestamp;
use ticktape_core::{encode_to_vec, Seq, Service};
use ticktape_runtime::{ManualClock, Node, NodeConfig};
use ticktape_sim::demo::{gen_transfer, Bank};
use ticktape_sim::Rng;
use ticktape_transport::{
    bind_udp, MemStore, Publisher, PublisherConfig, Receiver, ReceiverConfig, Replica,
    Retransmitter,
};

const SESSION: u64 = 0x5EED;

fn localhost(socket: &UdpSocket) -> SocketAddr {
    socket.local_addr().unwrap()
}

fn drain<S: PacketSourceExt>(receiver: &mut Receiver<S>, until: u64) -> Vec<u64> {
    let mut seqs = Vec::new();
    while seqs.last().copied() != Some(until) {
        match receiver.poll(Duration::from_secs(5)).unwrap() {
            Some(frame) => seqs.push(frame.seq.0),
            None => panic!("timed out at {:?}", seqs.last()),
        }
    }
    seqs
}

// Local alias so `drain` is generic without repeating the bound.
trait PacketSourceExt: ticktape_transport::PacketSource {}
impl<T: ticktape_transport::PacketSource> PacketSourceExt for T {}

fn spawn_retransmitter(store: MemStore) -> SocketAddr {
    let (retransmitter, addr) =
        Retransmitter::bind((Ipv4Addr::LOCALHOST, 0).into(), SESSION, store).unwrap();
    std::thread::spawn(move || retransmitter.serve_forever());
    addr
}

fn frame(seq: u64) -> ticktape_core::Frame {
    ticktape_core::Frame::new(
        Seq(seq),
        Timestamp(seq),
        1,
        ticktape_core::FrameKind::Input,
        encode_to_vec(&(seq as u32)),
    )
}

#[test]
fn ab_redundancy_covers_single_channel_loss() {
    // Receiver binds two sockets; the "publisher" sends odd frames only on
    // A and even frames only on B — total single-channel loss both ways.
    let sock_a = bind_udp((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let sock_b = bind_udp((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let (addr_a, addr_b) = (localhost(&sock_a), localhost(&sock_b));

    let sender = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    for seq in 1..=50u64 {
        let packet = ticktape_transport::wire::Packet::Data {
            session: SESSION,
            frames: vec![frame(seq)],
        };
        let dest = if seq % 2 == 1 { addr_a } else { addr_b };
        sender.send_to(&packet.encode(), dest).unwrap();
    }

    let mut receiver = Receiver::new(
        sock_a,
        Some(sock_b),
        ReceiverConfig {
            from: Seq(1),
            retransmitter: None,
        },
    );
    assert_eq!(drain(&mut receiver, 50), (1..=50).collect::<Vec<_>>());
}

#[test]
fn gap_fill_recovers_frames_lost_on_both_channels() {
    let sock_a = bind_udp((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let addr_a = localhost(&sock_a);

    // The store has everything; the wire drops seqs 10..=19 entirely.
    let store = MemStore::new();
    for seq in 1..=60u64 {
        store.record(frame(seq));
    }
    let retransmitter = spawn_retransmitter(store);

    let sender = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    for seq in (1..=60u64).filter(|s| !(10..=19).contains(s)) {
        let packet = ticktape_transport::wire::Packet::Data {
            session: SESSION,
            frames: vec![frame(seq)],
        };
        sender.send_to(&packet.encode(), addr_a).unwrap();
    }

    let mut receiver = Receiver::new(
        sock_a,
        None,
        ReceiverConfig {
            from: Seq(1),
            retransmitter: Some(retransmitter),
        },
    );
    assert_eq!(drain(&mut receiver, 60), (1..=60).collect::<Vec<_>>());
}

#[test]
fn late_joiner_catches_up_from_retransmitter() {
    let sock_a = bind_udp((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let addr_a = localhost(&sock_a);

    let store = MemStore::new();
    for seq in 1..=100u64 {
        store.record(frame(seq));
    }
    let retransmitter = spawn_retransmitter(store);

    // Joiner only ever sees live traffic from seq 95.
    let sender = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    for seq in 95..=100u64 {
        let packet = ticktape_transport::wire::Packet::Data {
            session: SESSION,
            frames: vec![frame(seq)],
        };
        sender.send_to(&packet.encode(), addr_a).unwrap();
    }

    let mut receiver = Receiver::new(
        sock_a,
        None,
        ReceiverConfig {
            from: Seq(1),
            retransmitter: Some(retransmitter),
        },
    );
    assert_eq!(drain(&mut receiver, 100), (1..=100).collect::<Vec<_>>());
}

/// The M3 acceptance: leader Node → UDP A/B + retransmitter → follower
/// Replica, ending in bit-identical state at the same seq.
#[test]
fn follower_replica_reaches_bit_identical_state() {
    let dir = tempfile::tempdir().unwrap();

    // Follower's sockets.
    let sock_a = bind_udp((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let sock_b = bind_udp((Ipv4Addr::LOCALHOST, 0).into()).unwrap();

    // Leader: journal-backed Bank node, publishing everything it sequences.
    let store = MemStore::new();
    let retransmitter = spawn_retransmitter(store.clone());
    let mut publisher = Publisher::new(PublisherConfig {
        session: SESSION,
        dest_a: localhost(&sock_a),
        dest_b: Some(localhost(&sock_b)),
    })
    .unwrap();

    let mut node: Node<Bank, _> = Node::open_with_clock(
        NodeConfig::new(dir.path()),
        (),
        ManualClock(Timestamp(1_000)),
    )
    .unwrap();
    let stream = node.subscribe();

    let mut rng = Rng::new(42);
    for _ in 0..300 {
        node.submit(gen_transfer(&mut rng)).unwrap();
    }
    for sequenced in stream.try_iter() {
        store.record(sequenced.clone());
        // Simulate real loss: drop ~10% of publishes entirely; the
        // retransmitter covers them.
        if sequenced.seq.0 % 10 != 3 {
            publisher.publish(&sequenced).unwrap();
        }
    }
    publisher.heartbeat().unwrap();

    // Follower: replay the stream into a fresh Bank replica.
    let mut receiver = Receiver::new(
        sock_a,
        Some(sock_b),
        ReceiverConfig {
            from: Seq(1),
            retransmitter: Some(retransmitter),
        },
    );
    let mut replica: Replica<Bank> = Replica::new(&());
    while replica.seq() < node.seq() {
        match receiver.poll(Duration::from_secs(5)).unwrap() {
            Some(frame) => {
                replica.apply(&frame).unwrap();
            }
            None => panic!("follower stalled at seq {}", replica.seq()),
        }
    }

    assert_eq!(replica.seq(), node.seq());
    assert_eq!(
        encode_to_vec(&replica.service().snapshot()),
        encode_to_vec(&node.service().snapshot()),
        "replica state must be bit-identical to the leader"
    );
}
