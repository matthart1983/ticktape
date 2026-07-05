//! Multi-process, single-box demo: run a leader in one terminal and any
//! number of followers in others. The leader sequences random Bank
//! transfers, journals them, and publishes the stream over UDP; followers
//! rebuild bit-identical state live, gap-filling anything lost.
//!
//! ```text
//! # terminal 1 — follower (bind first so nothing is missed; late joins
//! # also work — the retransmitter backfills)
//! cargo run -p feed -- sub --bind 127.0.0.1:7101 --retx 127.0.0.1:7110
//!
//! # terminal 2 — leader
//! cargo run -p feed -- pub --to 127.0.0.1:7101 --retx-port 7110
//! ```

use std::net::SocketAddr;
use std::time::Duration;
use ticktape::{Node, NodeConfig, Seq, Service};
use ticktape_journal::{JournalConfig, RealStorage};
use ticktape_sim::demo::{gen_transfer, Bank};
use ticktape_sim::{Invariants, Rng};
use ticktape_transport::{
    bind_udp, ChainStore, JournalRewinder, MemStore, Publisher, PublisherConfig, Receiver,
    ReceiverConfig, Replica, Retransmitter,
};

const SESSION: u64 = 0xF33D;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("pub") => run_publisher(&args[1..]),
        Some("sub") => run_subscriber(&args[1..]),
        _ => usage(),
    }
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn addr(args: &[String], name: &str) -> Option<SocketAddr> {
    flag(args, name).map(|s| s.parse().unwrap_or_else(|_| usage()))
}

fn run_publisher(args: &[String]) {
    let dest_a = addr(args, "--to").unwrap_or_else(|| usage());
    let dest_b = addr(args, "--to-b");
    let retx_port: u16 = flag(args, "--retx-port")
        .and_then(|s| s.parse().ok())
        .unwrap_or(7110);

    // Runs forever: snapshot + compaction bound disk, and the retransmitter
    // is a repeater/rewinder chain — recent gap-fill from a bounded RAM
    // window, historical (and post-restart late-join) ranges from the
    // journal on disk. No unbounded store, no lost history across restarts.
    let mut node_config = NodeConfig::new("feed-journal");
    node_config.snapshot_every = Some(500);
    let journal_config = JournalConfig::new("feed-journal");
    let repeater = MemStore::with_capacity(64 * 1024);
    let store = ChainStore {
        primary: repeater.clone(),
        secondary: JournalRewinder::new(journal_config, RealStorage),
    };
    let (retransmitter, retx_addr) = Retransmitter::bind(
        format!("127.0.0.1:{retx_port}").parse().unwrap(),
        SESSION,
        store,
    )
    .expect("bind retransmitter");
    std::thread::spawn(move || retransmitter.serve_forever());

    let mut publisher = Publisher::new(PublisherConfig {
        session: SESSION,
        dest_a,
        dest_b,
    })
    .expect("publisher");

    let mut node: Node<Bank> = Node::open(node_config, ()).expect("open node");
    let stream = node.subscribe();
    println!(
        "leader: recovered at seq {}, publishing to {dest_a}{}; retransmitter on {retx_addr}",
        node.seq(),
        dest_b.map(|b| format!(" + {b}")).unwrap_or_default(),
    );

    let mut rng = Rng::new(0xF33D);
    loop {
        node.submit(gen_transfer(&mut rng)).expect("submit");
        for frame in stream.try_iter() {
            repeater.record(frame.clone());
            publisher.publish(&frame).expect("publish");
        }
        publisher.heartbeat().expect("heartbeat");
        if node.seq().0.is_multiple_of(100) {
            println!("leader: seq {}", node.seq());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn run_subscriber(args: &[String]) {
    let bind_a = addr(args, "--bind").unwrap_or_else(|| usage());
    let bind_b = addr(args, "--bind-b");
    let retx = addr(args, "--retx");
    let from: u64 = flag(args, "--from")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let sock_a = bind_udp(bind_a).expect("bind A");
    let sock_b = bind_b.map(|b| bind_udp(b).expect("bind B"));
    let mut receiver = Receiver::new(
        sock_a,
        sock_b,
        ReceiverConfig {
            from: Seq(from),
            retransmitter: retx,
        },
    );
    let mut replica: Replica<Bank> = Replica::new(&());
    println!("follower: listening on {bind_a}, expecting seq {from}");

    loop {
        match receiver.poll(Duration::from_secs(1)) {
            Ok(Some(frame)) => {
                replica.apply(&frame).expect("apply");
                if replica.seq().0.is_multiple_of(100) {
                    replica.service().check().expect("invariants");
                    println!(
                        "follower: seq {} · balances {:?} · invariants OK",
                        replica.seq(),
                        replica.service().snapshot()
                    );
                }
            }
            Ok(None) => {} // quiet second; keep listening
            Err(e) => {
                eprintln!("follower: {e}");
                std::process::exit(1);
            }
        }
    }
}

fn usage() -> ! {
    eprintln!(
        "usage:\n  feed pub --to <addr> [--to-b <addr>] [--retx-port <port>]\n  feed sub --bind <addr> [--bind-b <addr>] [--retx <addr>] [--from <seq>]"
    );
    std::process::exit(2)
}
