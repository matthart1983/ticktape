//! The end-to-end exchange demo: a TCP gateway in front of the journaled
//! order book.
//!
//! ```text
//! # terminal 1 — the exchange
//! cargo run -p exchange -- serve --addr 127.0.0.1:7200
//!
//! # terminal 2 — a trader (session 1); type commands:
//! cargo run -p exchange -- client --addr 127.0.0.1:7200 --session 1
//! > sell 100 102
//! > buy 50 99
//! > cancel 3
//!
//! # terminal 3 — compliance, watching session 1's outcomes
//! cargo run -p exchange -- watch --addr 127.0.0.1:7200 --session 1
//! ```
//!
//! Kill a client mid-session: its resting orders are pulled
//! (cancel-on-disconnect), visibly on the watcher.

use exchange::Exchange;
use orderbook::{Cmd, Evt, Side};
use std::io::BufRead;
use std::net::{TcpListener, TcpStream};
use ticktape::{Node, NodeConfig};
use ticktape_gateway::{read_msg, serve, write_msg, ClientMsg, ServeConfig, ServerMsg};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let addr = flag(&args, "--addr").unwrap_or_else(|| "127.0.0.1:7200".into());
    match args.first().map(String::as_str) {
        Some("serve") => run_server(&addr),
        Some("client") => run_client(&addr, session(&args)),
        Some("watch") => run_watcher(&addr, session(&args)),
        _ => {
            eprintln!("usage: exchange [serve | client | watch] --addr <addr> [--session <n>]");
            std::process::exit(2);
        }
    }
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn session(args: &[String]) -> u64 {
    flag(args, "--session")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

fn run_server(addr: &str) {
    let mut config = NodeConfig::new("exchange-journal");
    config.snapshot_every = Some(500);
    let node: Node<Exchange> = Node::open(config, ()).expect("open node");
    let resume = node.service().sessions();
    println!(
        "exchange: recovered at seq {} ({} known sessions); listening on {addr}",
        node.seq(),
        resume.len()
    );
    let listener = TcpListener::bind(addr).expect("bind");
    serve(
        node,
        listener,
        ServeConfig {
            window: 64,
            resume,
            ..ServeConfig::default()
        },
    );
}

fn run_client(addr: &str, session: u64) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    write_msg(
        &mut stream,
        &ClientMsg::<Cmd>::Hello {
            session,
            from_event_seq: 0,
        },
    )
    .expect("hello");
    let mut reader = std::io::BufReader::new(stream.try_clone().expect("clone"));
    std::thread::spawn(move || loop {
        match read_msg::<ServerMsg<Evt>>(&mut reader) {
            Ok(Some(msg)) => println!("  << {msg:?}"),
            _ => {
                println!("  (disconnected)");
                std::process::exit(0);
            }
        }
    });

    println!("session {session}: buy <qty> <price> | sell <qty> <price> | cancel <id>");
    let mut client_seq = 0u64;
    let mut next_order_id = session * 1_000_000;
    for line in std::io::stdin().lock().lines() {
        let line = line.unwrap_or_default();
        let parts: Vec<&str> = line.split_whitespace().collect();
        let cmd = match parts.as_slice() {
            [verb @ ("buy" | "sell"), qty, price] => {
                next_order_id += 1;
                Cmd::Submit {
                    id: next_order_id,
                    side: if *verb == "buy" {
                        Side::Buy
                    } else {
                        Side::Sell
                    },
                    price: price.parse().unwrap_or(0),
                    qty: qty.parse().unwrap_or(0),
                }
            }
            ["cancel", id] => Cmd::Cancel {
                id: id.parse().unwrap_or(0),
            },
            [] => continue,
            _ => {
                println!("  ?");
                continue;
            }
        };
        client_seq += 1;
        write_msg(&mut stream, &ClientMsg::Cmd { client_seq, cmd }).expect("send");
    }
}

fn run_watcher(addr: &str, session: u64) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    write_msg(
        &mut stream,
        &ClientMsg::<Cmd>::DropCopy {
            session,
            from_event_seq: 0,
        },
    )
    .expect("hello");
    println!("drop-copy: watching session {session}");
    let mut reader = std::io::BufReader::new(stream);
    loop {
        match read_msg::<ServerMsg<Evt>>(&mut reader) {
            Ok(Some(msg)) => println!("  << {msg:?}"),
            _ => return,
        }
    }
}
