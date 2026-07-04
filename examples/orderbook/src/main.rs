//! CLI for the order-book example. Every invocation cold-starts, recovers
//! the book from `./orderbook-journal/` (latest snapshot + journal tail),
//! applies one command, and exits.
//!
//! ```text
//! cargo run -p orderbook -- sell 100 102
//! cargo run -p orderbook -- sell 50 101
//! cargo run -p orderbook -- buy 120 102     # trades 50@101 then 70@102
//! cargo run -p orderbook -- book
//! cargo run -p orderbook -- cancel 3
//! cargo run -p orderbook -- verify
//! ```

use orderbook::{Cmd, OrderBook, Side};
use ticktape::{Node, NodeConfig};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut config = NodeConfig::new("orderbook-journal");
    config.snapshot_every = Some(100);
    let mut node: Node<OrderBook> = Node::open(config, ()).expect("open node");
    let info = node.recovery_info();
    eprintln!(
        "recovered {} resting orders at seq {} ({}, {} inputs replayed)",
        node.service().resting_orders(),
        node.seq(),
        match info.snapshot_seq {
            Some(seq) => format!("snapshot at seq {seq}"),
            None => "no snapshot".to_string(),
        },
        info.inputs_replayed,
    );

    let arg = |i: usize| -> u64 {
        args.get(i)
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| usage())
    };

    match args.first().map(String::as_str) {
        Some(verb @ ("buy" | "sell")) => {
            let side = if verb == "buy" { Side::Buy } else { Side::Sell };
            let (qty, price) = (arg(1) as u32, arg(2) as u32);
            // Deterministic default order id: the seq this submit will get.
            let id = match args.get(3) {
                Some(s) => s.parse().unwrap_or_else(|_| usage()),
                None => node.seq().as_u64() + 1,
            };
            let (seq, events) = node
                .submit(Cmd::Submit {
                    id,
                    side,
                    price,
                    qty,
                })
                .expect("submit");
            println!("seq {seq} (order id {id}):");
            for event in events {
                println!("  {event:?}");
            }
        }
        Some("cancel") => {
            let (seq, events) = node.submit(Cmd::Cancel { id: arg(1) }).expect("submit");
            println!("seq {seq}: {events:?}");
        }
        Some("book") => {
            let (bids, asks) = node.service().depth(10);
            println!("{:>12} | {:<12}", "BID qty@px", "ASK qty@px");
            let rows = bids.len().max(asks.len());
            for i in 0..rows {
                let bid = bids
                    .get(i)
                    .map(|(p, q)| format!("{q}@{p}"))
                    .unwrap_or_default();
                let ask = asks
                    .get(i)
                    .map(|(p, q)| format!("{q}@{p}"))
                    .unwrap_or_default();
                println!("{bid:>12} | {ask:<12}");
            }
            if rows == 0 {
                println!("       (empty book)");
            }
        }
        Some("verify") => {
            use ticktape_sim::Invariants;
            node.service().check().expect("book invariants");
            let ok = node.verify_replay().expect("verify");
            println!(
                "invariants: OK · replay equivalence: {}",
                if ok { "OK" } else { "DIVERGED" }
            );
            if !ok {
                std::process::exit(1);
            }
        }
        _ => usage(),
    }
}

fn usage() -> ! {
    eprintln!("usage: orderbook [buy <qty> <price> [id] | sell <qty> <price> [id] | cancel <id> | book | verify]");
    std::process::exit(2)
}
