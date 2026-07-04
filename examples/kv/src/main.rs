//! CLI for the Ticktape KV example. Every invocation cold-starts and
//! rebuilds state by replaying `./kv-journal/`, then applies one command:
//!
//! ```text
//! cargo run -p kv -- put name ada
//! cargo run -p kv -- get name          # Value(Some("ada")), via replay
//! cargo run -p kv -- del name
//! cargo run -p kv -- list
//! cargo run -p kv -- verify            # replay-equivalence self-check
//! ```

use kv::{Cmd, Kv};
use ticktape::{Node, NodeConfig};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut node: Node<Kv> = Node::open(NodeConfig::new("kv-journal"), ()).expect("open node");
    eprintln!(
        "recovered {} keys at seq {} (replayed from ./kv-journal)",
        node.service().len(),
        node.seq()
    );

    let arg =
        |i: usize, what: &str| -> String { args.get(i).unwrap_or_else(|| usage(what)).clone() };

    match args.first().map(String::as_str) {
        Some("put") => {
            let cmd = Cmd::Put {
                key: arg(1, "put <key> <value>"),
                value: arg(2, "put <key> <value>"),
            };
            let (seq, outs) = node.submit(cmd).expect("submit");
            println!("seq {seq}: {outs:?}");
        }
        Some("del") => {
            let (seq, outs) = node
                .submit(Cmd::Del {
                    key: arg(1, "del <key>"),
                })
                .expect("submit");
            println!("seq {seq}: {outs:?}");
        }
        Some("get") => {
            let (seq, outs) = node
                .submit(Cmd::Get {
                    key: arg(1, "get <key>"),
                })
                .expect("submit");
            println!("seq {seq}: {outs:?}");
        }
        Some("list") => {
            for (k, v) in node.service().iter() {
                println!("{k} = {v}");
            }
        }
        Some("verify") => {
            let ok = node.verify_replay().expect("verify");
            println!("replay equivalence: {}", if ok { "OK" } else { "DIVERGED" });
            if !ok {
                std::process::exit(1);
            }
        }
        _ => {
            usage("put <key> <value> | del <key> | get <key> | list | verify");
        }
    }
}

fn usage(what: &str) -> ! {
    eprintln!("usage: kv {what}");
    std::process::exit(2)
}
