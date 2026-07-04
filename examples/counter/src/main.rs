//! The Ticktape hello world: a deterministic counter.
//!
//! ```text
//! cargo run -p counter -- add 5      # sequence + journal + apply
//! cargo run -p counter -- add 37
//! cargo run -p counter -- show       # state recovered by replay: 42
//! cargo run -p counter -- reset
//! ```
//!
//! Every invocation is a "crash recovery": the process starts cold, rebuilds
//! state as `genesis + replay(journal)`, applies any new command, and exits.
//! The journal lives in `./counter-journal/`.

use ticktape::{Ctx, Decode, Encode, Node, NodeConfig, Seq, Service};

struct Counter {
    value: i64,
}

#[derive(Encode, Decode, Debug)]
enum Cmd {
    Add(i64),
    Reset,
}

#[derive(Encode, Decode, Debug)]
enum Evt {
    Value(i64),
}

impl Service for Counter {
    type Input = Cmd;
    type Output = Evt;
    type Snapshot = i64;
    type Config = ();

    fn genesis(_: &()) -> Self {
        Counter { value: 0 }
    }

    fn apply(&mut self, _seq: Seq, cmd: &Cmd, ctx: &mut Ctx<'_, Evt>) {
        match cmd {
            Cmd::Add(n) => self.value += n,
            Cmd::Reset => self.value = 0,
        }
        ctx.emit(Evt::Value(self.value));
    }

    fn snapshot(&self) -> i64 {
        self.value
    }

    fn restore(v: i64, _: &()) -> Self {
        Counter { value: v }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut node: Node<Counter> =
        Node::open(NodeConfig::new("counter-journal"), ()).expect("open node");
    println!(
        "recovered: value={} at seq {} (replayed from ./counter-journal)",
        node.service().value,
        node.seq()
    );

    let cmd = match args.first().map(String::as_str) {
        Some("add") => {
            let n: i64 = args
                .get(1)
                .and_then(|s| s.parse().ok())
                .expect("usage: counter add <n>");
            Some(Cmd::Add(n))
        }
        Some("reset") => Some(Cmd::Reset),
        Some("show") | None => None,
        Some(other) => {
            eprintln!("unknown command {other:?}; usage: counter [add <n> | reset | show]");
            std::process::exit(2);
        }
    };

    if let Some(cmd) = cmd {
        let (seq, outs) = node.submit(cmd).expect("submit");
        println!("applied at seq {seq}: {outs:?}");
    }
}
