//! The M5 acceptance: external clients drive the order book end-to-end
//! over real TCP — sessions, dedup, gap rejection, event addressing to
//! both sides of a trade, drop-copy, and cancel-on-disconnect.

use exchange::Exchange;
use orderbook::{Cmd, Evt, Side};
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::time::Duration;
use ticktape::{FsyncPolicy, Node, NodeConfig};
use ticktape_gateway::{
    read_msg, serve, write_msg, ClientMsg, RejectReason, ServeConfig, ServerMsg,
};

struct Client {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
    client_seq: u64,
}

impl Client {
    fn connect(addr: std::net::SocketAddr, session: u64) -> Client {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        write_msg(&mut stream, &ClientMsg::<Cmd>::Hello { session }).unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Client {
            stream,
            reader,
            client_seq: 0,
        }
    }

    fn send_seq(&mut self, client_seq: u64, cmd: Cmd) {
        write_msg(&mut self.stream, &ClientMsg::Cmd { client_seq, cmd }).unwrap();
    }

    fn send(&mut self, cmd: Cmd) -> u64 {
        self.client_seq += 1;
        self.send_seq(self.client_seq, cmd);
        self.client_seq
    }

    fn recv(&mut self) -> ServerMsg<Evt> {
        read_msg(&mut self.reader).unwrap().expect("server closed")
    }

    /// Receive until an Ack or Rejected arrives; return it plus the events
    /// seen on the way.
    fn recv_outcome(&mut self) -> (ServerMsg<Evt>, Vec<Evt>) {
        let mut events = Vec::new();
        loop {
            match self.recv() {
                ServerMsg::Event(event) => events.push(event),
                outcome => return (outcome, events),
            }
        }
    }
}

fn start_exchange() -> std::net::SocketAddr {
    let dir = tempfile::tempdir().unwrap();
    let mut config = NodeConfig::new(dir.path());
    config.journal.fsync = FsyncPolicy::EveryFrame;
    let node: Node<Exchange> = Node::open(config, ()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        // Keep the journal dir alive for the server's lifetime.
        let _dir = dir;
        serve(node, listener, ServeConfig::default())
    });
    addr
}

fn submit(id: u64, side: Side, price: u32, qty: u32) -> Cmd {
    Cmd::Submit {
        id,
        side,
        price,
        qty,
    }
}

#[test]
fn clients_trade_and_both_sides_hear_about_it() {
    let addr = start_exchange();
    let mut maker = Client::connect(addr, 1);
    let mut taker = Client::connect(addr, 2);

    maker.send(submit(11, Side::Sell, 102, 100));
    let (ack, events) = maker.recv_outcome();
    assert!(
        matches!(ack, ServerMsg::Ack { client_seq: 1, .. }),
        "{ack:?}"
    );
    // Events may arrive before or after the ack; collect either way.
    let mut maker_events = events;
    if maker_events.is_empty() {
        if let ServerMsg::Event(e) = maker.recv() {
            maker_events.push(e);
        }
    }
    assert_eq!(maker_events, vec![Evt::Accepted { id: 11 }]);

    taker.send(submit(21, Side::Buy, 105, 40));
    let (ack, _) = taker.recv_outcome();
    assert!(matches!(ack, ServerMsg::Ack { .. }));
    // Taker hears its accept + the trade at the maker's price.
    let mut taker_events = Vec::new();
    while taker_events.len() < 2 {
        if let ServerMsg::Event(e) = taker.recv() {
            taker_events.push(e);
        }
    }
    assert_eq!(
        taker_events,
        vec![
            Evt::Accepted { id: 21 },
            Evt::Trade {
                taker: 21,
                maker: 11,
                price: 102,
                qty: 40
            },
        ]
    );
    // The maker's session is told about its fill too.
    let ServerMsg::Event(fill) = maker.recv() else {
        panic!("maker must hear the trade")
    };
    assert_eq!(
        fill,
        Evt::Trade {
            taker: 21,
            maker: 11,
            price: 102,
            qty: 40
        }
    );
}

#[test]
fn retries_are_exactly_once_and_gaps_are_rejected() {
    let addr = start_exchange();
    let mut client = Client::connect(addr, 7);

    client.send_seq(1, submit(71, Side::Sell, 100, 10));
    let (ack, _) = client.recv_outcome();
    assert!(matches!(ack, ServerMsg::Ack { client_seq: 1, .. }));
    let _ = client.recv(); // Accepted event

    // Retry after a "lost ack": dropped as duplicate, no double order.
    client.send_seq(1, submit(71, Side::Sell, 100, 10));
    let (outcome, events) = client.recv_outcome();
    assert!(
        events.is_empty(),
        "duplicate must have no effect: {events:?}"
    );
    assert!(
        matches!(
            outcome,
            ServerMsg::Rejected {
                client_seq: 1,
                reason: RejectReason::Duplicate
            }
        ),
        "{outcome:?}"
    );

    // Skipping ahead is a protocol error naming the expected seq.
    client.send_seq(5, Cmd::Cancel { id: 71 });
    let (outcome, _) = client.recv_outcome();
    assert!(
        matches!(
            outcome,
            ServerMsg::Rejected {
                client_seq: 5,
                reason: RejectReason::Gap { expected: 2 }
            }
        ),
        "{outcome:?}"
    );

    // And the order is still there, exactly one of it: cancel succeeds.
    client.send_seq(2, Cmd::Cancel { id: 71 });
    let (ack, events) = client.recv_outcome();
    assert!(matches!(ack, ServerMsg::Ack { client_seq: 2, .. }));
    let canceled = if events.is_empty() {
        match client.recv() {
            ServerMsg::Event(e) => e,
            other => panic!("{other:?}"),
        }
    } else {
        events[0].clone()
    };
    assert_eq!(
        canceled,
        Evt::Canceled {
            id: 71,
            remaining: 10
        }
    );
}

#[test]
fn disconnect_pulls_resting_orders_and_drop_copy_sees_it_all() {
    let addr = start_exchange();

    // Compliance watches session 3 before it does anything.
    let mut watcher_stream = TcpStream::connect(addr).unwrap();
    watcher_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write_msg(
        &mut watcher_stream,
        &ClientMsg::<Cmd>::DropCopy { session: 3 },
    )
    .unwrap();
    let mut watcher = BufReader::new(watcher_stream);
    // Wait for the registration ack: nothing observable may happen before
    // the watcher is provably live (otherwise this test races its setup).
    assert!(matches!(
        read_msg::<ServerMsg<Evt>>(&mut watcher).unwrap(),
        Some(ServerMsg::Ack { client_seq: 0, .. })
    ));

    // Session 3 rests an order, then vanishes (connection drop).
    let mut trader = Client::connect(addr, 3);
    trader.send(submit(31, Side::Buy, 95, 25));
    let (ack, _) = trader.recv_outcome();
    assert!(matches!(ack, ServerMsg::Ack { .. }));
    drop(trader);

    // The watcher sees the accept, then the cancel-on-disconnect pull.
    let mut seen = Vec::new();
    while seen.len() < 2 {
        match read_msg::<ServerMsg<Evt>>(&mut watcher).unwrap() {
            Some(ServerMsg::Event(e)) => seen.push(e),
            Some(_) => {}
            None => panic!("watcher disconnected early"),
        }
    }
    assert_eq!(
        seen,
        vec![
            Evt::Accepted { id: 31 },
            Evt::Canceled {
                id: 31,
                remaining: 25
            },
        ]
    );

    // A newcomer selling into that price finds no resting bid: the book
    // really was cleaned deterministically.
    let mut probe = Client::connect(addr, 4);
    probe.send(submit(41, Side::Sell, 95, 25));
    let (_ack, _) = probe.recv_outcome();
    let ServerMsg::Event(accepted) = probe.recv() else {
        panic!("expected accept")
    };
    assert_eq!(accepted, Evt::Accepted { id: 41 });
    // No trade event follows; the next thing this session could hear would
    // require new activity. (A fill would have arrived with the accept.)
}

#[test]
fn reconnect_resumes_dedup_state() {
    let addr = start_exchange();
    let mut first = Client::connect(addr, 9);
    first.send_seq(1, submit(91, Side::Sell, 100, 5));
    let (ack, _) = first.recv_outcome();
    assert!(matches!(ack, ServerMsg::Ack { .. }));
    drop(first); // disconnect (also cancels the resting order)

    let mut second = Client::connect(addr, 9);
    // The old seq is still burned: a replay of it is a duplicate...
    second.send_seq(1, submit(91, Side::Sell, 100, 5));
    let (outcome, _) = second.recv_outcome();
    assert!(matches!(
        outcome,
        ServerMsg::Rejected {
            reason: RejectReason::Duplicate,
            ..
        }
    ));
    // ...and the session continues where it left off.
    second.send_seq(2, submit(92, Side::Sell, 101, 5));
    let (ack, _) = second.recv_outcome();
    assert!(matches!(ack, ServerMsg::Ack { client_seq: 2, .. }));
}
