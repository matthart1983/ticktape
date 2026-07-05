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
    /// Highest per-session event_seq seen — passed as `from_event_seq` on
    /// reconnect to backfill exactly the gap.
    last_event_seq: u64,
}

impl Client {
    fn connect(addr: std::net::SocketAddr, session: u64) -> Client {
        Client::resume(addr, session, 0)
    }

    /// Connect (or reconnect) requesting replay of events after
    /// `from_event_seq`.
    fn resume(addr: std::net::SocketAddr, session: u64, from_event_seq: u64) -> Client {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        write_msg(
            &mut stream,
            &ClientMsg::<Cmd>::Hello {
                session,
                from_event_seq,
            },
        )
        .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Client {
            stream,
            reader,
            client_seq: 0,
            last_event_seq: from_event_seq,
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
        let msg = read_msg(&mut self.reader).unwrap().expect("server closed");
        if let ServerMsg::Event { event_seq, .. } = &msg {
            self.last_event_seq = self.last_event_seq.max(*event_seq);
        }
        msg
    }

    /// The event payload of the next message, asserting it is an Event.
    fn recv_event(&mut self) -> Evt {
        match self.recv() {
            ServerMsg::Event { event, .. } => event,
            other => panic!("expected an event, got {other:?}"),
        }
    }

    /// Receive until an Ack or Rejected arrives; return it plus the events
    /// seen on the way.
    fn recv_outcome(&mut self) -> (ServerMsg<Evt>, Vec<Evt>) {
        let mut events = Vec::new();
        loop {
            match self.recv() {
                ServerMsg::Event { event, .. } => events.push(event),
                outcome => return (outcome, events),
            }
        }
    }
}

/// A drop-copy observer connection, able to (re)join from a given
/// `from_event_seq` and read the events addressed to a session.
struct Observer {
    reader: BufReader<TcpStream>,
    last_event_seq: u64,
}

impl Observer {
    fn watch(addr: std::net::SocketAddr, session: u64, from_event_seq: u64) -> Observer {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        write_msg(
            &mut stream,
            &ClientMsg::<Cmd>::DropCopy {
                session,
                from_event_seq,
            },
        )
        .unwrap();
        let mut reader = BufReader::new(stream);
        // The registration ack proves the observer is live before we assert.
        assert!(matches!(
            read_msg::<ServerMsg<Evt>>(&mut reader).unwrap(),
            Some(ServerMsg::Ack { client_seq: 0, .. })
        ));
        Observer {
            reader,
            last_event_seq: from_event_seq,
        }
    }

    /// Next event, tracking the running event_seq (asserting monotonicity).
    fn recv_event(&mut self) -> (u64, Evt) {
        loop {
            match read_msg::<ServerMsg<Evt>>(&mut self.reader).unwrap() {
                Some(ServerMsg::Event { event_seq, event }) => {
                    assert!(
                        event_seq > self.last_event_seq,
                        "event_seq must be strictly increasing: {event_seq} after {}",
                        self.last_event_seq
                    );
                    self.last_event_seq = event_seq;
                    return (event_seq, event);
                }
                Some(_) => {}
                None => panic!("observer disconnected early"),
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
        maker_events.push(maker.recv_event());
    }
    assert_eq!(maker_events, vec![Evt::Accepted { id: 11 }]);

    taker.send(submit(21, Side::Buy, 105, 40));
    let (ack, _) = taker.recv_outcome();
    assert!(matches!(ack, ServerMsg::Ack { .. }));
    // Taker hears its accept + the trade at the maker's price.
    let mut taker_events = Vec::new();
    while taker_events.len() < 2 {
        taker_events.push(taker.recv_event());
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
    let fill = maker.recv_event();
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
        client.recv_event()
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
        &ClientMsg::<Cmd>::DropCopy {
            session: 3,
            from_event_seq: 0,
        },
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
            Some(ServerMsg::Event { event, .. }) => seen.push(event),
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
    let accepted = probe.recv_event();
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

// ---- P1.2: per-session outbox + reconnect replay ----

#[test]
fn drop_copy_observer_replays_the_fills_it_missed_while_disconnected() {
    // The headline scenario: an observer watches a maker's session, drops
    // offline, the maker's resting book trades while it is gone, and on
    // reconnect the observer replays exactly the fills it missed — no gap,
    // no duplication. (The maker stays connected, so its orders survive; the
    // observer is the party that reconnects.)
    let addr = start_exchange();

    // Maker rests a big sell on session 1.
    let mut maker = Client::connect(addr, 1);
    maker.send(submit(11, Side::Sell, 102, 100));
    let (ack, _) = maker.recv_outcome();
    assert!(matches!(ack, ServerMsg::Ack { .. }));

    // Compliance watches session 1 from the start and sees the Accepted
    // (event_seq 1), then goes offline.
    let mut observer = Observer::watch(addr, 1, 0);
    let (seq1, e1) = observer.recv_event();
    assert_eq!((seq1, e1), (1, Evt::Accepted { id: 11 }));
    let seen_through = observer.last_event_seq; // == 1
    drop(observer);

    // While the observer is gone, a taker lifts part of the maker's order.
    let mut taker = Client::connect(addr, 2);
    taker.send(submit(21, Side::Buy, 105, 40));
    let (ack, _) = taker.recv_outcome();
    assert!(matches!(ack, ServerMsg::Ack { .. }));
    // Drain the maker's stream up to and including its own live fill (the
    // Accepted may still be buffered after the earlier ack), so the trade is
    // provably sequenced before the observer reconnects.
    let trade = Evt::Trade {
        taker: 21,
        maker: 11,
        price: 102,
        qty: 40,
    };
    loop {
        if maker.recv_event() == trade {
            break;
        }
    }

    // The observer reconnects from where it left off and must replay the
    // maker's fill (event_seq 2) — the event it missed while offline.
    let mut observer = Observer::watch(addr, 1, seen_through);
    let (seq2, e2) = observer.recv_event();
    assert_eq!(
        (seq2, e2),
        (
            2,
            Evt::Trade {
                taker: 21,
                maker: 11,
                price: 102,
                qty: 40
            }
        ),
        "observer must replay exactly the fill it missed"
    );
}

#[test]
fn a_reconnecting_command_client_is_backfilled_its_missed_events() {
    // A command client rests an order and disconnects; cancel-on-disconnect
    // generates a Canceled event it never saw. On reconnect (resuming from
    // its last seen event_seq) the gateway backfills that missed event.
    let addr = start_exchange();

    let mut client = Client::connect(addr, 8);
    client.send(submit(81, Side::Sell, 100, 7));
    let (ack, mut events) = client.recv_outcome();
    assert!(matches!(ack, ServerMsg::Ack { .. }));
    if events.is_empty() {
        events.push(client.recv_event());
    }
    assert_eq!(events, vec![Evt::Accepted { id: 81 }]);
    let seen_through = client.last_event_seq; // Accepted == event_seq 1
    drop(client); // cancel-on-disconnect pulls order 81 while we are away

    // Reconnect resuming from the Accepted; the missed Canceled (whether it
    // was sequenced just before or just after we re-attach) must reach us —
    // as backfill if it landed first, live otherwise.
    let mut client = Client::resume(addr, 8, seen_through);
    let canceled = client.recv_event();
    assert_eq!(
        canceled,
        Evt::Canceled {
            id: 81,
            remaining: 7
        },
        "the reconnecting client must be told its order was pulled"
    );
    assert!(
        client.last_event_seq > seen_through,
        "the backfilled event advances the client's event_seq"
    );
}
