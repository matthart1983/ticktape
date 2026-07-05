//! A threaded TCP gateway hosting any session-aware `Service`.
//!
//! One **sequencer thread** owns the `Node` (single logical writer, as
//! always); per-connection reader threads admit client commands through
//! [`SessionFlow`] and forward them; per-connection writer threads drain
//! outboxes. Events are routed by the [`Addressed`] session on each output,
//! acks/rejects go to the owning command connection, and drop-copy
//! observers receive the same event bytes the client does.
//!
//! Lifecycle notes (M5 scope): `serve` blocks forever (run it on a
//! thread); connections are cheap threads rather than an event loop; a
//! dropped command connection injects `SessionClosed` exactly once.

use crate::flow::{Admit, SessionFlow};
use crate::wire::{read_msg, Addressed, ClientMsg, GatewayInput, RejectReason, ServerMsg};
use std::collections::{BTreeMap, VecDeque};
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use ticktape_core::{Decode, Encode, Service};
use ticktape_journal::Storage;
use ticktape_runtime::{Node, TimeSource};

#[derive(Debug, Clone)]
pub struct ServeConfig {
    /// Max unacknowledged commands per session (1 = single-outstanding).
    pub window: u32,
    /// Sessions recovered from the journaled envelopes:
    /// `(session, last_admitted_client_seq)`. Compute from the recovered
    /// service state before serving so restarts keep dedup exact.
    pub resume: Vec<(u64, u64)>,
    /// How many recent outbound events to retain per session for reconnect
    /// replay. A reconnecting client (or drop-copy observer) that is within
    /// this many events of the tip gets an exact backfill; one that fell
    /// further behind sees an `event_seq` gap and must resync. Bounds gateway
    /// memory — the outbox is the edge-side analogue of the journal's bounded
    /// retention.
    pub outbox_capacity: usize,
}

impl Default for ServeConfig {
    fn default() -> Self {
        ServeConfig {
            window: 64,
            resume: Vec::new(),
            outbox_capacity: 1024,
        }
    }
}

enum Req<C> {
    /// A command connection claimed `session`; replies flow to `sink`, and
    /// events after `from_event_seq` are replayed from the outbox.
    Attach {
        session: u64,
        from_event_seq: u64,
        sink: mpsc::Sender<Vec<u8>>,
    },
    /// A drop-copy observer for `session`, replaying from `from_event_seq`.
    Watch {
        session: u64,
        from_event_seq: u64,
        sink: mpsc::Sender<Vec<u8>>,
    },
    Cmd {
        session: u64,
        client_seq: u64,
        cmd: C,
    },
    /// The command connection dropped.
    Close { session: u64 },
}

/// Per-session edge state: the live sinks plus a bounded replay outbox.
struct SessionSinks {
    cmd: Option<mpsc::Sender<Vec<u8>>>,
    watchers: Vec<mpsc::Sender<Vec<u8>>>,
    /// The next per-session `event_seq` to assign (monotonic from 1).
    next_event_seq: u64,
    /// Recent outbound events as `(event_seq, encoded ServerMsg::Event)`,
    /// oldest first, capped at `outbox_capacity` — the reconnect backfill.
    outbox: VecDeque<(u64, Vec<u8>)>,
}

impl SessionSinks {
    fn new() -> Self {
        SessionSinks {
            cmd: None,
            watchers: Vec::new(),
            next_event_seq: 1,
            outbox: VecDeque::new(),
        }
    }

    /// Every `event_seq > from` still held, in order — the reconnect backfill.
    fn replay_after(&self, from: u64) -> Vec<Vec<u8>> {
        self.outbox
            .iter()
            .filter(|(eseq, _)| *eseq > from)
            .map(|(_, bytes)| bytes.clone())
            .collect()
    }
}

/// Serve `node` on `listener`, forever. Run on a dedicated thread.
pub fn serve<S, C, E, T, St>(node: Node<S, T, St>, listener: TcpListener, config: ServeConfig) -> !
where
    S: Service<Input = GatewayInput<C>, Output = Addressed<E>> + Send + 'static,
    S::Config: Send,
    C: Encode + Decode + Send + 'static,
    E: Encode + Send + 'static,
    T: TimeSource + Send + 'static,
    St: Storage + Clone + Send + 'static,
    St::File: Send,
{
    let (req_tx, req_rx) = mpsc::channel::<Req<C>>();

    std::thread::spawn(move || sequencer_loop(node, req_rx, config));

    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let req_tx = req_tx.clone();
        std::thread::spawn(move || connection_loop(stream, req_tx));
    }
    unreachable!("listener.incoming() never returns None")
}

fn sequencer_loop<S, C, E, T, St>(
    mut node: Node<S, T, St>,
    req_rx: mpsc::Receiver<Req<C>>,
    config: ServeConfig,
) where
    S: Service<Input = GatewayInput<C>, Output = Addressed<E>>,
    C: Encode + Decode,
    E: Encode,
    T: TimeSource,
    St: Storage + Clone,
{
    let mut flows: BTreeMap<u64, SessionFlow> = config
        .resume
        .iter()
        .map(|&(session, last)| (session, SessionFlow::resume(last, config.window)))
        .collect();
    let mut sinks: BTreeMap<u64, SessionSinks> = BTreeMap::new();
    let cap = config.outbox_capacity;

    while let Ok(req) = req_rx.recv() {
        match req {
            Req::Attach {
                session,
                from_event_seq,
                sink,
            } => {
                flows
                    .entry(session)
                    .or_insert_with(|| SessionFlow::new(config.window));
                let entry = sinks.entry(session).or_insert_with(SessionSinks::new);
                entry.cmd = Some(sink.clone());
                // Backfill the gap this client missed while disconnected.
                for bytes in entry.replay_after(from_event_seq) {
                    if sink.send(bytes).is_err() {
                        entry.cmd = None;
                        break;
                    }
                }
            }
            Req::Watch {
                session,
                from_event_seq,
                sink,
            } => {
                // Registration is acked (client_seq 0) so observers know
                // they are live before events they must not miss occur.
                let _ = sink.send(encode_msg::<E>(&ServerMsg::Ack {
                    client_seq: 0,
                    seq: 0,
                }));
                let entry = sinks.entry(session).or_insert_with(SessionSinks::new);
                // A drop-copy observer can join from any point still held.
                // Replay before registering, so a dead observer never lands
                // in the live set (and ordering stays backfill-then-live).
                let mut alive = true;
                for bytes in entry.replay_after(from_event_seq) {
                    if sink.send(bytes).is_err() {
                        alive = false;
                        break;
                    }
                }
                if alive {
                    entry.watchers.push(sink);
                }
            }
            Req::Cmd {
                session,
                client_seq,
                cmd,
            } => {
                let flow = flows
                    .entry(session)
                    .or_insert_with(|| SessionFlow::new(config.window));
                let verdict = flow.admit(client_seq);
                match verdict {
                    Admit::Accept => {
                        let input = GatewayInput::Client {
                            session,
                            client_seq,
                            cmd,
                        };
                        match node.submit(input) {
                            Ok((seq, outputs)) => {
                                flow.on_acked();
                                send_to_cmd::<E>(
                                    &mut sinks,
                                    session,
                                    &ServerMsg::Ack {
                                        client_seq,
                                        seq: seq.as_u64(),
                                    },
                                );
                                route_events(&mut sinks, outputs, cap);
                            }
                            Err(e) => {
                                // Journal failure: the input was not
                                // durably sequenced; drop the connection
                                // rather than lie with an ack.
                                eprintln!("gateway: submit failed: {e}");
                                sinks.remove(&session);
                            }
                        }
                    }
                    Admit::Duplicate => send_to_cmd::<E>(
                        &mut sinks,
                        session,
                        &ServerMsg::Rejected {
                            client_seq,
                            reason: RejectReason::Duplicate,
                        },
                    ),
                    Admit::Gap { expected } => send_to_cmd::<E>(
                        &mut sinks,
                        session,
                        &ServerMsg::Rejected {
                            client_seq,
                            reason: RejectReason::Gap { expected },
                        },
                    ),
                    Admit::Throttled => send_to_cmd::<E>(
                        &mut sinks,
                        session,
                        &ServerMsg::Rejected {
                            client_seq,
                            reason: RejectReason::Throttled,
                        },
                    ),
                }
            }
            Req::Close { session } => {
                if let Some(entry) = sinks.get_mut(&session) {
                    entry.cmd = None;
                }
                // Deterministic cancel-on-disconnect: the close itself is a
                // sequenced input, identical on every replica and replay.
                match node.submit(GatewayInput::SessionClosed { session }) {
                    Ok((_seq, outputs)) => route_events(&mut sinks, outputs, cap),
                    Err(e) => eprintln!("gateway: session-close submit failed: {e}"),
                }
            }
        }
    }
}

fn encode_msg<E: Encode>(msg: &ServerMsg<E>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4 + msg.encoded_len());
    bytes.extend_from_slice(&(msg.encoded_len() as u32).to_le_bytes());
    msg.encode(&mut bytes);
    bytes
}

fn send_to_cmd<E: Encode>(
    sinks: &mut BTreeMap<u64, SessionSinks>,
    session: u64,
    msg: &ServerMsg<E>,
) {
    if let Some(entry) = sinks.get_mut(&session) {
        let bytes = encode_msg(msg);
        if let Some(cmd) = &entry.cmd {
            if cmd.send(bytes).is_err() {
                entry.cmd = None;
            }
        }
    }
}

fn route_events<E: Encode>(
    sinks: &mut BTreeMap<u64, SessionSinks>,
    outputs: Vec<Addressed<E>>,
    outbox_capacity: usize,
) {
    for addressed in outputs {
        let session = addressed.session;
        // Create the session entry even with no live connection: the event
        // still gets an event_seq and lands in the outbox, so a client that
        // reconnects later can replay it. This is the fix for P1.2 — events
        // for offline sessions are retained, not silently dropped.
        let entry = sinks.entry(session).or_insert_with(SessionSinks::new);
        let event_seq = entry.next_event_seq;
        entry.next_event_seq += 1;
        let bytes = encode_msg(&ServerMsg::Event {
            event_seq,
            event: addressed.event,
        });
        // Retain for reconnect replay, bounded (drop the oldest past the cap).
        entry.outbox.push_back((event_seq, bytes.clone()));
        while entry.outbox.len() > outbox_capacity {
            entry.outbox.pop_front();
        }
        if let Some(cmd) = &entry.cmd {
            if cmd.send(bytes.clone()).is_err() {
                entry.cmd = None;
            }
        }
        entry.watchers.retain(|w| w.send(bytes.clone()).is_ok());
    }
}

fn connection_loop<C: Encode + Decode>(stream: TcpStream, req_tx: mpsc::Sender<Req<C>>) {
    let Ok(write_half) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(stream);

    // First message declares the connection's role.
    let hello: ClientMsg<C> = match read_msg(&mut reader) {
        Ok(Some(msg)) => msg,
        _ => return,
    };

    let (sink_tx, sink_rx) = mpsc::channel::<Vec<u8>>();
    let mut write_half = write_half;
    std::thread::spawn(move || {
        use std::io::Write;
        while let Ok(bytes) = sink_rx.recv() {
            if write_half.write_all(&bytes).is_err() {
                return;
            }
            let _ = write_half.flush();
        }
    });

    let session = match hello {
        ClientMsg::Hello {
            session,
            from_event_seq,
        } => {
            let _ = req_tx.send(Req::Attach {
                session,
                from_event_seq,
                sink: sink_tx,
            });
            session
        }
        ClientMsg::DropCopy {
            session,
            from_event_seq,
        } => {
            let _ = req_tx.send(Req::Watch {
                session,
                from_event_seq,
                sink: sink_tx,
            });
            // Observers only listen; hold the connection open until EOF.
            while let Ok(Some(_)) = read_msg::<ClientMsg<C>>(&mut reader) {}
            return;
        }
        ClientMsg::Cmd { .. } => return, // protocol violation: no Hello
    };

    loop {
        match read_msg::<ClientMsg<C>>(&mut reader) {
            Ok(Some(ClientMsg::Cmd { client_seq, cmd })) => {
                if req_tx
                    .send(Req::Cmd {
                        session,
                        client_seq,
                        cmd,
                    })
                    .is_err()
                {
                    return;
                }
            }
            Ok(Some(_)) => break, // repeated Hello: protocol violation
            Ok(None) | Err(_) => break,
        }
    }
    let _ = req_tx.send(Req::Close { session });
}
