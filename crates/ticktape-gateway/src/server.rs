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
use std::collections::BTreeMap;
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
}

impl Default for ServeConfig {
    fn default() -> Self {
        ServeConfig {
            window: 64,
            resume: Vec::new(),
        }
    }
}

enum Req<C> {
    /// A command connection claimed `session`; replies flow to `sink`.
    Attach {
        session: u64,
        sink: mpsc::Sender<Vec<u8>>,
    },
    /// A drop-copy observer for `session`.
    Watch {
        session: u64,
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

struct SessionSinks {
    cmd: Option<mpsc::Sender<Vec<u8>>>,
    watchers: Vec<mpsc::Sender<Vec<u8>>>,
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

    while let Ok(req) = req_rx.recv() {
        match req {
            Req::Attach { session, sink } => {
                flows
                    .entry(session)
                    .or_insert_with(|| SessionFlow::new(config.window));
                sinks
                    .entry(session)
                    .or_insert_with(|| SessionSinks {
                        cmd: None,
                        watchers: Vec::new(),
                    })
                    .cmd = Some(sink);
            }
            Req::Watch { session, sink } => {
                // Registration is acked (client_seq 0) so observers know
                // they are live before events they must not miss occur.
                let _ = sink.send(encode_msg::<E>(&ServerMsg::Ack {
                    client_seq: 0,
                    seq: 0,
                }));
                sinks
                    .entry(session)
                    .or_insert_with(|| SessionSinks {
                        cmd: None,
                        watchers: Vec::new(),
                    })
                    .watchers
                    .push(sink);
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
                                route_events(&mut sinks, outputs);
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
                    Ok((_seq, outputs)) => route_events(&mut sinks, outputs),
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

fn route_events<E: Encode>(sinks: &mut BTreeMap<u64, SessionSinks>, outputs: Vec<Addressed<E>>) {
    for addressed in outputs {
        let session = addressed.session;
        let Some(entry) = sinks.get_mut(&session) else {
            continue; // no live connection or watcher; events are not queued
        };
        let bytes = encode_msg(&ServerMsg::Event(addressed.event));
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
        ClientMsg::Hello { session } => {
            let _ = req_tx.send(Req::Attach {
                session,
                sink: sink_tx,
            });
            session
        }
        ClientMsg::DropCopy { session } => {
            let _ = req_tx.send(Req::Watch {
                session,
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
