//! Gateway envelopes and the client/server message protocol.
//!
//! [`GatewayInput`] and [`Addressed`] are the *sequenced* types — they go
//! through the journal, so session identity and client seqs are part of
//! deterministic history. [`ClientMsg`]/[`ServerMsg`] are the *edge*
//! protocol, length-prefixed over TCP.
//!
//! These are generic over the application's command/event types, and the
//! derive macros don't support generics yet, so the codecs are manual —
//! same canonical rules (little-endian, u16 discriminants in declaration
//! order).

use std::io::{Read, Write};
use ticktape_core::{CodecError, Decode, Encode};

/// What the gateway injects into the sequencer on behalf of clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayInput<C> {
    /// An admitted client command, tagged with its session and the
    /// client's own sequence number (journaled ⇒ dedup state is
    /// deterministic and survives gateway restarts).
    Client {
        session: u64,
        client_seq: u64,
        cmd: C,
    },
    /// The session's connection dropped: the service reacts
    /// deterministically (cancel-on-disconnect).
    SessionClosed { session: u64 },
}

impl<C: Encode> Encode for GatewayInput<C> {
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            GatewayInput::Client {
                session,
                client_seq,
                cmd,
            } => {
                0u16.encode(out);
                session.encode(out);
                client_seq.encode(out);
                cmd.encode(out);
            }
            GatewayInput::SessionClosed { session } => {
                1u16.encode(out);
                session.encode(out);
            }
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            GatewayInput::Client { cmd, .. } => 2 + 8 + 8 + cmd.encoded_len(),
            GatewayInput::SessionClosed { .. } => 2 + 8,
        }
    }
}

impl<C: Decode> Decode for GatewayInput<C> {
    fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        match u16::decode(buf)? {
            0 => Ok(GatewayInput::Client {
                session: u64::decode(buf)?,
                client_seq: u64::decode(buf)?,
                cmd: C::decode(buf)?,
            }),
            1 => Ok(GatewayInput::SessionClosed {
                session: u64::decode(buf)?,
            }),
            _ => Err(CodecError::InvalidValue("GatewayInput discriminant")),
        }
    }
}

/// An output event addressed to the session that must see it. A service
/// emits one `Addressed` per interested party (an exchange addresses a
/// trade to both taker and maker sessions).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Addressed<E> {
    pub session: u64,
    pub event: E,
}

impl<E: Encode> Encode for Addressed<E> {
    fn encode(&self, out: &mut Vec<u8>) {
        self.session.encode(out);
        self.event.encode(out);
    }

    fn encoded_len(&self) -> usize {
        8 + self.event.encoded_len()
    }
}

impl<E: Decode> Decode for Addressed<E> {
    fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        Ok(Addressed {
            session: u64::decode(buf)?,
            event: E::decode(buf)?,
        })
    }
}

/// Why the gateway refused a command (edge protocol, not sequenced).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// Retry of an already-admitted seq — the effect already happened.
    Duplicate,
    /// Client seq skipped ahead; resynchronize at `expected`.
    Gap { expected: u64 },
    /// Window full; retry the same seq after acks drain.
    Throttled,
}

/// Client → gateway messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientMsg<C> {
    /// First message on a command connection. Reconnecting with the same
    /// session id resumes its dedup state.
    Hello {
        session: u64,
    },
    /// First message on an observer connection: receive a copy of every
    /// event addressed to `session`.
    DropCopy {
        session: u64,
    },
    Cmd {
        client_seq: u64,
        cmd: C,
    },
}

impl<C: Encode> Encode for ClientMsg<C> {
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            ClientMsg::Hello { session } => {
                0u16.encode(out);
                session.encode(out);
            }
            ClientMsg::DropCopy { session } => {
                1u16.encode(out);
                session.encode(out);
            }
            ClientMsg::Cmd { client_seq, cmd } => {
                2u16.encode(out);
                client_seq.encode(out);
                cmd.encode(out);
            }
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            ClientMsg::Hello { .. } | ClientMsg::DropCopy { .. } => 2 + 8,
            ClientMsg::Cmd { cmd, .. } => 2 + 8 + cmd.encoded_len(),
        }
    }
}

impl<C: Decode> Decode for ClientMsg<C> {
    fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        match u16::decode(buf)? {
            0 => Ok(ClientMsg::Hello {
                session: u64::decode(buf)?,
            }),
            1 => Ok(ClientMsg::DropCopy {
                session: u64::decode(buf)?,
            }),
            2 => Ok(ClientMsg::Cmd {
                client_seq: u64::decode(buf)?,
                cmd: C::decode(buf)?,
            }),
            _ => Err(CodecError::InvalidValue("ClientMsg discriminant")),
        }
    }
}

/// Gateway → client messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMsg<E> {
    /// `client_seq` was sequenced at global `seq`.
    Ack { client_seq: u64, seq: u64 },
    /// An event addressed to this session (or watched via drop-copy).
    Event(E),
    Rejected {
        client_seq: u64,
        reason: RejectReason,
    },
}

impl<E: Encode> Encode for ServerMsg<E> {
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            ServerMsg::Ack { client_seq, seq } => {
                0u16.encode(out);
                client_seq.encode(out);
                seq.encode(out);
            }
            ServerMsg::Event(event) => {
                1u16.encode(out);
                event.encode(out);
            }
            ServerMsg::Rejected { client_seq, reason } => {
                2u16.encode(out);
                client_seq.encode(out);
                match reason {
                    RejectReason::Duplicate => 0u16.encode(out),
                    RejectReason::Gap { expected } => {
                        1u16.encode(out);
                        expected.encode(out);
                    }
                    RejectReason::Throttled => 2u16.encode(out),
                }
            }
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            ServerMsg::Ack { .. } => 2 + 8 + 8,
            ServerMsg::Event(event) => 2 + event.encoded_len(),
            ServerMsg::Rejected { reason, .. } => {
                2 + 8
                    + 2
                    + if matches!(reason, RejectReason::Gap { .. }) {
                        8
                    } else {
                        0
                    }
            }
        }
    }
}

impl<E: Decode> Decode for ServerMsg<E> {
    fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        match u16::decode(buf)? {
            0 => Ok(ServerMsg::Ack {
                client_seq: u64::decode(buf)?,
                seq: u64::decode(buf)?,
            }),
            1 => Ok(ServerMsg::Event(E::decode(buf)?)),
            2 => {
                let client_seq = u64::decode(buf)?;
                let reason = match u16::decode(buf)? {
                    0 => RejectReason::Duplicate,
                    1 => RejectReason::Gap {
                        expected: u64::decode(buf)?,
                    },
                    2 => RejectReason::Throttled,
                    _ => return Err(CodecError::InvalidValue("RejectReason discriminant")),
                };
                Ok(ServerMsg::Rejected { client_seq, reason })
            }
            _ => Err(CodecError::InvalidValue("ServerMsg discriminant")),
        }
    }
}

/// Write one length-prefixed message.
pub fn write_msg<T: Encode>(w: &mut impl Write, msg: &T) -> std::io::Result<()> {
    let mut bytes = Vec::with_capacity(4 + msg.encoded_len());
    bytes.extend_from_slice(&(msg.encoded_len() as u32).to_le_bytes());
    msg.encode(&mut bytes);
    w.write_all(&bytes)
}

/// Read one length-prefixed message; `Ok(None)` on clean EOF.
pub fn read_msg<T: Decode>(r: &mut impl Read) -> std::io::Result<Option<T>> {
    let mut len_bytes = [0u8; 4];
    match r.read_exact(&mut len_bytes) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > 16 * 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "message too large",
        ));
    }
    let mut bytes = vec![0u8; len];
    r.read_exact(&mut bytes)?;
    ticktape_core::decode_all(&bytes)
        .map(Some)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ticktape_core::{decode_all, encode_to_vec};

    #[test]
    fn envelope_roundtrips() {
        let input: GatewayInput<u32> = GatewayInput::Client {
            session: 7,
            client_seq: 3,
            cmd: 99,
        };
        let bytes = encode_to_vec(&input);
        assert_eq!(bytes.len(), input.encoded_len());
        assert_eq!(decode_all::<GatewayInput<u32>>(&bytes).unwrap(), input);

        let closed: GatewayInput<u32> = GatewayInput::SessionClosed { session: 7 };
        assert_eq!(
            decode_all::<GatewayInput<u32>>(&encode_to_vec(&closed)).unwrap(),
            closed
        );

        let addressed = Addressed {
            session: 5,
            event: String::from("fill"),
        };
        assert_eq!(
            decode_all::<Addressed<String>>(&encode_to_vec(&addressed)).unwrap(),
            addressed
        );
    }

    #[test]
    fn protocol_roundtrips_over_a_stream() {
        let msgs: Vec<ServerMsg<String>> = vec![
            ServerMsg::Ack {
                client_seq: 1,
                seq: 42,
            },
            ServerMsg::Event("hello".into()),
            ServerMsg::Rejected {
                client_seq: 2,
                reason: RejectReason::Gap { expected: 2 },
            },
        ];
        let mut wire = Vec::new();
        for msg in &msgs {
            write_msg(&mut wire, msg).unwrap();
        }
        let mut cursor = wire.as_slice();
        for expected in &msgs {
            let got: ServerMsg<String> = read_msg(&mut cursor).unwrap().unwrap();
            assert_eq!(&got, expected);
        }
        assert!(read_msg::<ServerMsg<String>>(&mut cursor)
            .unwrap()
            .is_none());
    }
}
