//! Gateway envelopes and the client/server message protocol.
//!
//! [`GatewayInput`] and [`Addressed`] are the *sequenced* types — they go
//! through the journal, so session identity and client seqs are part of
//! deterministic history. [`ClientMsg`]/[`ServerMsg`] are the *edge*
//! protocol, length-prefixed over TCP.
//!
//! These are generic over the application's command/event types; the derive
//! macros support generics, so the codecs are derived — same canonical rules
//! (little-endian, u16 discriminants in declaration order) as any other
//! `#[derive(Encode, Decode)]` type.

use std::io::{Read, Write};
use ticktape_codec::{Decode, Encode};

/// What the gateway injects into the sequencer on behalf of clients.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
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

/// An output event addressed to the session that must see it. A service
/// emits one `Addressed` per interested party (an exchange addresses a
/// trade to both taker and maker sessions).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct Addressed<E> {
    pub session: u64,
    pub event: E,
}

/// Why the gateway refused a command (edge protocol, not sequenced).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum RejectReason {
    /// Retry of an already-admitted seq — the effect already happened.
    Duplicate,
    /// Client seq skipped ahead; resynchronize at `expected`.
    Gap { expected: u64 },
    /// Window full; retry the same seq after acks drain.
    Throttled,
}

/// Client → gateway messages.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum ClientMsg<C> {
    /// First message on a command connection. Reconnecting with the same
    /// session id resumes its dedup state; `from_event_seq` is the last
    /// per-session `event_seq` the client durably saw, so the gateway
    /// replays everything after it from its outbox (`0` = "I have nothing,
    /// send whatever you still hold").
    Hello {
        session: u64,
        from_event_seq: u64,
    },
    /// First message on an observer connection: receive a copy of every
    /// event addressed to `session`, replaying from `from_event_seq` so a
    /// drop-copy observer can join (or rejoin) from any point it still has
    /// in the outbox.
    DropCopy {
        session: u64,
        from_event_seq: u64,
    },
    Cmd {
        client_seq: u64,
        cmd: C,
    },
}

/// Gateway → client messages.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum ServerMsg<E> {
    /// `client_seq` was sequenced at global `seq`.
    Ack { client_seq: u64, seq: u64 },
    /// An event addressed to this session (or watched via drop-copy), tagged
    /// with its monotonic per-session `event_seq`. A client tracks the
    /// highest `event_seq` it has processed and passes it as `from_event_seq`
    /// on reconnect; a gap (received `event_seq` > expected) means the
    /// outbox was trimmed past the client's position and a full resync is
    /// needed.
    Event { event_seq: u64, event: E },
    Rejected {
        client_seq: u64,
        reason: RejectReason,
    },
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
            ServerMsg::Event {
                event_seq: 1,
                event: "hello".into(),
            },
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
