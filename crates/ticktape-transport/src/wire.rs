//! Packet layout for the sequenced stream (MoldUDP64-inspired).
//!
//! ```text
//!  offset size field
//!    0     4   magic       "TKTW"
//!    4     1   kind        0 = data, 1 = heartbeat
//!    5     2   count       u16  frames in this packet (0 for heartbeat)
//!    7     8   session     u64  publisher session id
//!   15     8   first_seq   u64  data: seq of first frame · heartbeat: next
//!                               seq the publisher will assign (high-water+1)
//!   23     4   header_crc  u32  CRC32C of bytes [0,23)
//!   27   ...   frames           back-to-back encoded Frames (data only;
//!                               each frame carries its own CRCs)
//! ```
//!
//! Frames inside one packet are seq-contiguous starting at `first_seq`.
//! The retransmit request (unicast TCP, SoupBinTCP-style) reuses the same
//! header discipline:
//!
//! ```text
//!    0     4   magic       "TKTR"
//!    4     8   session     u64
//!   12     8   from        u64  first seq wanted
//!   20     4   count       u32  how many frames
//!   24     4   crc         u32  CRC32C of bytes [0,24)
//! ```

use crate::TransportError;
use ticktape_core::crc32c::crc32c;
use ticktape_core::{Frame, Seq};

pub const PACKET_MAGIC: &[u8; 4] = b"TKTW";
pub const REQUEST_MAGIC: &[u8; 4] = b"TKTR";
pub const PACKET_HEADER_LEN: usize = 27;
pub const REQUEST_LEN: usize = 28;

/// Keep packets under a conservative ethernet-safe payload size.
pub const MAX_PACKET_BYTES: usize = 1400;

const KIND_DATA: u8 = 0;
const KIND_HEARTBEAT: u8 = 1;

/// One datagram on the sequenced stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    /// Seq-contiguous frames starting at `frames[0].seq`.
    Data { session: u64, frames: Vec<Frame> },
    /// Liveness + high-water: `next_seq` is the next seq the publisher will
    /// assign, so receivers can detect tail loss.
    Heartbeat { session: u64, next_seq: Seq },
}

impl Packet {
    pub fn session(&self) -> u64 {
        match self {
            Packet::Data { session, .. } | Packet::Heartbeat { session, .. } => *session,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let (kind, count, first_seq, frames): (u8, u16, u64, &[Frame]) = match self {
            Packet::Data { frames, .. } => {
                debug_assert!(!frames.is_empty(), "data packet must carry frames");
                (KIND_DATA, frames.len() as u16, frames[0].seq.0, frames)
            }
            Packet::Heartbeat { next_seq, .. } => (KIND_HEARTBEAT, 0, next_seq.0, &[]),
        };
        let mut out = Vec::with_capacity(
            PACKET_HEADER_LEN + frames.iter().map(Frame::encoded_len).sum::<usize>(),
        );
        out.extend_from_slice(PACKET_MAGIC);
        out.push(kind);
        out.extend_from_slice(&count.to_le_bytes());
        out.extend_from_slice(&self.session().to_le_bytes());
        out.extend_from_slice(&first_seq.to_le_bytes());
        let crc = crc32c(&out[0..23]);
        out.extend_from_slice(&crc.to_le_bytes());
        for frame in frames {
            frame.write_to(&mut out);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Packet, TransportError> {
        if bytes.len() < PACKET_HEADER_LEN {
            return Err(TransportError::Corrupt("short packet header"));
        }
        if &bytes[0..4] != PACKET_MAGIC {
            return Err(TransportError::Corrupt("bad packet magic"));
        }
        let stored_crc = u32::from_le_bytes(bytes[23..27].try_into().unwrap());
        if crc32c(&bytes[0..23]) != stored_crc {
            return Err(TransportError::Corrupt("packet header CRC mismatch"));
        }
        let kind = bytes[4];
        let count = u16::from_le_bytes(bytes[5..7].try_into().unwrap());
        let session = u64::from_le_bytes(bytes[7..15].try_into().unwrap());
        let first_seq = u64::from_le_bytes(bytes[15..23].try_into().unwrap());

        match kind {
            KIND_HEARTBEAT => Ok(Packet::Heartbeat {
                session,
                next_seq: Seq(first_seq),
            }),
            KIND_DATA => {
                if count == 0 {
                    return Err(TransportError::Corrupt("data packet with zero frames"));
                }
                let mut cursor = &bytes[PACKET_HEADER_LEN..];
                let mut frames = Vec::with_capacity(count as usize);
                for i in 0..count as u64 {
                    let frame = Frame::read_from(&mut cursor)?;
                    if frame.seq.0 != first_seq + i {
                        return Err(TransportError::Corrupt("non-contiguous frames in packet"));
                    }
                    frames.push(frame);
                }
                if !cursor.is_empty() {
                    return Err(TransportError::Corrupt("trailing bytes after frames"));
                }
                Ok(Packet::Data { session, frames })
            }
            _ => Err(TransportError::Corrupt("unknown packet kind")),
        }
    }
}

/// A retransmit range request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetransmitRequest {
    pub session: u64,
    pub from: Seq,
    pub count: u32,
}

impl RetransmitRequest {
    pub fn encode(&self) -> [u8; REQUEST_LEN] {
        let mut out = [0u8; REQUEST_LEN];
        out[0..4].copy_from_slice(REQUEST_MAGIC);
        out[4..12].copy_from_slice(&self.session.to_le_bytes());
        out[12..20].copy_from_slice(&self.from.0.to_le_bytes());
        out[20..24].copy_from_slice(&self.count.to_le_bytes());
        let crc = crc32c(&out[0..24]);
        out[24..28].copy_from_slice(&crc.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<RetransmitRequest, TransportError> {
        if bytes.len() < REQUEST_LEN {
            return Err(TransportError::Corrupt("short retransmit request"));
        }
        if &bytes[0..4] != REQUEST_MAGIC {
            return Err(TransportError::Corrupt("bad request magic"));
        }
        let stored_crc = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
        if crc32c(&bytes[0..24]) != stored_crc {
            return Err(TransportError::Corrupt("request CRC mismatch"));
        }
        Ok(RetransmitRequest {
            session: u64::from_le_bytes(bytes[4..12].try_into().unwrap()),
            from: Seq(u64::from_le_bytes(bytes[12..20].try_into().unwrap())),
            count: u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ticktape_core::{FrameKind, Timestamp};

    fn frame(seq: u64) -> Frame {
        Frame::new(
            Seq(seq),
            Timestamp(seq * 10),
            1,
            FrameKind::Input,
            format!("payload-{seq}").into_bytes(),
        )
    }

    #[test]
    fn data_packet_roundtrip() {
        let packet = Packet::Data {
            session: 0xABCD,
            frames: vec![frame(5), frame(6), frame(7)],
        };
        let bytes = packet.encode();
        assert_eq!(Packet::decode(&bytes).unwrap(), packet);
    }

    #[test]
    fn heartbeat_roundtrip() {
        let packet = Packet::Heartbeat {
            session: 9,
            next_seq: Seq(1234),
        };
        assert_eq!(Packet::decode(&packet.encode()).unwrap(), packet);
    }

    #[test]
    fn corruption_detected() {
        let packet = Packet::Data {
            session: 1,
            frames: vec![frame(1)],
        };
        let mut bytes = packet.encode();
        bytes[8] ^= 0xFF; // session byte → header CRC must catch
        assert!(matches!(
            Packet::decode(&bytes),
            Err(TransportError::Corrupt(_))
        ));
    }

    #[test]
    fn non_contiguous_frames_rejected() {
        // Hand-build a packet claiming first_seq=1 but carrying seq 1,3.
        let good = Packet::Data {
            session: 1,
            frames: vec![frame(1), frame(2)],
        };
        let mut bytes = good.encode();
        // Overwrite second frame with seq 3's encoding.
        let f1_len = frame(1).encoded_len();
        let f3 = frame(3).to_bytes();
        // frame(2) and frame(3) encode to the same length here.
        bytes.truncate(PACKET_HEADER_LEN + f1_len);
        bytes.extend_from_slice(&f3);
        assert!(matches!(
            Packet::decode(&bytes),
            Err(TransportError::Corrupt("non-contiguous frames in packet"))
        ));
    }

    #[test]
    fn request_roundtrip() {
        let req = RetransmitRequest {
            session: 42,
            from: Seq(100),
            count: 512,
        };
        assert_eq!(RetransmitRequest::decode(&req.encode()).unwrap(), req);
    }
}
