//! The `Frame`: the framework-owned record layout used on the wire and in
//! the journal.
//!
//! Fixed little-endian header, opaque app-encoded payload, CRC32C over both:
//!
//! ```text
//!  offset size field
//!    0     8   seq           u64  monotonic global sequence number
//!    8     8   timestamp     u64  sequencer-assigned nanos since epoch
//!   16     2   stream_id     u16  logical stream/topic
//!   18     2   kind          u16  frame kind
//!   20     4   payload_len   u32  payload length in bytes
//!   24     4   header_crc    u32  CRC32C of bytes [0,24)
//!   28   ...   payload
//!   ..     4   payload_crc   u32  CRC32C of payload
//! ```
//!
//! App `Input`/`Output` payloads are encoded by the application's codec; the
//! frame layout is framework-owned and stable, so framework wire stability
//! is decoupled from app schema evolution.

use crate::crc32c::crc32c;
use crate::seq::{Seq, Timestamp};
use core::fmt;

/// Size of the fixed frame header, bytes `[0, 28)`.
pub const FRAME_HEADER_LEN: usize = 28;

/// Hard sanity cap on payload size (64 MiB). A `payload_len` above this is
/// treated as corruption rather than an allocation request.
pub const MAX_PAYLOAD_LEN: u32 = 64 * 1024 * 1024;

/// What a frame carries. Values are stable wire constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum FrameKind {
    /// Application command (payload = encoded `Service::Input`).
    Input = 0x0001,
    /// Application event (payload = encoded `Service::Output`).
    Output = 0x0002,
    /// Injected time event; payload empty, `timestamp` authoritative.
    Tick = 0x0010,
    /// A deterministic timer reached its deadline. Payload = encoded timer
    /// `id` (u64); the sequencer injects this as a sequenced input when
    /// sequenced time passes the timer's `at`, so firing is journaled and
    /// replays identically on every replica.
    TimerFired = 0x0013,
    /// Gateway session lifecycle.
    SessionOpen = 0x0011,
    SessionClose = 0x0012,
    /// Marks a seq at which a snapshot was taken.
    SnapshotMark = 0x0020,
    /// Leadership epoch transition.
    EpochChange = 0x0030,
    /// Sequencer liveness + current high-water seq.
    Heartbeat = 0x00FF,
}

impl FrameKind {
    pub fn from_u16(v: u16) -> Option<FrameKind> {
        Some(match v {
            0x0001 => FrameKind::Input,
            0x0002 => FrameKind::Output,
            0x0010 => FrameKind::Tick,
            0x0013 => FrameKind::TimerFired,
            0x0011 => FrameKind::SessionOpen,
            0x0012 => FrameKind::SessionClose,
            0x0020 => FrameKind::SnapshotMark,
            0x0030 => FrameKind::EpochChange,
            0x00FF => FrameKind::Heartbeat,
            _ => return None,
        })
    }
}

/// Errors from decoding a frame out of a byte buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    /// Fewer bytes remain than a complete frame; distinguishes a clean or
    /// torn tail from mid-stream corruption at the journal layer.
    Truncated,
    /// Header CRC mismatch.
    HeaderCrc,
    /// Payload CRC mismatch.
    PayloadCrc,
    /// Unknown `kind` value.
    UnknownKind(u16),
    /// `payload_len` exceeds the sanity cap.
    PayloadTooLarge(u32),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Truncated => write!(f, "truncated frame"),
            FrameError::HeaderCrc => write!(f, "frame header CRC mismatch"),
            FrameError::PayloadCrc => write!(f, "frame payload CRC mismatch"),
            FrameError::UnknownKind(k) => write!(f, "unknown frame kind {k:#06x}"),
            FrameError::PayloadTooLarge(n) => write!(f, "payload_len {n} exceeds cap"),
        }
    }
}

impl std::error::Error for FrameError {}

/// One sequenced record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub seq: Seq,
    pub timestamp: Timestamp,
    pub stream_id: u16,
    pub kind: FrameKind,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(
        seq: Seq,
        timestamp: Timestamp,
        stream_id: u16,
        kind: FrameKind,
        payload: Vec<u8>,
    ) -> Frame {
        assert!(
            payload.len() <= MAX_PAYLOAD_LEN as usize,
            "payload exceeds MAX_PAYLOAD_LEN"
        );
        Frame {
            seq,
            timestamp,
            stream_id,
            kind,
            payload,
        }
    }

    /// Total encoded size: header + payload + payload CRC.
    pub fn encoded_len(&self) -> usize {
        FRAME_HEADER_LEN + self.payload.len() + 4
    }

    /// Append the encoded frame to `out`.
    pub fn write_to(&self, out: &mut Vec<u8>) {
        let start = out.len();
        out.extend_from_slice(&self.seq.0.to_le_bytes());
        out.extend_from_slice(&self.timestamp.0.to_le_bytes());
        out.extend_from_slice(&self.stream_id.to_le_bytes());
        out.extend_from_slice(&(self.kind as u16).to_le_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        let header_crc = crc32c(&out[start..start + 24]);
        out.extend_from_slice(&header_crc.to_le_bytes());
        out.extend_from_slice(&self.payload);
        out.extend_from_slice(&crc32c(&self.payload).to_le_bytes());
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        self.write_to(&mut out);
        out
    }

    /// Decode one frame from the front of `buf`, advancing it. On error the
    /// buffer position is unspecified; callers treat the position where the
    /// frame *started* as the corruption point.
    pub fn read_from(buf: &mut &[u8]) -> Result<Frame, FrameError> {
        if buf.len() < FRAME_HEADER_LEN {
            return Err(FrameError::Truncated);
        }
        let header = &buf[..24];
        let stored_header_crc = u32::from_le_bytes(buf[24..28].try_into().unwrap());
        if crc32c(header) != stored_header_crc {
            return Err(FrameError::HeaderCrc);
        }
        let seq = Seq(u64::from_le_bytes(header[0..8].try_into().unwrap()));
        let timestamp = Timestamp(u64::from_le_bytes(header[8..16].try_into().unwrap()));
        let stream_id = u16::from_le_bytes(header[16..18].try_into().unwrap());
        let kind_raw = u16::from_le_bytes(header[18..20].try_into().unwrap());
        let payload_len = u32::from_le_bytes(header[20..24].try_into().unwrap());
        if payload_len > MAX_PAYLOAD_LEN {
            return Err(FrameError::PayloadTooLarge(payload_len));
        }
        let kind = FrameKind::from_u16(kind_raw).ok_or(FrameError::UnknownKind(kind_raw))?;
        let total = FRAME_HEADER_LEN + payload_len as usize + 4;
        if buf.len() < total {
            return Err(FrameError::Truncated);
        }
        let payload = buf[FRAME_HEADER_LEN..FRAME_HEADER_LEN + payload_len as usize].to_vec();
        let stored_payload_crc = u32::from_le_bytes(buf[total - 4..total].try_into().unwrap());
        if crc32c(&payload) != stored_payload_crc {
            return Err(FrameError::PayloadCrc);
        }
        *buf = &buf[total..];
        Ok(Frame {
            seq,
            timestamp,
            stream_id,
            kind,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Frame {
        Frame::new(
            Seq(42),
            Timestamp(1_700_000_000_000_000_000),
            7,
            FrameKind::Input,
            b"hello frame".to_vec(),
        )
    }

    #[test]
    fn roundtrip() {
        let frame = sample();
        let bytes = frame.to_bytes();
        assert_eq!(bytes.len(), frame.encoded_len());
        let mut cursor = bytes.as_slice();
        let back = Frame::read_from(&mut cursor).unwrap();
        assert!(cursor.is_empty());
        assert_eq!(back, frame);
    }

    #[test]
    fn empty_payload_roundtrip() {
        let frame = Frame::new(Seq(1), Timestamp(5), 0, FrameKind::Tick, Vec::new());
        let mut cursor = frame.to_bytes();
        let mut slice = cursor.as_mut_slice() as &[u8];
        assert_eq!(Frame::read_from(&mut slice).unwrap(), frame);
    }

    #[test]
    fn detects_header_corruption() {
        let mut bytes = sample().to_bytes();
        bytes[3] ^= 0xFF; // flip a bit inside seq
        let mut cursor = bytes.as_slice();
        assert_eq!(
            Frame::read_from(&mut cursor).unwrap_err(),
            FrameError::HeaderCrc
        );
    }

    #[test]
    fn detects_payload_corruption() {
        let mut bytes = sample().to_bytes();
        let idx = FRAME_HEADER_LEN + 2;
        bytes[idx] ^= 0xFF;
        let mut cursor = bytes.as_slice();
        assert_eq!(
            Frame::read_from(&mut cursor).unwrap_err(),
            FrameError::PayloadCrc
        );
    }

    #[test]
    fn header_layout_matches_wire_spec() {
        // Pins the exact byte offsets documented in WIRE.md §1 so the code
        // and the spec cannot drift apart silently.
        use crate::crc32c::crc32c;
        let f = Frame::new(
            Seq(0x0102_0304_0506_0708),
            Timestamp(0x1112_1314_1516_1718),
            0x2122,
            FrameKind::Input,
            b"xy".to_vec(),
        );
        let b = f.to_bytes();
        assert_eq!(b.len(), 28 + 2 + 4);
        assert_eq!(&b[0..8], &0x0102_0304_0506_0708u64.to_le_bytes(), "seq @0");
        assert_eq!(
            &b[8..16],
            &0x1112_1314_1516_1718u64.to_le_bytes(),
            "timestamp @8"
        );
        assert_eq!(&b[16..18], &0x2122u16.to_le_bytes(), "stream_id @16");
        assert_eq!(&b[18..20], &0x0001u16.to_le_bytes(), "kind @18 (Input)");
        assert_eq!(&b[20..24], &2u32.to_le_bytes(), "payload_len @20");
        assert_eq!(
            &b[24..28],
            &crc32c(&b[0..24]).to_le_bytes(),
            "header_crc @24 over [0,24)"
        );
        assert_eq!(&b[28..30], b"xy", "payload @28");
        assert_eq!(&b[30..34], &crc32c(b"xy").to_le_bytes(), "payload_crc");
    }

    #[test]
    fn detects_truncation() {
        let bytes = sample().to_bytes();
        let mut cursor = &bytes[..bytes.len() - 1];
        assert_eq!(
            Frame::read_from(&mut cursor).unwrap_err(),
            FrameError::Truncated
        );
        let mut cursor = &bytes[..10];
        assert_eq!(
            Frame::read_from(&mut cursor).unwrap_err(),
            FrameError::Truncated
        );
    }

    #[test]
    fn two_frames_back_to_back() {
        let a = sample();
        let b = Frame::new(Seq(43), Timestamp(9), 7, FrameKind::Output, b"out".to_vec());
        let mut bytes = a.to_bytes();
        b.write_to(&mut bytes);
        let mut cursor = bytes.as_slice();
        assert_eq!(Frame::read_from(&mut cursor).unwrap(), a);
        assert_eq!(Frame::read_from(&mut cursor).unwrap(), b);
        assert!(cursor.is_empty());
    }
}
