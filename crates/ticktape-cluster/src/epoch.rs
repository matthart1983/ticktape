//! The epoch fence: the first frame a new leader sequences.
//!
//! `EpochChange { epoch, first_seq }` is appended at `first_seq` by the
//! winner of the election for `epoch`. Its meaning:
//!
//! - Everything the *previous* epoch assigned at seqs `>= first_seq` is
//!   fenced off — discarded history, never merged. (In Tier 2 the election
//!   guarantees nothing committed lies there; in Tier 1 that window is the
//!   documented bounded loss.)
//! - Replicas that already applied fenced frames must rebuild from the
//!   canonical stream (snapshot + replay); replicas at or behind the fence
//!   just keep consuming.
//! - Stream messages carrying an epoch older than the highest `EpochChange`
//!   seen are rejected — this is what makes a deposed, still-running
//!   leader harmless.

use ticktape_core::{decode_all, encode_to_vec, CodecError, Frame, FrameKind, Seq, Timestamp};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochChange {
    pub epoch: u64,
    /// The seq of the EpochChange frame itself — the new epoch's first seq.
    pub first_seq: Seq,
    /// The leader's application input-schema version ([`Service::SCHEMA_VERSION`]).
    /// A replica whose own version differs rejects the stream at this fence.
    pub schema_version: u32,
}

impl EpochChange {
    /// Build the fence frame (sequenced at `first_seq`). Payload is
    /// `(epoch, first_seq, schema_version)`.
    pub fn to_frame(self, timestamp: Timestamp, stream_id: u16) -> Frame {
        Frame::new(
            self.first_seq,
            timestamp,
            stream_id,
            FrameKind::EpochChange,
            encode_to_vec(&(self.epoch, self.first_seq.0, self.schema_version)),
        )
    }

    /// Decode a fence frame. Tolerant of the older 2-field payload (no schema
    /// version) written before this field existed — those decode with
    /// `schema_version = 0`.
    pub fn from_frame(frame: &Frame) -> Result<EpochChange, CodecError> {
        debug_assert_eq!(frame.kind, FrameKind::EpochChange);
        // 3-field payload is 8+8+4 = 20 bytes; the legacy 2-field one is 16.
        if frame.payload.len() >= 20 {
            let (epoch, first_seq, schema_version): (u64, u64, u32) = decode_all(&frame.payload)?;
            Ok(EpochChange {
                epoch,
                first_seq: Seq(first_seq),
                schema_version,
            })
        } else {
            let (epoch, first_seq): (u64, u64) = decode_all(&frame.payload)?;
            Ok(EpochChange {
                epoch,
                first_seq: Seq(first_seq),
                schema_version: 0,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip() {
        let change = EpochChange {
            epoch: 7,
            first_seq: Seq(1234),
            schema_version: 3,
        };
        let frame = change.to_frame(Timestamp(9), 1);
        assert_eq!(frame.seq, Seq(1234));
        assert_eq!(frame.kind, FrameKind::EpochChange);
        assert_eq!(EpochChange::from_frame(&frame).unwrap(), change);
    }

    #[test]
    fn decodes_legacy_two_field_fence_as_schema_zero() {
        // A fence written before schema_version existed (payload = (epoch,
        // first_seq)) must still decode, defaulting to schema 0.
        let legacy = Frame::new(
            Seq(5),
            Timestamp(1),
            1,
            FrameKind::EpochChange,
            encode_to_vec(&(9u64, 5u64)),
        );
        let change = EpochChange::from_frame(&legacy).unwrap();
        assert_eq!(change.epoch, 9);
        assert_eq!(change.first_seq, Seq(5));
        assert_eq!(change.schema_version, 0);
    }
}
