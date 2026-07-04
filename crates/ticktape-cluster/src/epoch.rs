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
}

impl EpochChange {
    /// Build the fence frame (sequenced at `first_seq`).
    pub fn to_frame(self, timestamp: Timestamp, stream_id: u16) -> Frame {
        Frame::new(
            self.first_seq,
            timestamp,
            stream_id,
            FrameKind::EpochChange,
            encode_to_vec(&(self.epoch, self.first_seq.0)),
        )
    }

    pub fn from_frame(frame: &Frame) -> Result<EpochChange, CodecError> {
        debug_assert_eq!(frame.kind, FrameKind::EpochChange);
        let (epoch, first_seq): (u64, u64) = decode_all(&frame.payload)?;
        Ok(EpochChange {
            epoch,
            first_seq: Seq(first_seq),
        })
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
        };
        let frame = change.to_frame(Timestamp(9), 1);
        assert_eq!(frame.seq, Seq(1234));
        assert_eq!(frame.kind, FrameKind::EpochChange);
        assert_eq!(EpochChange::from_frame(&frame).unwrap(), change);
    }
}
