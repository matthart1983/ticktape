//! The reliability core: a pure, socket-free state machine that turns an
//! unordered, lossy, duplicated packet stream back into the exact in-order
//! frame sequence.
//!
//! Everything the transport promises lives here, where it can be fuzzed
//! deterministically: A/B duplicate discard (a second copy of a seq is
//! simply ignored), out-of-order buffering, gap detection (from both
//! buffered-ahead frames and heartbeat high-water marks), and late-join
//! catch-up (start `from` any seq and gap-fill the history).
//!
//! The socket layer's whole job is: feed packets in, pop frames out, and
//! when [`Reassembler::gap`] reports a missing range, fetch it from the
//! retransmitter and feed that in too.

use crate::wire::Packet;
use crate::TransportError;
use std::collections::BTreeMap;
use ticktape_core::{Frame, Seq};

/// Bound on buffered out-of-order frames. Past this, farthest-ahead frames
/// are dropped — they will be re-fetched by gap-fill once the stream
/// advances, so a hostile or wildly reordered feed costs memory O(cap),
/// never unbounded.
const PENDING_CAP: usize = 64 * 1024;

pub struct Reassembler {
    /// Locked on the first packet seen; later sessions are rejected.
    session: Option<u64>,
    /// Next seq to deliver.
    next: u64,
    /// Buffered frames with seq > next.
    pending: BTreeMap<u64, Frame>,
    /// Highest seq known to exist (from frames seen and heartbeats):
    /// `announced_next - 1` frames exist upstream.
    announced_next: u64,
}

impl Reassembler {
    /// Start expecting `from` as the first delivered seq (use `Seq(1)` to
    /// follow a stream from genesis; anything later for a warm join).
    pub fn new(from: Seq) -> Self {
        let start = from.0.max(1);
        Reassembler {
            session: None,
            next: start,
            pending: BTreeMap::new(),
            announced_next: start,
        }
    }

    /// The session this stream locked onto, once known.
    pub fn session(&self) -> Option<u64> {
        self.session
    }

    /// Next seq that will be delivered.
    pub fn next_expected(&self) -> Seq {
        Seq(self.next)
    }

    /// Highest seq known to exist upstream (from data + heartbeats). A
    /// follower's lag is `announced_high_water - (next_expected - 1)`.
    pub fn announced_high_water(&self) -> Seq {
        Seq(self.announced_next.saturating_sub(1))
    }

    /// Feed one packet (from either feed channel, or a retransmit reply).
    pub fn ingest(&mut self, packet: Packet) -> Result<(), TransportError> {
        match self.session {
            None => self.session = Some(packet.session()),
            Some(expected) if expected != packet.session() => {
                return Err(TransportError::SessionMismatch {
                    expected,
                    got: packet.session(),
                });
            }
            Some(_) => {}
        }
        match packet {
            Packet::Heartbeat { next_seq, .. } => {
                self.announced_next = self.announced_next.max(next_seq.0);
            }
            Packet::Data { frames, .. } => {
                for frame in frames {
                    let seq = frame.seq.0;
                    self.announced_next = self.announced_next.max(seq + 1);
                    if seq < self.next || self.pending.contains_key(&seq) {
                        continue; // duplicate (A/B or replayed) — discard
                    }
                    self.pending.insert(seq, frame);
                }
                // Bound memory: drop the farthest-ahead frames; gap-fill
                // will recover them when the stream reaches that point.
                while self.pending.len() > PENDING_CAP {
                    self.pending.pop_last();
                }
            }
        }
        Ok(())
    }

    /// Pop the next in-order frame, if it has arrived.
    pub fn next_frame(&mut self) -> Option<Frame> {
        let frame = self.pending.remove(&self.next)?;
        self.next += 1;
        Some(frame)
    }

    /// The currently known missing range `(from, count)`: frames that are
    /// proven to exist (something after them arrived, or a heartbeat
    /// announced past them) but have not been received. `None` means the
    /// stream is intact so far.
    pub fn gap(&self) -> Option<(Seq, u64)> {
        let until = match self.pending.keys().next() {
            // Something is buffered ahead: everything up to it is missing.
            Some(&first_pending) => first_pending,
            // Nothing buffered: the heartbeat high-water proves what exists.
            None => self.announced_next,
        };
        if until > self.next {
            Some((Seq(self.next), until - self.next))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ticktape_core::{FrameKind, Timestamp};

    const SESSION: u64 = 7;

    fn frame(seq: u64) -> Frame {
        Frame::new(
            Seq(seq),
            Timestamp(seq),
            1,
            FrameKind::Input,
            seq.to_le_bytes().to_vec(),
        )
    }

    fn data(seqs: std::ops::RangeInclusive<u64>) -> Packet {
        Packet::Data {
            session: SESSION,
            frames: seqs.map(frame).collect(),
        }
    }

    fn drain(r: &mut Reassembler) -> Vec<u64> {
        std::iter::from_fn(|| r.next_frame())
            .map(|f| f.seq.0)
            .collect()
    }

    #[test]
    fn in_order_delivery() {
        let mut r = Reassembler::new(Seq(1));
        r.ingest(data(1..=3)).unwrap();
        assert_eq!(drain(&mut r), vec![1, 2, 3]);
        assert_eq!(r.gap(), None);
    }

    #[test]
    fn ab_duplicates_discarded() {
        let mut r = Reassembler::new(Seq(1));
        r.ingest(data(1..=2)).unwrap(); // channel A
        r.ingest(data(1..=2)).unwrap(); // channel B copy
        r.ingest(data(2..=3)).unwrap(); // overlapping repackaging
        assert_eq!(drain(&mut r), vec![1, 2, 3]);
    }

    #[test]
    fn reorder_buffers_until_gap_filled() {
        let mut r = Reassembler::new(Seq(1));
        r.ingest(data(3..=4)).unwrap();
        assert_eq!(drain(&mut r), Vec::<u64>::new());
        assert_eq!(r.gap(), Some((Seq(1), 2)));
        r.ingest(data(1..=2)).unwrap(); // gap-fill reply
        assert_eq!(drain(&mut r), vec![1, 2, 3, 4]);
        assert_eq!(r.gap(), None);
    }

    #[test]
    fn heartbeat_reveals_tail_loss() {
        let mut r = Reassembler::new(Seq(1));
        r.ingest(data(1..=2)).unwrap();
        assert_eq!(drain(&mut r), vec![1, 2]);
        assert_eq!(r.gap(), None, "nothing known to be missing yet");
        // Frames 3..=5 were lost entirely; the heartbeat proves they exist.
        r.ingest(Packet::Heartbeat {
            session: SESSION,
            next_seq: Seq(6),
        })
        .unwrap();
        assert_eq!(r.gap(), Some((Seq(3), 3)));
    }

    #[test]
    fn late_join_requests_history() {
        let mut r = Reassembler::new(Seq(1));
        r.ingest(data(100..=101)).unwrap(); // joined mid-stream
        assert_eq!(r.gap(), Some((Seq(1), 99)));
    }

    #[test]
    fn warm_join_skips_history() {
        let mut r = Reassembler::new(Seq(100));
        r.ingest(data(100..=101)).unwrap();
        assert_eq!(drain(&mut r), vec![100, 101]);
        assert_eq!(r.gap(), None);
    }

    #[test]
    fn session_locking() {
        let mut r = Reassembler::new(Seq(1));
        r.ingest(data(1..=1)).unwrap();
        let foreign = Packet::Data {
            session: SESSION + 1,
            frames: vec![frame(2)],
        };
        assert!(matches!(
            r.ingest(foreign),
            Err(TransportError::SessionMismatch { .. })
        ));
    }
}
