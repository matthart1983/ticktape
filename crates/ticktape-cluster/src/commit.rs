//! Tier 2 commit tracking: an input is committed once a **majority** of
//! replicas have durably journaled it; outputs are withheld until then.
//!
//! Because replica journals are gapless prefixes of the stream, one
//! high-water seq per replica fully describes what it holds, and the
//! commit watermark is simply the majority-th highest high-water: at least
//! `majority` replicas hold everything up to it.

use std::collections::BTreeMap;
use ticktape_core::Seq;

#[derive(Debug)]
pub struct CommitTracker {
    /// Last contiguously-journaled seq per replica (the leader counts as a
    /// replica of itself).
    high_waters: BTreeMap<u32, Seq>,
    voters: usize,
}

impl CommitTracker {
    pub fn new(voters: usize) -> Self {
        CommitTracker {
            high_waters: BTreeMap::new(),
            voters,
        }
    }

    /// Record that `replica` has durably journaled everything up to `seq`.
    /// Returns the (possibly advanced) commit watermark.
    pub fn record_ack(&mut self, replica: u32, seq: Seq) -> Seq {
        let entry = self.high_waters.entry(replica).or_insert(Seq::GENESIS);
        *entry = (*entry).max(seq);
        self.committed()
    }

    /// The commit watermark: the highest seq journaled by at least a
    /// majority of voters.
    pub fn committed(&self) -> Seq {
        let needed = crate::majority(self.voters);
        if self.high_waters.len() < needed {
            return Seq::GENESIS;
        }
        let mut waters: Vec<Seq> = self.high_waters.values().copied().collect();
        waters.sort_unstable_by(|a, b| b.cmp(a)); // descending
        waters[needed - 1]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_is_majority_th_highest() {
        let mut tracker = CommitTracker::new(5);
        assert_eq!(tracker.record_ack(0, Seq(10)), Seq::GENESIS);
        assert_eq!(tracker.record_ack(1, Seq(8)), Seq::GENESIS);
        // Third ack forms a majority: committed = min of top-3 = 5.
        assert_eq!(tracker.record_ack(2, Seq(5)), Seq(5));
        // A slow fourth replica doesn't lower the watermark.
        assert_eq!(tracker.record_ack(3, Seq(2)), Seq(5));
        // The slow one catching up raises it: top-3 = [10, 8, 7] → 7.
        assert_eq!(tracker.record_ack(3, Seq(7)), Seq(7));
    }

    #[test]
    fn acks_are_monotonic() {
        let mut tracker = CommitTracker::new(3);
        tracker.record_ack(0, Seq(9));
        tracker.record_ack(0, Seq(4)); // stale ack must not regress
        assert_eq!(tracker.record_ack(1, Seq(9)), Seq(9));
    }
}
