//! The sequencer's deterministic timer wheel.
//!
//! Services arm timers through [`ticktape_core::Ctx::set_timer`]; the runtime
//! collects the requests and keeps them here. When sequenced time reaches a
//! timer's deadline, the [`crate::Node`] pops it and injects a `TimerFired`
//! frame — so firing is a *sequenced, journaled* event that replays
//! identically everywhere. The wheel itself is only a live scheduling aid:
//! it is rebuilt on replay from the same `set`/`cancel` calls and, so that
//! timers armed before a snapshot still fire after a snapshot-based
//! recovery, is serialized into the snapshot.
//!
//! Ordering is total and deterministic: due timers fire in `(deadline,
//! set-at-seq, id)` order, never in hash order.

use std::collections::{BTreeMap, BTreeSet};
use ticktape_core::{Seq, TimerReq, Timestamp};

pub(crate) struct TimerWheel {
    /// `id -> (deadline, seq it was armed at)`. The source of truth for
    /// cancel/re-arm lookups; iterated only in id order (deterministic).
    by_id: BTreeMap<u64, (Timestamp, Seq)>,
    /// The firing order: earliest `(deadline, set-at, id)` first.
    order: BTreeSet<(Timestamp, Seq, u64)>,
}

impl TimerWheel {
    pub(crate) fn new() -> Self {
        TimerWheel {
            by_id: BTreeMap::new(),
            order: BTreeSet::new(),
        }
    }

    /// Apply one scheduling request recorded during the step at `set_at`.
    pub(crate) fn apply(&mut self, req: TimerReq, set_at: Seq) {
        match req {
            TimerReq::Set { id, at } => self.set(id, at, set_at),
            TimerReq::Cancel { id } => self.cancel(id),
        }
    }

    /// Arm or re-arm `id`; re-arming replaces the old deadline.
    pub(crate) fn set(&mut self, id: u64, at: Timestamp, set_at: Seq) {
        if let Some((old_at, old_seq)) = self.by_id.remove(&id) {
            self.order.remove(&(old_at, old_seq, id));
        }
        self.by_id.insert(id, (at, set_at));
        self.order.insert((at, set_at, id));
    }

    /// Cancel `id` if pending (also how a fired timer is removed).
    pub(crate) fn cancel(&mut self, id: u64) {
        if let Some((at, seq)) = self.by_id.remove(&id) {
            self.order.remove(&(at, seq, id));
        }
    }

    /// Remove and return the earliest timer due at `now` (deadline <= now),
    /// in `(deadline, set-at, id)` order. `None` when nothing is due — that
    /// is the loop's termination signal (firing does not advance time, so a
    /// finite set of same-time timers drains in finite steps).
    pub(crate) fn pop_due(&mut self, now: Timestamp) -> Option<u64> {
        let &(at, seq, id) = self.order.iter().next()?;
        if at > now {
            return None;
        }
        self.order.remove(&(at, seq, id));
        self.by_id.remove(&id);
        Some(id)
    }

    /// Serialize for the snapshot as `(id, deadline_nanos, set_at_seq)`,
    /// in id order (deterministic bytes on every replica).
    pub(crate) fn snapshot(&self) -> Vec<(u64, u64, u64)> {
        self.by_id
            .iter()
            .map(|(&id, &(at, seq))| (id, at.0, seq.0))
            .collect()
    }

    /// Rebuild from a snapshot produced by [`TimerWheel::snapshot`].
    pub(crate) fn restore(entries: Vec<(u64, u64, u64)>) -> Self {
        let mut wheel = TimerWheel::new();
        for (id, at, seq) in entries {
            wheel.set(id, Timestamp(at), Seq(seq));
        }
        wheel
    }

    /// Number of pending timers (a snapshot-size / disk-pressure gauge).
    pub(crate) fn len(&self) -> usize {
        self.by_id.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fires_in_deadline_then_setorder_then_id_order() {
        let mut w = TimerWheel::new();
        w.set(7, Timestamp(100), Seq(1));
        w.set(3, Timestamp(50), Seq(2));
        w.set(9, Timestamp(50), Seq(2)); // same deadline+seq, higher id last
                                         // Nothing due before 50.
        assert_eq!(w.pop_due(Timestamp(49)), None);
        // At 50: the two deadline-50 timers fire, id-ordered (3 before 9).
        assert_eq!(w.pop_due(Timestamp(50)), Some(3));
        assert_eq!(w.pop_due(Timestamp(50)), Some(9));
        assert_eq!(w.pop_due(Timestamp(50)), None); // 7 not due yet
        assert_eq!(w.pop_due(Timestamp(100)), Some(7));
    }

    #[test]
    fn rearm_replaces_and_cancel_removes() {
        let mut w = TimerWheel::new();
        w.set(1, Timestamp(100), Seq(1));
        w.set(1, Timestamp(20), Seq(2)); // re-arm earlier
        assert_eq!(w.len(), 1);
        assert_eq!(w.pop_due(Timestamp(20)), Some(1));

        w.set(2, Timestamp(30), Seq(3));
        w.cancel(2);
        assert_eq!(w.pop_due(Timestamp(1_000)), None);
        assert_eq!(w.len(), 0);
    }

    #[test]
    fn snapshot_round_trips() {
        let mut w = TimerWheel::new();
        w.set(5, Timestamp(500), Seq(3));
        w.set(2, Timestamp(200), Seq(1));
        let snap = w.snapshot();
        // Serialized in id order.
        assert_eq!(snap, vec![(2, 200, 1), (5, 500, 3)]);
        let mut w2 = TimerWheel::restore(snap);
        assert_eq!(w2.pop_due(Timestamp(200)), Some(2));
        assert_eq!(w2.pop_due(Timestamp(500)), Some(5));
    }
}
