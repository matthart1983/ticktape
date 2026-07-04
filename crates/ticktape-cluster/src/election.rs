//! Epoch-lease leader election: Paxos-phase-1-shaped majority grants.
//!
//! Safety argument, in full: an [`Acceptor`] grants an epoch only if it is
//! strictly greater than any epoch it has already promised, and `promised`
//! only ever increases. So for any single epoch `e`, each acceptor grants
//! `e` at most once, to at most one candidate — and since winning requires
//! a majority of grants and two majorities always intersect, **at most one
//! candidate can ever win epoch `e`**. Two leaders may exist briefly across
//! *different* epochs; the [`crate::EpochChange`] fence makes the older one
//! harmless (its frames are rejected), and it steps down on discovering
//! the higher epoch.
//!
//! Grants carry the acceptor's journal high-water. A Tier 2 winner must
//! sync to the **max high-water among its granting majority** before
//! sequencing: any committed input lives on a majority of replicas, every
//! two majorities intersect, therefore the max granted high-water covers
//! the commit watermark — no committed input is lost.
//!
//! Durability note: in production `promised` MUST be persisted before a
//! grant is sent (an acceptor that forgets its promise can elect two
//! leaders for one epoch). The simulator does not yet crash acceptors;
//! embedders must journal `promised`.

use std::collections::BTreeSet;
use ticktape_core::Seq;

/// A candidate's request for the lease on `epoch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteRequest {
    pub epoch: u64,
}

/// An acceptor's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteReply {
    /// The lease on this epoch is granted; `high_water` is the last seq
    /// this acceptor's replica has journaled.
    Grant { epoch: u64, high_water: Seq },
    /// Refused: the acceptor has already promised `promised` (≥ requested).
    Reject { promised: u64 },
}

/// One voter's persistent election state.
#[derive(Debug, Default)]
pub struct Acceptor {
    promised: u64,
    high_water: Seq,
}

impl Acceptor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Update the journal high-water this acceptor reports in grants
    /// (call as the co-located replica journals frames).
    pub fn observe_seq(&mut self, seq: Seq) {
        self.high_water = self.high_water.max(seq);
    }

    /// Reset the reported high-water after a fence truncated the
    /// co-located journal. REQUIRED: a high-water may only ever vouch for
    /// frames consistent with the current epoch's history — reporting a
    /// fenced (discarded) suffix would let elections sync to garbage and
    /// commit trackers release uncommitted data.
    pub fn reset_high_water(&mut self, seq: Seq) {
        self.high_water = seq;
    }

    /// The highest epoch promised so far (persist this before replying).
    pub fn promised(&self) -> u64 {
        self.promised
    }

    pub fn handle(&mut self, request: VoteRequest) -> VoteReply {
        if request.epoch > self.promised {
            self.promised = request.epoch;
            VoteReply::Grant {
                epoch: request.epoch,
                high_water: self.high_water,
            }
        } else {
            VoteReply::Reject {
                promised: self.promised,
            }
        }
    }
}

/// What an [`Election`] has concluded so far.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionOutcome {
    /// Not enough replies yet.
    Pending,
    /// Majority granted. `sync_to` is the max high-water among granters —
    /// a Tier 2 winner must hold every frame up to it before sequencing.
    Won { epoch: u64, sync_to: Seq },
    /// A rejection revealed a promised epoch ≥ ours; retry higher.
    Lost { highest_promised: u64 },
}

/// A candidate's tally for one epoch attempt.
#[derive(Debug)]
pub struct Election {
    epoch: u64,
    needed: usize,
    granters: BTreeSet<u32>,
    sync_to: Seq,
    highest_promised: u64,
    lost: bool,
}

impl Election {
    /// Start a bid for `epoch` in a cluster of `voters` acceptors.
    pub fn new(epoch: u64, voters: usize) -> Self {
        Election {
            epoch,
            needed: crate::majority(voters),
            granters: BTreeSet::new(),
            sync_to: Seq::GENESIS,
            highest_promised: 0,
            lost: false,
        }
    }

    pub fn request(&self) -> VoteRequest {
        VoteRequest { epoch: self.epoch }
    }

    /// Record one acceptor's reply (duplicates from the same acceptor are
    /// counted once).
    pub fn on_reply(&mut self, from: u32, reply: VoteReply) -> ElectionOutcome {
        match reply {
            VoteReply::Grant { epoch, high_water } if epoch == self.epoch => {
                self.granters.insert(from);
                self.sync_to = self.sync_to.max(high_water);
            }
            VoteReply::Grant { .. } => {} // stale grant for another epoch
            VoteReply::Reject { promised } => {
                self.highest_promised = self.highest_promised.max(promised);
                if promised >= self.epoch {
                    self.lost = true;
                }
            }
        }
        self.outcome()
    }

    pub fn outcome(&self) -> ElectionOutcome {
        if self.granters.len() >= self.needed {
            ElectionOutcome::Won {
                epoch: self.epoch,
                sync_to: self.sync_to,
            }
        } else if self.lost {
            ElectionOutcome::Lost {
                highest_promised: self.highest_promised,
            }
        } else {
            ElectionOutcome::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_candidate_wins_majority() {
        let mut acceptors: Vec<Acceptor> = (0..3).map(|_| Acceptor::new()).collect();
        acceptors[1].observe_seq(Seq(42));
        let mut election = Election::new(1, 3);
        for (i, acceptor) in acceptors.iter_mut().enumerate() {
            let reply = acceptor.handle(election.request());
            election.on_reply(i as u32, reply);
        }
        assert_eq!(
            election.outcome(),
            ElectionOutcome::Won {
                epoch: 1,
                sync_to: Seq(42)
            },
            "sync_to must be the max granted high-water"
        );
    }

    #[test]
    fn at_most_one_winner_per_epoch() {
        // Two candidates race for epoch 1 across 5 acceptors, with every
        // possible split of first-arrival order: never two winners.
        for split in 0..=5usize {
            let mut acceptors: Vec<Acceptor> = (0..5).map(|_| Acceptor::new()).collect();
            let mut a = Election::new(1, 5);
            let mut b = Election::new(1, 5);
            for (i, acceptor) in acceptors.iter_mut().enumerate() {
                // First `split` acceptors hear A first, the rest hear B first.
                let (first, second): (&mut Election, &mut Election) = if i < split {
                    (&mut a, &mut b)
                } else {
                    (&mut b, &mut a)
                };
                let reply = acceptor.handle(first.request());
                first.on_reply(i as u32, reply);
                let reply = acceptor.handle(second.request());
                second.on_reply(i as u32, reply);
            }
            let winners = [a.outcome(), b.outcome()]
                .iter()
                .filter(|o| matches!(o, ElectionOutcome::Won { .. }))
                .count();
            assert!(winners <= 1, "split {split}: two winners for one epoch");
        }
    }

    #[test]
    fn rejection_reveals_higher_promise() {
        let mut acceptor = Acceptor::new();
        assert!(matches!(
            acceptor.handle(VoteRequest { epoch: 5 }),
            VoteReply::Grant { .. }
        ));
        let mut election = Election::new(3, 1);
        let reply = acceptor.handle(election.request());
        assert_eq!(
            election.on_reply(0, reply),
            ElectionOutcome::Lost {
                highest_promised: 5
            }
        );
    }

    #[test]
    fn duplicate_grants_counted_once() {
        let mut acceptor = Acceptor::new();
        let mut election = Election::new(1, 3);
        let reply = acceptor.handle(election.request());
        election.on_reply(0, reply);
        assert_eq!(election.on_reply(0, reply), ElectionOutcome::Pending);
    }
}
