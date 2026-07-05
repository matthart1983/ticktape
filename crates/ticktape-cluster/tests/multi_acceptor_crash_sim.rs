//! Multi-node acceptor crash/restart safety (P3 DST gap).
//!
//! The single-acceptor `acceptor_persistence.rs` proves one `PersistentAcceptor`
//! never grants an epoch twice across crashes. This closes the *multi-node*
//! gap: N acceptors, two candidates **dueling for the same epoch**, with
//! acceptors crashing and restarting *mid-election*. The property that must
//! survive all interleavings is the one the whole leader-election safety
//! argument rests on:
//!
//! - **No acceptor grants the same epoch twice** — even after a crash+restart
//!   between the two requests. `PersistentAcceptor` fsyncs `promised` before
//!   returning a grant, so a restarted acceptor rejects a re-request at or
//!   below what it already promised.
//! - **Therefore at most one candidate wins any epoch** — two majorities for
//!   the same epoch would need an acceptor to grant it twice, which the above
//!   forbids. A crashed-and-restarted acceptor can never resurrect a second
//!   leader.
//!
//! Each acceptor gets its own fault-injecting `SimStorage`, so a crash keeps
//! only synced bytes (the fsync'd promise) and may leave a torn state file
//! (which reopen rejects loudly — a lost voter, which is safe).

use std::collections::BTreeSet;
use ticktape_cluster::{Election, ElectionOutcome, PersistentAcceptor, VoteReply};
use ticktape_sim::{Rng, SimStorage};

const NODES: usize = 5;
const PATH: &str = "/acc/state";

/// Fisher–Yates over `items` using the seeded RNG (deterministic).
fn shuffle<T>(items: &mut [T], rng: &mut Rng) {
    for i in (1..items.len()).rev() {
        let j = rng.below(i as u64 + 1) as usize;
        items.swap(i, j);
    }
}

#[test]
fn dueling_candidates_never_both_win_across_acceptor_crashes() {
    for seed in 0..300u64 {
        let mut rng = Rng::new(seed);

        // Each acceptor: its own simulated disk + persistent acceptor. `None`
        // once a torn-write crash bricks it (a safely-lost voter).
        let storages: Vec<SimStorage> = (0..NODES).map(|_| SimStorage::new()).collect();
        let mut accs: Vec<Option<PersistentAcceptor<SimStorage>>> = storages
            .iter()
            .map(|s| Some(PersistentAcceptor::open(PATH, s.clone()).unwrap()))
            .collect();

        // Every epoch each acceptor has granted — the double-grant tripwire.
        let mut granted: Vec<BTreeSet<u64>> = vec![BTreeSet::new(); NODES];

        // Each round bids a fresh, monotonically increasing epoch.
        for target in 1u64..=40 {
            // Two candidates duel for the SAME epoch.
            let mut duel = [Election::new(target, NODES), Election::new(target, NODES)];

            // Interleave each candidate's vote request to each acceptor in a
            // seeded order, with crashes injected mid-election.
            let mut order: Vec<(usize, usize)> = Vec::new(); // (candidate, acceptor)
            for a in 0..NODES {
                for c in 0..2 {
                    order.push((c, a));
                }
            }
            shuffle(&mut order, &mut rng);

            for (c, a) in order {
                // Occasionally crash + restart this acceptor right now.
                if rng.chance(1, 6) {
                    storages[a].crash(&mut rng, true);
                    accs[a] = PersistentAcceptor::open(PATH, storages[a].clone()).ok();
                }
                // Occasionally the acceptor is simply unreachable this round.
                if rng.chance(1, 5) {
                    continue;
                }
                let Some(acc) = accs[a].as_mut() else {
                    continue; // bricked by a torn write — a lost voter
                };
                let request = duel[c].request();
                let Ok(reply) = acc.handle(request) else {
                    continue; // durable-write failure ⇒ no vote
                };
                if let VoteReply::Grant { epoch, .. } = reply {
                    // THE core invariant: an acceptor never grants an epoch it
                    // already granted — not even across a crash+restart.
                    assert!(
                        granted[a].insert(epoch),
                        "seed {seed}: acceptor {a} granted epoch {epoch} twice",
                    );
                    assert!(
                        acc.promised() >= epoch,
                        "seed {seed}: promised below a just-granted epoch",
                    );
                }
                duel[c].on_reply(a as u32, reply);
            }

            // The consequence: at most one candidate wins this epoch.
            let winners = duel
                .iter()
                .filter(|e| matches!(e.outcome(), ElectionOutcome::Won { .. }))
                .count();
            assert!(
                winners <= 1,
                "seed {seed}: SPLIT-BRAIN — {winners} winners for epoch {target} \
                 across acceptor crashes",
            );
        }
    }
}
