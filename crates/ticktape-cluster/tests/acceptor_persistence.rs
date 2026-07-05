//! The acceptor-crash safety property, under seeded fault injection: a
//! `PersistentAcceptor` driven through vote requests interleaved with
//! power-loss crashes must never grant an epoch twice and its `promised`
//! must never regress — even across crash + restart. This is the property
//! that makes the leader-election safety argument hold in a real
//! deployment where acceptors can die mid-election.
//!
//! Uses the same in-memory fault-injecting `SimStorage` as the M1 harness,
//! so a crash keeps only synced bytes (the acceptor fsyncs `promised`
//! before granting) and may leave a torn state file (which reopen must
//! reject loudly, never silently reset to 0).

use ticktape_cluster::{PersistentAcceptor, VoteReply, VoteRequest};
use ticktape_sim::{Rng, SimStorage};

const PATH: &str = "/acc/state";

#[test]
fn promised_is_monotonic_and_no_epoch_granted_twice_under_crashes() {
    for seed in 0..300u64 {
        let mut rng = Rng::new(seed);
        let storage = SimStorage::new();
        let mut acc = PersistentAcceptor::open(PATH, storage.clone()).unwrap();

        // Every epoch that was ever GRANTED, to prove none is granted twice.
        let mut granted: Vec<u64> = Vec::new();
        // The highest promise we have observed durably survive a crash.
        let mut floor = 0u64;
        let mut bricked = false;

        for _ in 0..200 {
            match rng.below(10) {
                // Crash: keep synced bytes, drop unsynced tail; then reopen.
                0 | 1 => {
                    storage.crash(&mut rng, true);
                    match PersistentAcceptor::open(PATH, storage.clone()) {
                        Ok(reopened) => {
                            // Recovered promise must be >= everything durably
                            // granted before the crash.
                            assert!(
                                reopened.promised() >= floor,
                                "seed {seed}: promised regressed on reopen: \
                                 {} < floor {floor}",
                                reopened.promised()
                            );
                            acc = reopened;
                        }
                        Err(_) => {
                            // A torn state file bricks this acceptor — a lost
                            // vote, which is SAFE (never a forgotten promise).
                            // Stop driving it; the property held to here.
                            bricked = true;
                            break;
                        }
                    }
                }
                // Vote request at a seeded epoch (sometimes stale, sometimes
                // ahead), which exercises both grant and reject paths.
                _ => {
                    let epoch = 1 + rng.below(floor + 5);
                    // A durable-write failure returns Err; treat as no vote.
                    let Ok(reply) = acc.handle(VoteRequest { epoch }) else {
                        continue;
                    };
                    match reply {
                        VoteReply::Grant { epoch: e, .. } => {
                            assert_eq!(e, epoch, "seed {seed}: grant echoed wrong epoch");
                            // The core safety invariant: never grant the same
                            // epoch twice (that would elect two leaders).
                            assert!(
                                !granted.contains(&e),
                                "seed {seed}: epoch {e} granted twice"
                            );
                            granted.push(e);
                            // A granted epoch is a durable promise floor.
                            floor = floor.max(e);
                            // And promised must never be below what we granted.
                            assert!(acc.promised() >= e);
                        }
                        VoteReply::Reject { promised } => {
                            // Reject only when the epoch was <= a prior promise.
                            assert!(
                                epoch <= promised,
                                "seed {seed}: rejected epoch {epoch} > promised {promised}"
                            );
                        }
                    }
                }
            }
        }

        // Whatever happened, promised is monotonic to the end (unless bricked
        // by a torn write, which is the safe terminal state).
        if !bricked {
            assert!(
                acc.promised() >= floor,
                "seed {seed}: final promised below floor"
            );
        }
    }
}
