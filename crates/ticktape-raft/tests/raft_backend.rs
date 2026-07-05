//! The raft-rs delegation backend end to end: three nodes driven entirely
//! in-process (no threads, no wall clock — the loop *is* the clock), electing
//! a leader and replicating a Ticktape `Service` through the Raft log until
//! all three converge to bit-identical state. This is the property that makes
//! the backend real: the deterministic state machine is unchanged; only the
//! log ordering is delegated to Raft.
#![cfg(feature = "raft-backend")]

use ticktape_codec::{Decode, Encode};
use ticktape_core::{Ctx, Seq, Service};
use ticktape_raft::ServiceNode;

/// A trivial replicated counter.
struct Counter {
    total: i64,
}

#[derive(Encode, Decode, PartialEq, Debug)]
enum Cmd {
    Add(i64),
    Reset,
}

#[derive(Encode, Decode, PartialEq, Debug)]
struct Snap {
    total: i64,
}

impl Service for Counter {
    type Input = Cmd;
    type Output = ();
    type Snapshot = Snap;
    type Config = ();

    fn genesis(_: &()) -> Self {
        Counter { total: 0 }
    }
    fn apply(&mut self, _: Seq, input: &Cmd, _: &mut Ctx<'_, ()>) {
        match input {
            Cmd::Add(n) => self.total += n,
            Cmd::Reset => self.total = 0,
        }
    }
    fn snapshot(&self) -> Snap {
        Snap { total: self.total }
    }
    fn restore(s: Snap, _: &()) -> Self {
        Counter { total: s.total }
    }
}

/// Tick every node, then collect and route the messages each emits, for one
/// round. Node ids are `1..=n`, mapped to indices `0..n`.
fn round(nodes: &mut [ServiceNode<Counter>]) {
    for n in nodes.iter_mut() {
        n.tick();
    }
    let mut inbox = Vec::new();
    for n in nodes.iter_mut() {
        inbox.extend(n.drive_ready());
    }
    // Deliver each message to its target; the target's *reply* is produced by
    // its `drive_ready` on the next round, so a request/response exchange
    // takes a couple of rounds — which the caller's loop provides.
    for msg in inbox {
        let to = msg.get_to();
        if to == 0 {
            continue;
        }
        let idx = (to - 1) as usize;
        if let Some(target) = nodes.get_mut(idx) {
            target.step(msg).unwrap();
        }
    }
}

#[test]
fn three_nodes_elect_replicate_and_converge() {
    let voters = vec![1u64, 2, 3];
    let mut nodes: Vec<ServiceNode<Counter>> = voters
        .iter()
        .map(|&id| ServiceNode::new(id, voters.clone(), &()).unwrap())
        .collect();

    // Node 1 stands for election; drive rounds until it leads.
    nodes[0].campaign().unwrap();
    for _ in 0..100 {
        round(&mut nodes);
        if nodes.iter().any(|n| n.is_leader()) {
            break;
        }
    }
    let leader = nodes
        .iter()
        .position(|n| n.is_leader())
        .expect("a leader must emerge");

    // Propose 50 increments on the leader; drive until they replicate.
    let mut expected = 0i64;
    for i in 1..=50i64 {
        nodes[leader].propose(&Cmd::Add(i)).unwrap();
        expected += i;
    }
    for _ in 0..400 {
        round(&mut nodes);
        if nodes.iter().all(|n| n.service().total == expected) {
            break;
        }
    }

    // All three converged to the identical Raft-ordered state.
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            n.service().total,
            expected,
            "node {} diverged: {} != {expected}",
            i + 1,
            n.service().total
        );
    }
}

#[test]
fn a_follower_cannot_propose() {
    let voters = vec![1u64, 2, 3];
    let mut nodes: Vec<ServiceNode<Counter>> = voters
        .iter()
        .map(|&id| ServiceNode::new(id, voters.clone(), &()).unwrap())
        .collect();
    nodes[0].campaign().unwrap();
    for _ in 0..100 {
        round(&mut nodes);
        if nodes.iter().any(|n| n.is_leader()) {
            break;
        }
    }
    // A non-leader node's proposal is refused by Raft.
    let follower = nodes.iter().position(|n| !n.is_leader()).unwrap();
    assert!(
        nodes[follower].propose(&Cmd::Add(1)).is_err(),
        "a follower must not be able to propose",
    );
}
