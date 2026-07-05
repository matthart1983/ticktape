//! Deterministic multi-node cluster simulation: leader kills, partitions,
//! lagging replicas, zombie leaders, and dueling candidates, across many
//! seeds — asserting the M4 safety properties:
//!
//! - **Split-brain safety**: at most one leader wins any epoch; no
//!   (seq, epoch) is ever assigned two different payloads.
//! - **Tier 2 no-loss**: an input whose outputs were released (committed by
//!   a majority) is never fenced away by a failover.
//! - **Tier 1 bounded loss**: what a failover loses is exactly the dead
//!   leader's unreplicated tail — never anything at or before the fence.
//! - **Convergence**: at quiescence every surviving node holds the
//!   identical canonical history and bit-identical state.
//! - **Fencing is load-bearing**: with fencing disabled, the harness
//!   detects divergence (negative test).
//!
//! Elections here are triggered adversarially by the schedule (not by
//! timers), which explores strictly more interleavings than any real
//! failure detector would produce.

use ticktape_cluster::{majority, Acceptor, Election, ElectionOutcome, EpochChange, Tier};
use ticktape_core::{encode_to_vec, Frame, FrameKind, Seq, Service, Timestamp};
use ticktape_sim::demo::{gen_transfer, Bank};
use ticktape_sim::{Invariants, Rng};
use ticktape_transport::Replica;

const NODES: usize = 5;
const STREAM: u16 = 1;

struct NodeSim {
    alive: bool,
    partitioned: bool,
    /// Highest epoch this node has adopted (via an EpochChange frame).
    epoch: u64,
    acceptor: Acceptor,
    /// Gapless journal of applied frames.
    history: Vec<Frame>,
    replica: Replica<Bank>,
    /// Set when a fence proved this node applied discarded history; it
    /// must rebuild (snapshot+replay in production; replay here) before
    /// consuming further. Skipped when fencing is disabled (negative test).
    needs_rebuild: bool,
    /// This node believes it is the leader of `epoch` (a deposed leader
    /// keeps believing until it learns of the higher epoch).
    thinks_leader: bool,
}

struct Sim {
    rng: Rng,
    tier: Tier,
    fencing_enabled: bool,
    epoch: u64,
    leader: Option<usize>,
    /// The canonical history: what the current epoch's leader has
    /// sequenced (post-fence truth).
    canonical: Vec<Frame>,
    nodes: Vec<NodeSim>,
    /// Split-brain ledger: payload hash per (seq, epoch).
    assigned: std::collections::BTreeMap<(u64, u64), Vec<u8>>,
    /// Seqs of inputs whose outputs were released to "clients".
    released: Vec<u64>,
    /// Tier 1 audit: released seqs later fenced away (allowed, bounded).
    lost_released: Vec<u64>,
    /// Monotonic epoch allocator: rejected bids still burn their epoch
    /// (acceptors promised it), so the next bid must go higher.
    epoch_counter: u64,
    /// First seq not yet considered for release.
    release_cursor: u64,
    kills: u32,
    clock: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Violation(String);

impl Sim {
    fn new(seed: u64, tier: Tier, fencing_enabled: bool) -> Sim {
        let mut nodes: Vec<NodeSim> = (0..NODES)
            .map(|_| NodeSim {
                alive: true,
                partitioned: false,
                epoch: 1,
                acceptor: Acceptor::new(),
                history: Vec::new(),
                replica: Replica::new(&()),
                needs_rebuild: false,
                thinks_leader: false,
            })
            .collect();
        // Node 0 starts as the epoch-1 leader (its lease is axiomatic).
        nodes[0].thinks_leader = true;
        Sim {
            rng: Rng::new(seed),
            tier,
            fencing_enabled,
            epoch: 1,
            leader: Some(0),
            canonical: Vec::new(),
            nodes,
            assigned: Default::default(),
            released: Vec::new(),
            lost_released: Vec::new(),
            epoch_counter: 1,
            release_cursor: 1,
            kills: 0,
            clock: 1_000,
        }
    }

    fn record_assignment(&mut self, frame: &Frame, epoch: u64) -> Result<(), Violation> {
        let key = (frame.seq.0, epoch);
        let payload = frame.to_bytes();
        match self.assigned.get(&key) {
            Some(existing) if *existing != payload => Err(Violation(format!(
                "SPLIT-BRAIN: two payloads assigned at seq {} epoch {epoch}",
                frame.seq
            ))),
            Some(_) => Ok(()),
            None => {
                self.assigned.insert(key, payload);
                Ok(())
            }
        }
    }

    /// Committed watermark for Tier 2: the majority-th highest
    /// *canonical-consistent* journal prefix. A node vouches only for
    /// frames that match the current canonical history — a high-water
    /// carried over from a fenced-off suffix must not count, or the
    /// tracker would release data no quorum actually holds (acks are
    /// epoch-scoped; see `Acceptor::reset_high_water`).
    fn committed(&self) -> u64 {
        let mut waters: Vec<u64> = self
            .nodes
            .iter()
            .map(|n| {
                n.history
                    .iter()
                    .zip(&self.canonical)
                    .take_while(|(a, b)| a == b)
                    .count() as u64
            })
            .collect();
        waters.sort_unstable_by(|a, b| b.cmp(a));
        waters[majority(NODES) - 1]
    }

    fn release_outputs(&mut self) {
        let released_up_to = match self.tier {
            Tier::AsyncStandby => self.canonical.len() as u64,
            Tier::QuorumCommit => self.committed(),
        };
        while self.release_cursor <= released_up_to {
            // Control frames (EpochChange) occupy seqs but release nothing.
            if matches!(
                self.canonical
                    .get(self.release_cursor as usize - 1)
                    .map(|f| f.kind),
                Some(FrameKind::Input)
            ) {
                self.released.push(self.release_cursor);
            }
            self.release_cursor += 1;
        }
    }

    fn step(&mut self) -> Result<(), Violation> {
        self.clock += self.rng.below(1_000_000);
        match self.rng.below(100) {
            // Submit an input to the current leader.
            0..=44 => self.op_submit()?,
            // A replica pulls the next canonical frame (delivery).
            45..=79 => self.op_deliver()?,
            80..=84 => self.op_kill_leader(),
            85..=89 => self.op_partition_or_heal(),
            90..=95 => self.op_election(false)?,
            // Dueling candidates for the same epoch.
            _ => self.op_election(true)?,
        }
        Ok(())
    }

    fn op_submit(&mut self) -> Result<(), Violation> {
        let Some(leader) = self.leader else {
            return Ok(());
        };
        let seq = Seq(self.canonical.len() as u64 + 1);
        let cmd = gen_transfer(&mut self.rng);
        let frame = Frame::new(
            seq,
            Timestamp(self.clock),
            STREAM,
            FrameKind::Input,
            encode_to_vec(&cmd),
        );
        self.record_assignment(&frame, self.epoch)?;
        self.canonical.push(frame.clone());
        let node = &mut self.nodes[leader];
        node.history.push(frame.clone());
        node.replica
            .apply(&frame)
            .expect("leader applies own frame");
        node.acceptor.observe_seq(seq);
        node.replica
            .service()
            .check()
            .map_err(|v| Violation(format!("leader invariant: {v}")))?;
        self.release_outputs();
        Ok(())
    }

    fn op_deliver(&mut self) -> Result<(), Violation> {
        let candidates: Vec<usize> = (0..NODES)
            .filter(|&i| {
                self.nodes[i].alive && !self.nodes[i].partitioned && Some(i) != self.leader
            })
            .collect();
        if candidates.is_empty() {
            return Ok(());
        }
        let i = candidates[self.rng.below(candidates.len() as u64) as usize];
        self.deliver_to(i)?;
        self.release_outputs();
        Ok(())
    }

    fn deliver_to(&mut self, i: usize) -> Result<(), Violation> {
        if self.nodes[i].needs_rebuild {
            if !self.fencing_enabled {
                // Negative-test mode: the node ignores the fence and keeps
                // its divergent history. Convergence checks must catch it.
            } else {
                self.rebuild(i);
            }
            self.nodes[i].needs_rebuild = false;
        }
        let len = self.nodes[i].history.len();
        if len >= self.canonical.len() {
            return Ok(());
        }
        // In negative-test mode a divergent node may sit at a seq where
        // its next canonical frame no longer applies gaplessly; skip.
        let frame = self.canonical[len].clone();
        let node = &mut self.nodes[i];
        if node.replica.seq().0 != len as u64 {
            return Ok(());
        }
        if frame.kind == FrameKind::EpochChange {
            let change = EpochChange::from_frame(&frame).expect("valid fence frame");
            node.epoch = change.epoch;
            if node.thinks_leader {
                node.thinks_leader = false; // deposed leader learns and steps down
            }
        }
        node.history.push(frame.clone());
        node.replica.apply(&frame).expect("gapless apply");
        node.acceptor.observe_seq(frame.seq);
        node.replica
            .service()
            .check()
            .map_err(|v| Violation(format!("replica {i} invariant: {v}")))?;
        Ok(())
    }

    /// Snapshot + replay from the canonical stream (models state transfer
    /// from the new leader after this node's history was fenced off).
    fn rebuild(&mut self, i: usize) {
        let node = &mut self.nodes[i];
        node.history.clear();
        node.replica = Replica::new(&());
        node.acceptor.reset_high_water(Seq::GENESIS);
        // Catch up to a seeded point ≤ canonical tip (the rest arrives via
        // normal delivery).
        let upto = self.rng.below(self.canonical.len() as u64 + 1) as usize;
        for frame in &self.canonical[..upto] {
            if frame.kind == FrameKind::EpochChange {
                node.epoch = EpochChange::from_frame(frame).expect("fence").epoch;
            }
            node.history.push(frame.clone());
            node.replica.apply(frame).expect("rebuild apply");
            node.acceptor.observe_seq(frame.seq);
        }
    }

    fn op_kill_leader(&mut self) {
        // Keep a majority alive so elections stay possible.
        if self.kills as usize >= NODES - majority(NODES) {
            return;
        }
        if let Some(leader) = self.leader.take() {
            self.nodes[leader].alive = false;
            self.nodes[leader].thinks_leader = false;
            self.kills += 1;
        }
    }

    fn op_partition_or_heal(&mut self) {
        let i = self.rng.below(NODES as u64) as usize;
        if !self.nodes[i].alive {
            return;
        }
        if self.nodes[i].partitioned {
            self.nodes[i].partitioned = false;
        } else {
            // A partitioned current leader becomes a zombie: it still
            // thinks it leads, but new elections can depose it.
            self.nodes[i].partitioned = true;
            if Some(i) == self.leader {
                self.leader = None; // clients can no longer reach it
            }
        }
    }

    fn op_election(&mut self, duel: bool) -> Result<(), Violation> {
        // Every bid consumes a fresh epoch: acceptors that rejected a bid
        // still promised it, so re-bidding the same number can never win.
        self.epoch_counter += 1;
        let target = self.epoch_counter;
        let eligible: Vec<usize> = (0..NODES)
            .filter(|&i| self.nodes[i].alive && !self.nodes[i].partitioned)
            .collect();
        if eligible.is_empty() {
            return Ok(());
        }
        let pick = |rng: &mut Rng| eligible[rng.below(eligible.len() as u64) as usize];
        let candidate_a = pick(&mut self.rng);
        let candidates: Vec<usize> = if duel {
            vec![candidate_a, pick(&mut self.rng)]
        } else {
            vec![candidate_a]
        };

        let mut elections: Vec<(usize, Election)> = candidates
            .iter()
            .map(|&c| (c, Election::new(target, NODES)))
            .collect();
        // Interleave vote requests in seeded order across acceptors.
        let mut order: Vec<(usize, usize)> = Vec::new(); // (election idx, acceptor)
        for acceptor in 0..NODES {
            for election in 0..elections.len() {
                order.push((election, acceptor));
            }
        }
        for i in (1..order.len()).rev() {
            let j = self.rng.below(i as u64 + 1) as usize;
            order.swap(i, j);
        }
        for (e, a) in order {
            if !self.nodes[a].alive || self.nodes[a].partitioned {
                continue; // unreachable acceptor
            }
            let request = elections[e].1.request();
            let reply = self.nodes[a].acceptor.handle(request);
            elections[e].1.on_reply(a as u32, reply);
        }

        let winners: Vec<(usize, Seq)> = elections
            .iter()
            .filter_map(|(c, e)| match e.outcome() {
                ElectionOutcome::Won { sync_to, .. } => Some((*c, sync_to)),
                _ => None,
            })
            .collect();
        if winners.len() > 1 {
            return Err(Violation(format!(
                "SPLIT-BRAIN: {} winners for epoch {target}",
                winners.len()
            )));
        }
        let Some(&(winner, sync_to)) = winners.first() else {
            return Ok(());
        };
        self.promote(winner, target, sync_to)
    }

    fn promote(&mut self, winner: usize, epoch: u64, sync_to: Seq) -> Result<(), Violation> {
        // A winner may hold a journal suffix that earlier fences condemned
        // (it applied a dead leader's tail and hasn't learned yet). It must
        // discard everything past its longest quorum-endorsed prefix and
        // rebuild before leading — otherwise fenced garbage becomes
        // canonical. (In production: vote grants carry the acceptors'
        // latest fence, and the candidate truncates its journal to it.)
        // This reconciliation always runs — a leader must be internally
        // consistent to lead; the fencing-disabled negative mode models
        // *followers* ignoring fences.
        let common = self.nodes[winner]
            .history
            .iter()
            .zip(&self.canonical)
            .take_while(|(a, b)| a == b)
            .count();
        if common < self.nodes[winner].history.len() {
            let node = &mut self.nodes[winner];
            node.history.truncate(common);
            node.replica = Replica::new(&());
            node.epoch = 1;
            node.acceptor.reset_high_water(Seq(common as u64));
            let frames: Vec<Frame> = node.history.clone();
            for frame in &frames {
                if frame.kind == FrameKind::EpochChange {
                    node.epoch = EpochChange::from_frame(frame).expect("fence").epoch;
                }
                node.replica.apply(frame).expect("reconcile replay");
            }
            node.needs_rebuild = false;
        }
        // Tier 1 promotes from the winner's own state; Tier 2 must first
        // sync to the max high-water of its granting majority.
        let fence = match self.tier {
            Tier::AsyncStandby => self.nodes[winner].history.len() as u64,
            Tier::QuorumCommit => {
                let have = self.nodes[winner].history.len() as u64;
                sync_to.0.max(have)
            }
        };
        // Granted high-waters may still include fenced suffixes (a granter
        // that hasn't rebuilt yet); the canonical history is the most any
        // quorum can actually vouch for. Clamping stays above the commit
        // watermark, which never exceeds canonical.
        let fence = fence.min(self.canonical.len() as u64);
        // State transfer for Tier 2 (frames the winner is missing exist on
        // some granter, whose history is a canonical prefix).
        while (self.nodes[winner].history.len() as u64) < fence {
            let len = self.nodes[winner].history.len();
            let frame = self.canonical[len].clone();
            let node = &mut self.nodes[winner];
            node.history.push(frame.clone());
            node.replica.apply(&frame).expect("sync apply");
            node.acceptor.observe_seq(frame.seq);
        }

        // THE FENCE: everything past `fence` in the old epoch is discarded.
        let lost: Vec<&Frame> = self.canonical[fence as usize..].iter().collect();
        for frame in &lost {
            if self.released.contains(&frame.seq.0) {
                if self.tier == Tier::QuorumCommit {
                    return Err(Violation(format!(
                        "COMMITTED LOSS: released seq {} fenced away (fence {fence})",
                        frame.seq
                    )));
                }
                self.lost_released.push(frame.seq.0);
            }
        }
        self.canonical.truncate(fence as usize);
        self.released.retain(|&s| s <= fence);
        self.release_cursor = self.release_cursor.min(fence + 1);

        // Nodes that applied fenced history must rebuild.
        for i in 0..NODES {
            if self.nodes[i].history.len() as u64 > fence {
                self.nodes[i].needs_rebuild = true;
            }
        }

        self.epoch = epoch;
        self.leader = Some(winner);
        self.nodes[winner].thinks_leader = true;
        self.nodes[winner].epoch = epoch;
        self.nodes[winner].needs_rebuild = false;

        // Seal the new epoch with its fence frame.
        let change = EpochChange {
            epoch,
            first_seq: Seq(fence + 1),
            schema_version: 0,
        };
        let frame = change.to_frame(Timestamp(self.clock), STREAM);
        self.record_assignment(&frame, epoch)?;
        self.canonical.push(frame.clone());
        let node = &mut self.nodes[winner];
        node.history.push(frame.clone());
        node.replica.apply(&frame).expect("fence apply");
        node.acceptor.observe_seq(frame.seq);
        Ok(())
    }

    /// Heal everything, elect if needed, deliver everything, then check
    /// convergence: every surviving node holds the canonical history and
    /// bit-identical state, and every released input survived (Tier 2) or
    /// its loss was accounted at fence time (Tier 1).
    fn quiesce_and_check(&mut self) -> Result<(), Violation> {
        for node in &mut self.nodes {
            node.partitioned = false;
        }
        let mut attempts = 0;
        while self.leader.is_none() {
            self.op_election(false)?;
            attempts += 1;
            assert!(attempts < 64, "no electable majority at quiescence");
        }
        for _ in 0..NODES * 4 {
            for i in 0..NODES {
                if self.nodes[i].alive && Some(i) != self.leader {
                    if self.nodes[i].needs_rebuild && self.fencing_enabled {
                        self.rebuild(i);
                        self.nodes[i].needs_rebuild = false;
                    }
                    while self.nodes[i].history.len() < self.canonical.len() {
                        self.deliver_to(i)?;
                        if self.nodes[i].replica.seq().0 != self.nodes[i].history.len() as u64 {
                            break; // divergent (negative mode)
                        }
                        if self.nodes[i].history.len() >= self.canonical.len() {
                            break;
                        }
                    }
                }
            }
        }
        self.release_outputs();

        let leader = self.leader.unwrap();
        let leader_snapshot = encode_to_vec(&self.nodes[leader].replica.service().snapshot());
        for (i, node) in self.nodes.iter().enumerate() {
            if !node.alive {
                continue;
            }
            let matches_canonical = node.history.len() == self.canonical.len()
                && node
                    .history
                    .iter()
                    .zip(&self.canonical)
                    .all(|(a, b)| a == b);
            if !matches_canonical {
                return Err(Violation(format!(
                    "DIVERGENCE: node {i} history != canonical at quiescence"
                )));
            }
            let snapshot = encode_to_vec(&node.replica.service().snapshot());
            if snapshot != leader_snapshot {
                return Err(Violation(format!(
                    "DIVERGENCE: node {i} state != leader state"
                )));
            }
            node.replica
                .service()
                .check()
                .map_err(|v| Violation(format!("node {i} invariant: {v}")))?;
        }
        for &seq in &self.released {
            if self.canonical.get(seq as usize - 1).is_none() {
                return Err(Violation(format!(
                    "released seq {seq} missing at quiescence"
                )));
            }
        }
        if self.tier == Tier::QuorumCommit && !self.lost_released.is_empty() {
            return Err(Violation("Tier 2 lost released inputs".into()));
        }
        Ok(())
    }
}

fn run(seed: u64, tier: Tier, fencing: bool, steps: u32) -> Result<(), Violation> {
    let mut sim = Sim::new(seed, tier, fencing);
    for _ in 0..steps {
        sim.step()?;
    }
    sim.quiesce_and_check()
}

#[test]
fn tier1_survives_kills_partitions_and_duels() {
    for seed in 0..60u64 {
        run(seed, Tier::AsyncStandby, true, 250)
            .unwrap_or_else(|v| panic!("tier1 seed {seed}: {}", v.0));
    }
}

#[test]
fn tier2_never_loses_committed_inputs() {
    for seed in 0..60u64 {
        run(seed, Tier::QuorumCommit, true, 250)
            .unwrap_or_else(|v| panic!("tier2 seed {seed}: {}", v.0));
    }
}

#[test]
fn tier1_loss_actually_happens_and_is_bounded() {
    // The Tier 1 tradeoff is real: across seeds, some released inputs ARE
    // lost to fences (that's the documented window) — and each loss was
    // accounted at fence time, never discovered as silent divergence.
    let mut total_lost = 0usize;
    for seed in 0..60u64 {
        let mut sim = Sim::new(seed, Tier::AsyncStandby, true);
        let _ = (0..250).try_for_each(|_| sim.step());
        total_lost += sim.lost_released.len();
        sim.quiesce_and_check()
            .unwrap_or_else(|v| panic!("seed {seed}: {}", v.0));
    }
    assert!(
        total_lost > 0,
        "the fuzz never exercised the Tier 1 loss window — weaken sync or raise kill rate"
    );
}

#[test]
fn fencing_is_load_bearing() {
    // With fencing disabled, deposed history is never discarded; the
    // convergence check must catch the divergence on at least some seeds.
    let mut caught = 0;
    for seed in 0..40u64 {
        if run(seed, Tier::AsyncStandby, false, 250).is_err() {
            caught += 1;
        }
    }
    assert!(
        caught > 0,
        "disabling fencing was never detected — the invariants are too weak"
    );
}

#[test]
fn same_seed_reproduces() {
    let a = run(7, Tier::QuorumCommit, true, 250);
    let b = run(7, Tier::QuorumCommit, true, 250);
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
}
