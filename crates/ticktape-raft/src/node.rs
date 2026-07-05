//! One Raft-ordered Ticktape node: a `RawNode` + a deterministic `Service`.
//!
//! You drive it — `tick` the clock, `propose` inputs on the leader, `step`
//! peer messages, and `drive_ready` to persist/apply and collect the messages
//! to send. Committed entries are decoded to `S::Input` and applied to the
//! `Service` exactly once, in log order, on every node — so the state machine
//! stays bit-identical across the cluster, ordered by Raft instead of by
//! Ticktape's native sequencer.

use raft::prelude::*;
use raft::storage::MemStorage;
use raft::{Config, RawNode, StateRole};
use ticktape_core::{decode_all, encode_to_vec, Ctx, OutBuf, Seq, Service, TimerReq, Timestamp};

/// A proposal was rejected (e.g. this node is not the leader).
#[derive(Debug)]
pub struct StepError(pub raft::Error);

impl std::fmt::Display for StepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "raft: {}", self.0)
    }
}
impl std::error::Error for StepError {}

impl From<raft::Error> for StepError {
    fn from(e: raft::Error) -> Self {
        StepError(e)
    }
}

/// One node of a Raft-ordered Ticktape deployment.
pub struct ServiceNode<S: Service> {
    raw: RawNode<MemStorage>,
    service: S,
    applied: u64,
    outputs: OutBuf<S::Output>,
    timer_ops: Vec<TimerReq>,
}

impl<S: Service> ServiceNode<S> {
    /// Create node `id` in a cluster whose voter set is `voters`, with a fresh
    /// `Service`. All nodes must be created with the same `voters` list.
    pub fn new(id: u64, voters: Vec<u64>, service_config: &S::Config) -> Result<Self, StepError> {
        let cfg = Config {
            id,
            election_tick: 10,
            heartbeat_tick: 3,
            ..Default::default()
        };
        cfg.validate().map_err(StepError)?;
        let storage = MemStorage::new_with_conf_state((voters, vec![]));
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let raw = RawNode::new(&cfg, storage, &logger).map_err(StepError)?;
        Ok(ServiceNode {
            raw,
            service: S::genesis(service_config),
            applied: 0,
            outputs: OutBuf::new(),
            timer_ops: Vec::new(),
        })
    }

    /// Advance the logical clock one tick (drives election + heartbeat timers).
    pub fn tick(&mut self) -> bool {
        self.raw.tick()
    }

    /// Make this node campaign to become leader immediately.
    pub fn campaign(&mut self) -> Result<(), StepError> {
        self.raw.campaign().map_err(StepError)
    }

    /// True if this node currently believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.raw.raft.state == StateRole::Leader
    }

    /// Propose an application input for ordering. Only the leader can propose;
    /// on a follower this returns an error. The input is committed and applied
    /// once Raft replicates it to a majority.
    pub fn propose(&mut self, input: &S::Input) -> Result<(), StepError> {
        self.raw
            .propose(Vec::new(), encode_to_vec(input))
            .map_err(StepError)
    }

    /// Feed a message received from a peer into the Raft state machine.
    pub fn step(&mut self, msg: Message) -> Result<(), StepError> {
        self.raw.step(msg).map_err(StepError)
    }

    /// Run one `Ready` cycle: persist new log entries and hard state, apply
    /// newly-committed entries to the `Service`, and return the messages to
    /// deliver to peers. Call whenever `tick`/`step`/`propose` may have made
    /// progress.
    pub fn drive_ready(&mut self) -> Vec<Message> {
        if !self.raw.has_ready() {
            return Vec::new();
        }
        let mut ready = self.raw.ready();

        let mut messages = ready.take_messages();

        // A snapshot would reset the whole state machine; not used in the
        // in-memory reference (compaction/snapshot transfer is future work).
        if !ready.snapshot().is_empty() {
            let snap = ready.snapshot().clone();
            self.raw.mut_store().wl().apply_snapshot(snap).unwrap();
        }

        self.apply_committed(ready.take_committed_entries());

        if !ready.entries().is_empty() {
            self.raw.mut_store().wl().append(ready.entries()).unwrap();
        }

        if let Some(hs) = ready.hs() {
            self.raw.mut_store().wl().set_hardstate(hs.clone());
        }

        messages.append(&mut ready.take_persisted_messages());

        let mut light = self.raw.advance(ready);
        if let Some(commit) = light.commit_index() {
            self.raw
                .mut_store()
                .wl()
                .mut_hard_state()
                .set_commit(commit);
        }
        messages.append(&mut light.take_messages());
        self.apply_committed(light.take_committed_entries());
        self.raw.advance_apply();

        messages
    }

    /// Apply committed log entries to the `Service`, in order, exactly once.
    fn apply_committed(&mut self, entries: Vec<Entry>) {
        for entry in entries {
            self.applied = entry.get_index();
            // Empty entries (a new leader's no-op) and config changes carry no
            // application input.
            if entry.get_data().is_empty() {
                continue;
            }
            if entry.get_entry_type() != EntryType::EntryNormal {
                continue;
            }
            let input: S::Input = match decode_all(entry.get_data()) {
                Ok(i) => i,
                Err(_) => continue, // not our data; skip defensively
            };
            // Seq and time are the log index — deterministic across replicas.
            // (A production build would carry the leader's timestamp in the
            // entry context so `ctx.now()` reflects wall time deterministically.)
            let seq = Seq(entry.get_index());
            self.timer_ops.clear();
            let mut ctx = Ctx::new(
                seq,
                Timestamp(entry.get_index()),
                &mut self.outputs,
                &mut self.timer_ops,
            );
            self.service.apply(seq, &input, &mut ctx);
            self.outputs.drain();
        }
    }

    /// The highest log index this node has applied.
    pub fn applied_index(&self) -> u64 {
        self.applied
    }

    /// Read-only access to the replicated state machine.
    pub fn service(&self) -> &S {
        &self.service
    }
}
