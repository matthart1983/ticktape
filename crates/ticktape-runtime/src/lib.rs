//! The single-node Ticktape runtime (M0): sequencer + deterministic
//! service host.
//!
//! A [`Node`] is Tier 0 of the spec's durability ladder — one sequencer,
//! journal-backed, no replication. It owns the three responsibilities of
//! the sequencer role:
//!
//! 1. **Order**: stamp each input with the next gapless [`Seq`] and a
//!    sequencer-assigned [`Timestamp`] (the only time a service ever sees).
//! 2. **Journal**: durably append the sequenced frame *before* applying it,
//!    so the log is the system of record.
//! 3. **Execute**: run [`Service::apply`] exactly once per input, in seq
//!    order, and hand back / publish the outputs.
//!
//! On [`Node::open`], state is rebuilt from the newest valid snapshot plus
//! a replay of the journal tail (or `genesis + replay` from scratch when no
//! snapshot exists) — crash recovery is the same code path as normal
//! startup. With `NodeConfig::snapshot_every = Some(n)`, the node snapshots
//! its state every `n` sequenced frames and appends a `SnapshotMark` frame;
//! snapshots are an optimization, never the system of record — a corrupt or
//! torn snapshot silently falls back to an older one or to full replay.
//!
//! The host is deliberately synchronous and single-threaded (run-to-
//! completion per input); concurrency belongs at the edges, not in the
//! state machine.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::mpsc;
use std::time::{SystemTime, UNIX_EPOCH};
use ticktape_core::{
    decode_all, encode_to_vec, Ctx, Frame, FrameKind, OutBuf, Seq, Service, Timestamp,
};
use ticktape_journal::{Journal, JournalConfig, JournalError, RealStorage, SnapshotStore, Storage};

pub use ticktape_journal::FsyncPolicy;

/// Where the sequencer's timestamps come from. Only the *sequencer* reads
/// this; services read `ctx.now()`, which is replayed from the journal —
/// that split is what keeps wall-clock nondeterminism out of the state
/// machine.
pub trait TimeSource {
    fn now(&mut self) -> Timestamp;
}

/// Wall clock (nanos since Unix epoch).
pub struct WallClock;

impl TimeSource for WallClock {
    fn now(&mut self) -> Timestamp {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        Timestamp(nanos as u64)
    }
}

/// A manually-advanced clock for tests and simulation.
pub struct ManualClock(pub Timestamp);

impl TimeSource for ManualClock {
    fn now(&mut self) -> Timestamp {
        self.0
    }
}

#[derive(Debug)]
pub enum NodeError {
    Journal(JournalError),
    /// A journaled input failed to decode — the app's `Input` schema no
    /// longer matches the journal (schema evolution broke the contract).
    CorruptInput {
        seq: Seq,
        err: ticktape_core::CodecError,
    },
    /// The journal was compacted (history starts at `journal_first`) but no
    /// usable snapshot covers the seqs before it, so state cannot be
    /// rebuilt. Means every retained snapshot was lost or corrupt — a
    /// genuine data-loss situation, surfaced rather than silently starting
    /// from a partial prefix.
    UncoveredHistory {
        journal_first: Seq,
    },
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeError::Journal(e) => write!(f, "{e}"),
            NodeError::CorruptInput { seq, err } => {
                write!(f, "journaled input at seq {seq} failed to decode: {err}")
            }
            NodeError::UncoveredHistory { journal_first } => write!(
                f,
                "journal compacted to seq {journal_first} but no snapshot covers earlier history"
            ),
        }
    }
}

impl std::error::Error for NodeError {}

impl From<JournalError> for NodeError {
    fn from(e: JournalError) -> Self {
        NodeError::Journal(e)
    }
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub journal: JournalConfig,
    /// Logical stream/topic stamped on every frame this node sequences.
    pub stream_id: u16,
    /// Snapshot state every `n` sequenced frames (`None` = never; recovery
    /// then always replays from genesis). Snapshots live next to the
    /// journal segments as `.snap` files.
    pub snapshot_every: Option<u64>,
    /// How many recent snapshots to retain after each new one is written;
    /// older snapshots are pruned and the journal is compacted below the
    /// oldest kept snapshot. `1` bounds disk most aggressively; a small N
    /// (default 2) keeps a corrupt-snapshot fallback. This is what makes
    /// 24×7 operation possible — without it, disk grows without bound.
    /// Ignored when `snapshot_every` is `None`.
    pub retain_snapshots: usize,
    /// Tier-2 quorum commit. `None` (the default) is Tier 0/1: outputs are
    /// released the moment they are sequenced. `Some(_)` makes this a
    /// leader whose outputs are *withheld* until a majority of the replica
    /// set has durably journaled the input (see [`CommitConfig`] and
    /// [`Node::record_ack`]).
    pub commit: Option<CommitConfig>,
}

/// Tier-2 quorum-commit configuration for a leader [`Node`].
///
/// A replica's journal is a gapless prefix of the stream, so one high-water
/// seq per replica fully describes what it holds durably. The commit
/// watermark is the majority-th highest of those high-waters: at least a
/// majority hold everything up to it, so it can never be lost to a single
/// failure, and any two majorities intersect ⇒ a committed input survives
/// every future election.
#[derive(Debug, Clone)]
pub struct CommitConfig {
    /// Total voters in the replica set, **including this leader**. The
    /// watermark needs `voters / 2 + 1` durable copies.
    pub voters: usize,
    /// This leader's own replica id, distinct from every follower id passed
    /// to [`Node::record_ack`]. The leader journals a frame *before* it
    /// applies it, so it counts as one durable voter of itself.
    pub self_replica: u32,
}

impl NodeConfig {
    pub fn new(journal_dir: impl Into<std::path::PathBuf>) -> Self {
        NodeConfig {
            journal: JournalConfig::new(journal_dir),
            stream_id: 1,
            snapshot_every: None,
            retain_snapshots: 2,
            commit: None,
        }
    }

    /// Make this node a Tier-2 quorum-commit leader with the given replica
    /// set size and own replica id. Chainable on top of [`NodeConfig::new`].
    pub fn with_quorum(mut self, voters: usize, self_replica: u32) -> Self {
        self.commit = Some(CommitConfig {
            voters,
            self_replica,
        });
        self
    }
}

/// The leader's live view of quorum commit: which replicas hold what, the
/// buffered-but-uncommitted outputs, and the watermark below which they are
/// safe to release. Present only on a Tier-2 leader.
struct CommitState<O> {
    voters: usize,
    self_replica: u32,
    /// Last contiguously-journaled seq per replica (leader included).
    high_waters: BTreeMap<u32, Seq>,
    /// Outputs of applied inputs, withheld until their seq is committed.
    /// Keyed by seq so a watermark advance drains a contiguous prefix.
    pending: BTreeMap<Seq, Vec<O>>,
    /// Highest seq released so far (monotonic).
    committed: Seq,
}

impl<O> CommitState<O> {
    fn new(config: &CommitConfig, self_seq: Seq) -> Self {
        let mut high_waters = BTreeMap::new();
        // Seed the leader's own high-water at its recovered tip: it already
        // holds everything it sequenced, so the first follower ack can form
        // a majority over the recovered prefix immediately.
        high_waters.insert(config.self_replica, self_seq);
        CommitState {
            voters: config.voters,
            self_replica: config.self_replica,
            high_waters,
            pending: BTreeMap::new(),
            committed: self_seq,
        }
    }

    /// Record that `replica` has durably journaled everything up to `seq`
    /// (monotonic — a stale ack never regresses a replica's high-water).
    fn record(&mut self, replica: u32, seq: Seq) {
        let entry = self.high_waters.entry(replica).or_insert(Seq::GENESIS);
        *entry = (*entry).max(seq);
    }

    /// The majority-th highest high-water — the highest seq a majority holds.
    fn watermark(&self) -> Seq {
        let needed = self.voters / 2 + 1;
        if self.high_waters.len() < needed {
            return Seq::GENESIS;
        }
        let mut waters: Vec<Seq> = self.high_waters.values().copied().collect();
        waters.sort_unstable_by(|a, b| b.cmp(a)); // descending
        waters[needed - 1]
    }

    /// Advance `committed` to the current watermark and drain every pending
    /// entry that crossed it, in seq order. Returns the released outputs.
    fn drain_committed(&mut self) -> Vec<(Seq, Vec<O>)> {
        let mark = self.watermark();
        if mark <= self.committed {
            return Vec::new();
        }
        self.committed = mark;
        let mut released = Vec::new();
        // Split off everything <= mark. BTreeMap keeps seq order.
        let above = self.pending.split_off(&mark.next());
        let below = std::mem::replace(&mut self.pending, above);
        for (seq, outputs) in below {
            released.push((seq, outputs));
        }
        released
    }
}

/// How the last [`Node::open_with`] rebuilt state — observability for the
/// fast-recovery path (and for tests that assert it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryInfo {
    /// The snapshot recovery restored from, if any.
    pub snapshot_seq: Option<Seq>,
    /// How many journaled inputs were applied on top.
    pub inputs_replayed: u64,
}

/// The in-process sequenced-stream fan-out: every subscriber receives every
/// sequenced frame, in seq order. The M0 stand-in for the transport layer
/// (IPC and reliable multicast are M3); useful for drop-copy/audit-style
/// auxiliary consumers living in the same process.
pub struct InProcBus {
    subscribers: Vec<mpsc::Sender<Frame>>,
}

impl InProcBus {
    fn new() -> Self {
        InProcBus {
            subscribers: Vec::new(),
        }
    }

    pub fn subscribe(&mut self) -> mpsc::Receiver<Frame> {
        let (tx, rx) = mpsc::channel();
        self.subscribers.push(tx);
        rx
    }

    fn publish(&mut self, frame: &Frame) {
        // Disconnected subscribers are dropped; the stream never blocks on
        // a dead consumer.
        self.subscribers.retain(|tx| tx.send(frame.clone()).is_ok());
    }
}

/// A single-node sequencer + service host. See the module docs.
pub struct Node<S: Service, T: TimeSource = WallClock, St: Storage + Clone = RealStorage> {
    service: S,
    service_config: S::Config,
    journal_config: JournalConfig,
    storage: St,
    journal: Journal<St>,
    snapshots: SnapshotStore<St>,
    snapshot_every: Option<u64>,
    retain_snapshots: usize,
    clock: T,
    stream_id: u16,
    seq: Seq,
    last_timestamp: Timestamp,
    outputs: OutBuf<S::Output>,
    bus: InProcBus,
    recovery: RecoveryInfo,
    /// Tier-2 quorum-commit state; `None` on a Tier 0/1 node.
    commit: Option<CommitState<S::Output>>,
}

impl<S: Service> Node<S, WallClock> {
    /// Open with the wall clock as the sequencer time source.
    pub fn open(config: NodeConfig, service_config: S::Config) -> Result<Self, NodeError> {
        Self::open_with_clock(config, service_config, WallClock)
    }
}

impl<S: Service, T: TimeSource> Node<S, T, RealStorage> {
    /// Open on the real filesystem with an explicit clock.
    pub fn open_with_clock(
        config: NodeConfig,
        service_config: S::Config,
        clock: T,
    ) -> Result<Self, NodeError> {
        Self::open_with(config, service_config, clock, RealStorage)
    }
}

impl<S: Service, T: TimeSource, St: Storage + Clone> Node<S, T, St> {
    /// Open the node: recover `genesis + replay(journal)`, then accept new
    /// inputs at the journal tail. Generic over [`Storage`] so the
    /// deterministic simulator can substitute fault-injecting storage.
    pub fn open_with(
        config: NodeConfig,
        service_config: S::Config,
        clock: T,
        storage: St,
    ) -> Result<Self, NodeError> {
        let journal_config = config.journal.clone();
        let recovered = Journal::open_with(config.journal, storage.clone())?;
        let snapshots = SnapshotStore::new(journal_config.dir.clone(), storage.clone());
        let journal_last = recovered.journal.last_seq();
        let journal_first = recovered.first_seq;

        // Choose the recovery anchor: the newest *valid* snapshot whose seq
        // is >= (journal_first - 1) — i.e. one the surviving journal can
        // continue from. A crash can leave a synced snapshot at a seq the
        // journal (which loses its unsynced tail) no longer reaches; that
        // snapshot is durable state, not stale, and is the anchor a
        // compacted journal depends on. Corrupt snapshots are skipped in
        // favor of an older retained one. Consider snapshots above the
        // journal tail too (bounded scan — retention keeps only a few).
        let scan_ceiling = Seq(journal_last.0.max(snapshots.high_water_seq()?.0));
        let mut from_seq = Seq::GENESIS;
        let mut service = None;
        for snap in snapshots.load_candidates(scan_ceiling)? {
            if snap.seq.0 + 1 < journal_first.0 {
                continue; // older than the journal floor — leaves a gap
            }
            if let Ok(decoded) = decode_all::<S::Snapshot>(&snap.payload) {
                service = Some(S::restore(decoded, &service_config));
                from_seq = snap.seq;
                break;
            }
        }
        // If the journal was compacted, a covering snapshot is mandatory —
        // there is no genesis to replay from.
        if service.is_none() && journal_first > Seq::GENESIS.next() {
            return Err(NodeError::UncoveredHistory { journal_first });
        }
        let snapshot_seq = (from_seq > Seq::GENESIS).then_some(from_seq);
        let mut service = service.unwrap_or_else(|| S::genesis(&service_config));

        // The recovered tip is the later of the journal tail and the anchor
        // snapshot; resume writing above it so no fork forms below the
        // snapshot. Now purge snapshots strictly above the tip — those are
        // the genuinely-future/forkable ones (the M2 stale-snapshot fix);
        // the anchor and everything at/below the tip are retained.
        let recovered_tip = Seq(journal_last.0.max(from_seq.0));
        snapshots.purge_after(recovered_tip)?;

        // If the anchor snapshot is beyond the journal's surviving tail,
        // the journal can't be appended to at tip+1 without a gap — reseat
        // it to a fresh segment at tip+1 (the snapshot covers the hole).
        let mut recovered = recovered;
        if from_seq > journal_last {
            recovered.journal.reseat_to(recovered_tip.next())?;
        }

        let mut outputs = OutBuf::new();
        let mut last_timestamp = Timestamp::ZERO;
        let mut inputs_replayed = 0u64;

        for frame in &recovered.frames {
            last_timestamp = frame.timestamp;
            if frame.seq <= from_seq {
                continue; // captured by the snapshot
            }
            if frame.kind == FrameKind::Input {
                let input: S::Input =
                    decode_all(&frame.payload).map_err(|err| NodeError::CorruptInput {
                        seq: frame.seq,
                        err,
                    })?;
                let mut ctx = Ctx::new(frame.seq, frame.timestamp, &mut outputs);
                service.apply(frame.seq, &input, &mut ctx);
                // Replayed outputs already had their effect (or were lost
                // with the process); recovery discards them.
                outputs.drain();
                inputs_replayed += 1;
            }
        }

        let seq = recovered_tip;
        let commit = config
            .commit
            .as_ref()
            .map(|c| CommitState::new(c, seq));
        Ok(Node {
            service,
            service_config,
            journal_config,
            storage,
            journal: recovered.journal,
            snapshots,
            snapshot_every: config.snapshot_every,
            retain_snapshots: config.retain_snapshots,
            clock,
            stream_id: config.stream_id,
            seq,
            last_timestamp,
            outputs,
            bus: InProcBus::new(),
            recovery: RecoveryInfo {
                snapshot_seq,
                inputs_replayed,
            },
            commit,
        })
    }

    /// Sequence, journal, and apply one input. Returns the assigned seq and
    /// the outputs the caller may now release.
    ///
    /// The frame is journaled *before* `apply` runs: an input either exists
    /// durably in the total order or it never happened.
    ///
    /// **Tier 0/1** (no [`CommitConfig`]): the returned outputs are exactly
    /// this input's outputs — released the instant it is sequenced.
    ///
    /// **Tier 2** (quorum commit): this input's outputs are *withheld* until
    /// a majority of the replica set has durably journaled it. The returned
    /// vector is instead whatever outputs crossed the commit watermark *as a
    /// result of this call* — normally empty here (the leader's own ack is
    /// only one voter), later released by [`Node::record_ack`] as followers
    /// report progress. With a single voter the input commits immediately
    /// and its outputs come back here.
    pub fn submit(&mut self, input: S::Input) -> Result<(Seq, Vec<S::Output>), NodeError> {
        let frame = self.sequence(FrameKind::Input, encode_to_vec(&input))?;
        let mut ctx = Ctx::new(frame.seq, frame.timestamp, &mut self.outputs);
        self.service.apply(frame.seq, &input, &mut ctx);
        let outputs = self.outputs.drain();
        self.bus.publish(&frame);
        self.maybe_snapshot(frame.seq)?;
        match &mut self.commit {
            None => Ok((frame.seq, outputs)),
            Some(state) => {
                state.pending.insert(frame.seq, outputs);
                let me = state.self_replica;
                state.record(me, frame.seq);
                let released = state
                    .drain_committed()
                    .into_iter()
                    .flat_map(|(_, outs)| outs)
                    .collect();
                Ok((frame.seq, released))
            }
        }
    }

    /// Record that follower `replica` has durably journaled everything up to
    /// `seq`. On a Tier-2 leader this may advance the commit watermark and
    /// release buffered outputs; the returned pairs are the newly-committed
    /// `(seq, outputs)` in seq order. A no-op (empty) on a Tier 0/1 node.
    ///
    /// `replica` must differ from the leader's own [`CommitConfig::self_replica`];
    /// the leader records itself automatically on each [`Node::submit`].
    pub fn record_ack(&mut self, replica: u32, seq: Seq) -> Vec<(Seq, Vec<S::Output>)> {
        match &mut self.commit {
            None => Vec::new(),
            Some(state) => {
                state.record(replica, seq);
                state.drain_committed()
            }
        }
    }

    /// The commit watermark: the highest seq whose outputs have been
    /// released. On a Tier-2 leader this trails [`Node::seq`] until a
    /// majority acks; on a Tier 0/1 node everything is committed on submit,
    /// so this equals [`Node::seq`].
    pub fn commit_watermark(&self) -> Seq {
        match &self.commit {
            Some(state) => state.committed,
            None => self.seq,
        }
    }

    /// Outputs currently withheld pending quorum (Tier-2 leader only). A
    /// gauge of in-flight, uncommitted work — the basis for edge flow
    /// control (throttle new inputs when this grows). `0` on a Tier 0/1 node.
    pub fn pending_commit_count(&self) -> usize {
        self.commit.as_ref().map_or(0, |s| s.pending.len())
    }

    /// Inject a sequenced `Tick`, advancing deterministic time for all
    /// consumers (and all future replays) without an application input.
    pub fn tick(&mut self) -> Result<Seq, NodeError> {
        let frame = self.sequence(FrameKind::Tick, Vec::new())?;
        let seq = frame.seq;
        self.bus.publish(&frame);
        self.maybe_snapshot(seq)?;
        self.note_self_progress(seq);
        Ok(seq)
    }

    /// Sequence an `EpochChange` fence frame — the first act of a newly
    /// promoted leader. Its payload is `(epoch, first_seq)` (the same
    /// encoding `ticktape-cluster::EpochChange` reads), sequenced at the
    /// next seq, so replicas consuming the stream adopt the new epoch and
    /// reject any straggler frames from the deposed leader's epoch. Returns
    /// the seq the fence was assigned.
    pub fn fence(&mut self, epoch: u64) -> Result<Seq, NodeError> {
        let next = self.seq.next();
        let payload = encode_to_vec(&(epoch, next.0));
        let frame = self.sequence(FrameKind::EpochChange, payload)?;
        let seq = frame.seq;
        self.bus.publish(&frame);
        self.maybe_snapshot(seq)?;
        self.note_self_progress(seq);
        Ok(seq)
    }

    /// A Tier-2 leader durably holds every frame it sequences, so any
    /// non-`Input` frame (tick, fence, snapshot mark) still advances its own
    /// high-water — otherwise the watermark would stall behind the last
    /// application input. No pending outputs attach to these, so this never
    /// releases anything on its own.
    fn note_self_progress(&mut self, seq: Seq) {
        if let Some(state) = &mut self.commit {
            let me = state.self_replica;
            state.record(me, seq);
        }
    }

    /// Snapshot on cadence: state at `seq` is durably written *before* the
    /// `SnapshotMark` frame advertises it. The mark itself never triggers
    /// another snapshot.
    fn maybe_snapshot(&mut self, seq: Seq) -> Result<(), NodeError> {
        let Some(every) = self.snapshot_every else {
            return Ok(());
        };
        if seq.0 % every.max(1) != 0 {
            return Ok(());
        }
        let payload = encode_to_vec(&self.service.snapshot());
        self.snapshots.write(seq, 0, &payload)?;
        let mark = self.sequence(FrameKind::SnapshotMark, encode_to_vec(&seq))?;
        self.bus.publish(&mark);

        // Bound disk: prune to the newest N snapshots, then compact the
        // journal below the oldest one still kept (so its tail survives).
        // This is the 24×7 loop — steady-state disk, no day-roll.
        if let Some(oldest_kept) = self.snapshots.prune_keep_newest(self.retain_snapshots)? {
            // Compact below oldest_kept - 1: the kept snapshot needs frames
            // strictly after its seq, so keep seq..; frames <= oldest_kept-1
            // are covered by the snapshot and unreachable.
            let cutoff = Seq(oldest_kept.0.saturating_sub(1));
            self.journal.compact_below(cutoff)?;
        }
        Ok(())
    }

    fn sequence(&mut self, kind: FrameKind, payload: Vec<u8>) -> Result<Frame, NodeError> {
        let seq = self.seq.next();
        // Clamp to monotonic non-decreasing so a stepping wall clock can
        // never make sequenced time run backwards.
        let now = self.clock.now().max(self.last_timestamp);
        let frame = Frame::new(seq, now, self.stream_id, kind, payload);
        self.journal.append(&frame)?;
        self.seq = seq;
        self.last_timestamp = now;
        Ok(frame)
    }

    /// Subscribe to the sequenced stream (all frames from now on).
    pub fn subscribe(&mut self) -> mpsc::Receiver<Frame> {
        self.bus.subscribe()
    }

    /// Read-only access to current state. State is a derived projection of
    /// the journal; there is intentionally no mutable access outside
    /// `Service::apply`.
    pub fn service(&self) -> &S {
        &self.service
    }

    /// The seq of the last sequenced frame ([`Seq::GENESIS`] if none).
    pub fn seq(&self) -> Seq {
        self.seq
    }

    /// How the last open rebuilt state (snapshot used, inputs replayed).
    pub fn recovery_info(&self) -> RecoveryInfo {
        self.recovery
    }

    /// Number of journal segment files (disk-pressure gauge; bounded by
    /// compaction under 24×7 operation).
    pub fn journal_segments(&self) -> usize {
        self.journal.segment_count()
    }

    /// The seq of the most recent durable snapshot, if any (from recovery
    /// or a snapshot taken since). Cheap: reads the snapshot store's
    /// high-water filename.
    pub fn latest_snapshot_seq(&self) -> Option<Seq> {
        match self.snapshots.high_water_seq() {
            Ok(seq) if seq > Seq::GENESIS => Some(seq),
            _ => None,
        }
    }

    /// Force the journal to stable storage (e.g. before a deliberate
    /// shutdown under `FsyncPolicy::Micros`).
    pub fn sync(&mut self) -> Result<(), NodeError> {
        Ok(self.journal.sync()?)
    }

    /// Replay-equivalence check: re-run `genesis + replay(journal)` in a
    /// fresh service and assert its snapshot byte-matches the live one.
    /// This is the M0 form of the simulator's continuous determinism check;
    /// run it in CI or after suspicious incidents.
    pub fn verify_replay(&mut self) -> Result<bool, NodeError> {
        // The replayer reads what's on disk, so flush lazy fsync policies.
        self.sync()?;
        let recovered = Journal::open_with(self.journal_config.clone(), self.storage.clone())?;
        let mut replayed = S::genesis(&self.service_config);
        let mut outputs = OutBuf::new();
        for frame in &recovered.frames {
            if frame.kind == FrameKind::Input {
                let input: S::Input =
                    decode_all(&frame.payload).map_err(|err| NodeError::CorruptInput {
                        seq: frame.seq,
                        err,
                    })?;
                let mut ctx = Ctx::new(frame.seq, frame.timestamp, &mut outputs);
                replayed.apply(frame.seq, &input, &mut ctx);
                outputs.drain();
            }
        }
        let live = encode_to_vec(&self.service.snapshot());
        let fresh = encode_to_vec(&replayed.snapshot());
        Ok(live == fresh)
    }
}
