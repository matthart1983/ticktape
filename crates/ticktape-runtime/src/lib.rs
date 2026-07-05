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
}

impl NodeConfig {
    pub fn new(journal_dir: impl Into<std::path::PathBuf>) -> Self {
        NodeConfig {
            journal: JournalConfig::new(journal_dir),
            stream_id: 1,
            snapshot_every: None,
            retain_snapshots: 2,
        }
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
        })
    }

    /// Sequence, journal, and apply one input. Returns the assigned seq and
    /// the outputs the service emitted.
    ///
    /// The frame is journaled *before* `apply` runs: an input either exists
    /// durably in the total order or it never happened.
    pub fn submit(&mut self, input: S::Input) -> Result<(Seq, Vec<S::Output>), NodeError> {
        let frame = self.sequence(FrameKind::Input, encode_to_vec(&input))?;
        let mut ctx = Ctx::new(frame.seq, frame.timestamp, &mut self.outputs);
        self.service.apply(frame.seq, &input, &mut ctx);
        let outputs = self.outputs.drain();
        self.bus.publish(&frame);
        self.maybe_snapshot(frame.seq)?;
        Ok((frame.seq, outputs))
    }

    /// Inject a sequenced `Tick`, advancing deterministic time for all
    /// consumers (and all future replays) without an application input.
    pub fn tick(&mut self) -> Result<Seq, NodeError> {
        let frame = self.sequence(FrameKind::Tick, Vec::new())?;
        let seq = frame.seq;
        self.bus.publish(&frame);
        self.maybe_snapshot(seq)?;
        Ok(seq)
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
