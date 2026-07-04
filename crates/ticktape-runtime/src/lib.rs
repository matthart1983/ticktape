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
//! On [`Node::open`], state is rebuilt as `genesis + replay(journal)` —
//! crash recovery is the same code path as normal startup. Snapshot-based
//! fast recovery is M2.
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
use ticktape_journal::{Journal, JournalConfig, JournalError, RealStorage, Storage};

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
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeError::Journal(e) => write!(f, "{e}"),
            NodeError::CorruptInput { seq, err } => {
                write!(f, "journaled input at seq {seq} failed to decode: {err}")
            }
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
}

impl NodeConfig {
    pub fn new(journal_dir: impl Into<std::path::PathBuf>) -> Self {
        NodeConfig {
            journal: JournalConfig::new(journal_dir),
            stream_id: 1,
        }
    }
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
    clock: T,
    stream_id: u16,
    seq: Seq,
    last_timestamp: Timestamp,
    outputs: OutBuf<S::Output>,
    bus: InProcBus,
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
        let mut service = S::genesis(&service_config);
        let mut outputs = OutBuf::new();
        let mut last_timestamp = Timestamp::ZERO;

        for frame in &recovered.frames {
            last_timestamp = frame.timestamp;
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
            }
        }

        let seq = recovered.journal.last_seq();
        Ok(Node {
            service,
            service_config,
            journal_config,
            storage,
            journal: recovered.journal,
            clock,
            stream_id: config.stream_id,
            seq,
            last_timestamp,
            outputs,
            bus: InProcBus::new(),
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
        Ok((frame.seq, outputs))
    }

    /// Inject a sequenced `Tick`, advancing deterministic time for all
    /// consumers (and all future replays) without an application input.
    pub fn tick(&mut self) -> Result<Seq, NodeError> {
        let frame = self.sequence(FrameKind::Tick, Vec::new())?;
        let seq = frame.seq;
        self.bus.publish(&frame);
        Ok(seq)
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

    /// The seq of the last applied input ([`Seq::GENESIS`] if none).
    pub fn seq(&self) -> Seq {
        self.seq
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
