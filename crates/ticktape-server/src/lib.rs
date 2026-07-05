//! A packaged replicated Ticktape server with operator-driven failover —
//! the piece that turns the simulation-proven machinery (M0–M4) into a
//! deployment with **no single point of failure**.
//!
//! One deployment is `N` [`Server`]s, each on its own host/ports. At any
//! time one is the **leader** (the sequencer: it owns a [`Node`], assigns
//! the total order, journals, and publishes the sequenced stream) and the
//! rest are **followers** (they consume the stream, journal every frame
//! they apply — so the journal is replicated with no separate subsystem —
//! and keep a [`Replica`] of the state). Every server co-hosts a durable
//! [`PersistentAcceptor`] answering votes over TCP.
//!
//! Because a follower journals exactly the frames it applies, promotion is
//! clean: the follower's own journal *is* a valid recovery source, so a
//! promoted follower simply opens a [`Node`] on it (replaying to the exact
//! replicated state), fences the new epoch, and starts publishing.
//!
//! ## Stage A: manual promotion
//!
//! [`Server::promote`] is the operator action. It runs an election across
//! the deployment's acceptors ([`run_election`]); on winning a majority it
//! reconciles its journal to the quorum-endorsed prefix, opens a [`Node`],
//! fences the new epoch, and becomes the leader. This is a legitimate
//! production posture (CoralSequencer ships manual primary failover) and
//! decouples the server from failure-detector work — the automatic
//! failure detector is Stage B.
//!
//! The server is driven **synchronously** ([`Server::pump`],
//! [`Server::submit`]) rather than owning its own threads, so a whole
//! deployment can run deterministically inside one integration test over
//! loopback sockets — the same discipline as the transport/gateway tests.
//! A threaded/`serve_forever` wrapper is a thin future addition.

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ticktape_cluster::{run_election, AcceptorServer, ElectionOutcome, EpochChange, PersistentAcceptor};
use ticktape_core::{encode_to_vec, Frame, FrameKind, Seq, Service};
use ticktape_journal::{FsyncPolicy, Journal, JournalConfig, RealStorage};
use ticktape_runtime::{Node, NodeConfig};
use ticktape_transport::{
    bind_udp, ChainStore, JournalRewinder, MemStore, Publisher, PublisherConfig, Receiver,
    ReceiverConfig, Replica, Retransmitter,
};

pub mod admin;
pub mod config;
pub use admin::{bind_metrics, serve_metrics, ServerStats};
pub use config::{ClusterConfig, PeerAddrs};

/// Errors from server operations.
#[derive(Debug)]
pub enum ServerError {
    Io(std::io::Error),
    Node(ticktape_runtime::NodeError),
    Persist(ticktape_cluster::PersistError),
    Transport(ticktape_transport::TransportError),
    /// Promotion lost the election (no majority, or a higher epoch exists).
    ElectionLost {
        highest_promised: u64,
    },
    /// Operation requires the server to be in the other role.
    WrongRole(&'static str),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::Io(e) => write!(f, "server I/O: {e}"),
            ServerError::Node(e) => write!(f, "{e}"),
            ServerError::Persist(e) => write!(f, "{e}"),
            ServerError::Transport(e) => write!(f, "{e}"),
            ServerError::ElectionLost { highest_promised } => {
                write!(
                    f,
                    "promotion lost the election (highest promised epoch {highest_promised})"
                )
            }
            ServerError::WrongRole(what) => write!(f, "wrong role: {what}"),
        }
    }
}
impl std::error::Error for ServerError {}
impl From<std::io::Error> for ServerError {
    fn from(e: std::io::Error) -> Self {
        ServerError::Io(e)
    }
}
impl From<ticktape_runtime::NodeError> for ServerError {
    fn from(e: ticktape_runtime::NodeError) -> Self {
        ServerError::Node(e)
    }
}
impl From<ticktape_cluster::PersistError> for ServerError {
    fn from(e: ticktape_cluster::PersistError) -> Self {
        ServerError::Persist(e)
    }
}
impl From<ticktape_transport::TransportError> for ServerError {
    fn from(e: ticktape_transport::TransportError) -> Self {
        ServerError::Transport(e)
    }
}
impl From<ticktape_journal::JournalError> for ServerError {
    fn from(e: ticktape_journal::JournalError) -> Self {
        ServerError::Node(ticktape_runtime::NodeError::Journal(e))
    }
}

/// Which role a server is playing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
}

/// A leader's transport plumbing.
struct LeaderMode<S: Service> {
    node: Node<S>,
    publisher: Publisher,
    repeater: MemStore,
    /// The subscribed stream from the node, so published frames also land
    /// in the repeater for gap-fill.
    stream: std::sync::mpsc::Receiver<Frame>,
}

/// A follower's transport plumbing.
struct FollowerMode<S: Service> {
    receiver: Receiver<UdpSocket>,
    journal: Journal,
    replica: Replica<S>,
    /// Frames the follower has journaled; its acceptor's high-water.
    applied: Seq,
    /// Automatic failure detector (Stage B).
    detector: FailureDetector,
}

/// A follower's leader-liveness detector: the leader is presumed alive as
/// long as *some* packet (a data frame or an idle heartbeat) keeps arriving.
/// If none arrives within `timeout`, the leader is presumed dead and the
/// follower stands for election. Real wall-clock — this is a liveness
/// policy, not a safety property (the epoch election guarantees safety no
/// matter how twitchy the detector is), so it lives outside the
/// deterministic simulator.
struct FailureDetector {
    /// Packet count last time we saw progress, and when that was.
    last_packets_seen: u64,
    last_progress: Instant,
    /// This node's effective timeout (`base + idx * stagger`), so the
    /// lowest-indexed survivor stands first.
    timeout: Duration,
}

impl FailureDetector {
    fn new(timeout: Duration, packets_seen: u64) -> Self {
        FailureDetector {
            last_packets_seen: packets_seen,
            last_progress: Instant::now(),
            timeout,
        }
    }

    /// Feed the latest packet count; refresh the liveness deadline if it
    /// advanced (the leader spoke).
    fn observe(&mut self, packets_seen: u64) {
        if packets_seen > self.last_packets_seen {
            self.last_packets_seen = packets_seen;
            self.last_progress = Instant::now();
        }
    }

    /// True once the leader has been silent past this node's timeout.
    fn suspects(&self) -> bool {
        self.last_progress.elapsed() >= self.timeout
    }

    /// Restart the clock — after a repoint to a new leader, or a promotion
    /// attempt, so we give the new target a full timeout window.
    fn reset(&mut self, packets_seen: u64) {
        self.last_packets_seen = packets_seen;
        self.last_progress = Instant::now();
    }
}

enum Mode<S: Service> {
    Leader(LeaderMode<S>),
    Follower(FollowerMode<S>),
    /// Transient during promotion.
    Transitioning,
}

/// One replicated server.
pub struct Server<S: Service> {
    idx: usize,
    config: ClusterConfig,
    service_config: S::Config,
    journal_dir: PathBuf,
    acceptor: Arc<Mutex<PersistentAcceptor<RealStorage>>>,
    epoch: u64,
    mode: Mode<S>,
    /// The retransmitter address this server serves gap-fill from when it
    /// is the leader (bound lazily on promotion / leader start).
    retx_addr: Option<SocketAddr>,
}

impl<S: Service> Server<S>
where
    S::Config: Clone,
{
    /// Open a **follower** server. Every server starts as a follower; one is
    /// then promoted (or started as leader with [`Self::start_as_leader`]).
    pub fn open_follower(
        idx: usize,
        config: ClusterConfig,
        service_config: S::Config,
        journal_dir: impl Into<PathBuf>,
    ) -> Result<Self, ServerError> {
        let journal_dir = journal_dir.into();
        let acceptor = Arc::new(Mutex::new(PersistentAcceptor::open(
            journal_dir.join("acceptor.state"),
            RealStorage,
        )?));
        // Serve votes.
        let (server, _) = AcceptorServer::bind(config.peers[idx].acceptor, acceptor.clone())?;
        std::thread::spawn(move || server.serve_forever());

        let follower =
            Self::build_follower(idx, &config, &journal_dir, &acceptor, &service_config)?;
        Ok(Server {
            idx,
            config,
            service_config,
            journal_dir,
            acceptor,
            epoch: 1,
            mode: Mode::Follower(follower),
            retx_addr: None,
        })
    }

    fn build_follower(
        idx: usize,
        config: &ClusterConfig,
        journal_dir: &PathBuf,
        acceptor: &Arc<Mutex<PersistentAcceptor<RealStorage>>>,
        service_config: &S::Config,
    ) -> Result<FollowerMode<S>, ServerError> {
        // Recover any existing journal so a restarted follower keeps its
        // state and high-water.
        let mut jcfg = JournalConfig::new(journal_dir);
        jcfg.fsync = FsyncPolicy::EveryFrame;
        let recovered = Journal::open_with(jcfg, RealStorage)?;
        let applied = recovered.journal.last_seq();
        let mut replica = Replica::new(service_config);
        // Rebuild replica state from the recovered journal.
        for frame in &recovered.frames {
            // Non-input frames advance seq only.
            let _ = replica.apply(frame);
        }
        acceptor.lock().unwrap().observe_seq(applied);

        let sock = bind_udp(config.peers[idx].feed)?;
        let receiver = Receiver::new(
            sock,
            None,
            ReceiverConfig {
                from: applied.next(),
                // Gap-fill from whoever is currently leader; set on pump.
                retransmitter: None,
            },
        );
        let timeout = config.failover_timeout + config.failover_stagger * idx as u32;
        let detector = FailureDetector::new(timeout, receiver.packets_seen());
        Ok(FollowerMode {
            receiver,
            journal: recovered.journal,
            replica,
            applied,
            detector,
        })
    }

    /// The server's role right now.
    pub fn role(&self) -> Role {
        match self.mode {
            Mode::Leader(_) => Role::Leader,
            _ => Role::Follower,
        }
    }

    /// Current epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Point this server's gap-fill at a new leader index (the Stage-A
    /// operator step after a promotion; Stage B maintains it automatically).
    pub fn set_leader_hint(&mut self, leader_idx: usize) {
        self.config.leader_hint = leader_idx;
    }

    /// Highest applied/sequenced seq.
    pub fn seq(&self) -> Seq {
        match &self.mode {
            Mode::Leader(l) => l.node.seq(),
            Mode::Follower(f) => f.applied,
            Mode::Transitioning => Seq::GENESIS,
        }
    }

    /// A point-in-time [`ServerStats`] for the admin/metrics endpoint —
    /// cheap to gather (no journal scan beyond a directory listing).
    pub fn stats(&self) -> ServerStats {
        let (role, seq, lag, snapshot_seq, journal_segments) = match &self.mode {
            Mode::Leader(l) => (
                "leader",
                l.node.seq().0,
                0,
                l.node.latest_snapshot_seq().map_or(0, |s| s.0),
                l.node.journal_segments() as u64,
            ),
            Mode::Follower(f) => {
                let high = f.receiver.announced_high_water().0;
                let lag = high.saturating_sub(f.applied.0);
                (
                    "follower",
                    f.applied.0,
                    lag,
                    0,
                    f.journal.segment_count() as u64,
                )
            }
            Mode::Transitioning => ("follower", 0, 0, 0, 0),
        };
        ServerStats {
            node: self.idx,
            role,
            epoch: self.epoch,
            seq,
            lag,
            snapshot_seq,
            journal_segments,
        }
    }

    /// Read-only view of the service state (leader's node or follower's
    /// replica).
    pub fn snapshot_bytes(&self) -> Vec<u8>
    where
        S::Snapshot: ticktape_core::Encode,
    {
        match &self.mode {
            Mode::Leader(l) => encode_to_vec(&l.node.service().snapshot()),
            Mode::Follower(f) => encode_to_vec(&f.replica.service().snapshot()),
            Mode::Transitioning => Vec::new(),
        }
    }

    /// Start this server as the initial leader of a fresh deployment
    /// (epoch 1). Opens a `Node`, binds the publisher + retransmitter.
    pub fn start_as_leader(&mut self) -> Result<(), ServerError> {
        let leader = self.become_leader(1)?;
        self.mode = Mode::Leader(leader);
        self.epoch = 1;
        Ok(())
    }

    /// Submit a client input (leader only). Returns the assigned seq and the
    /// service outputs; publishes the frame to followers + repeater.
    pub fn submit(&mut self, input: S::Input) -> Result<(Seq, Vec<S::Output>), ServerError> {
        let Mode::Leader(leader) = &mut self.mode else {
            return Err(ServerError::WrongRole("submit requires leader"));
        };
        let (seq, outputs) = leader.node.submit(input)?;
        // Drain the node's stream into the repeater and fan each frame out
        // to every follower feed (not the publisher's single placeholder).
        for frame in leader.stream.try_iter() {
            leader.repeater.record(frame.clone());
            for (i, peer) in self.config.peers.iter().enumerate() {
                if i == self.idx {
                    continue;
                }
                leader.publisher.publish_to(&frame, peer.feed)?;
            }
        }
        Ok((seq, outputs))
    }

    /// Followers: poll the feed for up to `wait`, journaling + applying every
    /// frame received (and gap-filling from the current leader). No-op for a
    /// leader. Returns how many frames were applied.
    pub fn pump(&mut self, wait: Duration) -> Result<usize, ServerError> {
        let leader_retx = self.config.peers[self.current_leader_hint()].retx;
        let Mode::Follower(f) = &mut self.mode else {
            return Ok(0);
        };
        f.receiver.set_retransmitter(Some(leader_retx));
        let mut applied = 0;
        let deadline = std::time::Instant::now() + wait;
        loop {
            match f.receiver.poll(Duration::from_millis(20))? {
                Some(frame) => {
                    // Journal verbatim (replicated journal), apply, observe.
                    if frame.seq == f.applied.next() {
                        f.journal.append(&frame)?;
                        let _ = f.replica.apply(&frame);
                        f.applied = frame.seq;
                        self.acceptor.lock().unwrap().observe_seq(frame.seq);
                        // Adopt the epoch a fence frame carries, so a
                        // subsequent failover targets a fresh epoch rather
                        // than re-bidding one already won (disjoint field
                        // borrow from `f`).
                        if frame.kind == FrameKind::EpochChange {
                            if let Ok(change) = EpochChange::from_frame(&frame) {
                                self.epoch = self.epoch.max(change.epoch);
                            }
                        }
                        applied += 1;
                    }
                }
                None => {
                    if std::time::Instant::now() >= deadline {
                        break;
                    }
                }
            }
        }
        // Feed the failure detector: any packet (frame or idle heartbeat)
        // received this pump counts as the leader being alive.
        f.detector.observe(f.receiver.packets_seen());
        Ok(applied)
    }

    /// **Leader liveness.** Emit a heartbeat to every follower feed so an
    /// idle leader (no inputs to sequence) is not mistaken for a dead one.
    /// The driver calls this every `heartbeat_interval`; no-op for a
    /// follower.
    pub fn heartbeat(&mut self) -> Result<(), ServerError> {
        let Mode::Leader(leader) = &mut self.mode else {
            return Ok(());
        };
        for (i, peer) in self.config.peers.iter().enumerate() {
            if i == self.idx {
                continue;
            }
            leader.publisher.heartbeat_to(peer.feed)?;
        }
        Ok(())
    }

    /// Whether this follower currently suspects the leader has failed (its
    /// detector has seen no liveness within its timeout). `false` for a
    /// leader. This is the trigger [`Server::maybe_failover`] acts on.
    pub fn leader_suspected(&self) -> bool {
        match &self.mode {
            Mode::Follower(f) => f.detector.suspects(),
            _ => false,
        }
    }

    /// **Automatic failover (Stage B).** If this follower suspects the leader
    /// is dead, stand for election with no operator action. On winning,
    /// become the leader (identical to [`Server::promote`]). On losing —
    /// another survivor won the epoch — repoint gap-fill at the presumptive
    /// new leader and resume following, so the deployment re-converges on
    /// its own. Returns the role this server holds afterwards.
    ///
    /// The driver calls this each loop; the epoch election makes concurrent
    /// attempts safe (at most one leader per epoch), and the per-index
    /// stagger makes the lowest-indexed survivor the usual winner.
    pub fn maybe_failover(&mut self) -> Result<Role, ServerError> {
        if !self.leader_suspected() {
            return Ok(self.role());
        }
        match self.promote() {
            Ok(()) => Ok(Role::Leader),
            Err(ServerError::ElectionLost { .. }) => {
                // Someone else is (or is becoming) leader. Point at the most
                // likely winner — the lowest-indexed peer that isn't us and
                // isn't the leader we just gave up on — and give it a fresh
                // timeout window. If we guessed a dead node, the detector
                // fires again next round and we rotate on.
                self.repoint_after_failed_promotion();
                Ok(Role::Follower)
            }
            Err(other) => Err(other),
        }
    }

    /// After losing a failover election, choose a new gap-fill target and
    /// reset the detector. Rotates through peers so repeated failures walk
    /// past dead nodes until frames flow from the real new leader.
    fn repoint_after_failed_promotion(&mut self) {
        let n = self.config.peers.len();
        let dead = self.config.leader_hint;
        // Next candidate after the one we suspected, skipping ourselves.
        let mut cand = (dead + 1) % n;
        while cand == self.idx {
            cand = (cand + 1) % n;
        }
        self.config.leader_hint = cand;
        if let Mode::Follower(f) = &mut self.mode {
            f.receiver
                .set_retransmitter(Some(self.config.peers[cand].retx));
            f.detector.reset(f.receiver.packets_seen());
        }
    }

    /// Which peer index this follower currently gap-fills from. Maintained
    /// automatically by the Stage-B failure detector (`maybe_failover`
    /// re-points it on a lost election); an operator may still override it
    /// via `set_leader_hint`.
    fn current_leader_hint(&self) -> usize {
        self.config.leader_hint
    }

    /// **Operator action — manual promotion.** Run an election; on winning a
    /// majority, reconcile, open a `Node` on the local journal, fence the
    /// new epoch, and become the leader.
    pub fn promote(&mut self) -> Result<(), ServerError> {
        let target = self.epoch + 1;
        let acceptor_addrs: Vec<SocketAddr> =
            self.config.peers.iter().map(|p| p.acceptor).collect();
        match run_election(target, &acceptor_addrs) {
            ElectionOutcome::Won { epoch, sync_to } => self.finish_promotion(epoch, sync_to),
            ElectionOutcome::Lost { highest_promised } => {
                Err(ServerError::ElectionLost { highest_promised })
            }
            ElectionOutcome::Pending => Err(ServerError::ElectionLost {
                highest_promised: target,
            }),
        }
    }

    fn finish_promotion(&mut self, epoch: u64, sync_to: Seq) -> Result<(), ServerError> {
        // Take the follower state.
        let Mode::Follower(f) = std::mem::replace(&mut self.mode, Mode::Transitioning) else {
            return Err(ServerError::WrongRole("promote requires follower"));
        };
        // Reconcile: the winner must hold everything up to sync_to (the max
        // high-water of its granting majority). In the clean case it already
        // does (a caught-up follower). If it were behind, it would fetch the
        // gap from a peer here; Stage A test keeps followers caught up, and a
        // shortfall is surfaced rather than silently led-from-behind.
        if f.applied < sync_to {
            self.mode = Mode::Follower(f);
            return Err(ServerError::ElectionLost {
                highest_promised: epoch, // reuse: "not caught up to lead"
            });
        }
        drop(f); // release the journal handle before Node reopens it
                 // Open a Node on our journal — replays to the exact replicated state.
        let leader = self.become_leader(epoch)?;
        self.mode = Mode::Leader(leader);
        self.epoch = epoch;
        Ok(())
    }

    /// Build leader plumbing at `epoch`: open a Node on the local journal,
    /// bind the publisher (to all follower feeds) + retransmitter, and fence.
    fn become_leader(&mut self, epoch: u64) -> Result<LeaderMode<S>, ServerError> {
        let mut node_config = NodeConfig::new(&self.journal_dir);
        node_config.journal.fsync = FsyncPolicy::EveryFrame;
        node_config.snapshot_every = Some(500);
        let mut node: Node<S> = Node::open(node_config, self.service_config.clone())?;
        let stream = node.subscribe();

        // Publisher sends to every peer's feed address (its own included and
        // harmlessly ignored — a follower only journals seqs it lacks).
        let dest_a = self.config.peers[(self.idx + 1) % self.config.peers.len()].feed;
        let mut publisher = Publisher::new(PublisherConfig {
            session: self.config.session,
            dest_a,
            dest_b: None,
        })?;
        // Multi-follower fan-out: publish to each follower feed explicitly.
        // (Publisher targets one/two dests; for N>2 we send per-peer below.)

        // Repeater + rewinder retransmitter at this server's retx address.
        let repeater = MemStore::with_capacity(64 * 1024);
        let jcfg = JournalConfig::new(&self.journal_dir);
        let store = ChainStore {
            primary: repeater.clone(),
            secondary: JournalRewinder::new(jcfg, RealStorage),
        };
        let (retransmitter, retx_addr) =
            Retransmitter::bind(self.config.peers[self.idx].retx, self.config.session, store)?;
        std::thread::spawn(move || retransmitter.serve_forever());
        self.retx_addr = Some(retx_addr);

        // Fence the new epoch as the first sequenced act.
        node.fence(epoch)?;
        for frame in stream.try_iter() {
            repeater.record(frame.clone());
            self.publish_to_all(&mut publisher, &frame)?;
        }
        Ok(LeaderMode {
            node,
            publisher,
            repeater,
            stream,
        })
    }

    /// Send a frame to every follower feed address (skip our own).
    fn publish_to_all(&self, publisher: &mut Publisher, frame: &Frame) -> Result<(), ServerError> {
        for (i, peer) in self.config.peers.iter().enumerate() {
            if i == self.idx {
                continue;
            }
            publisher.publish_to(frame, peer.feed)?;
        }
        Ok(())
    }
}
