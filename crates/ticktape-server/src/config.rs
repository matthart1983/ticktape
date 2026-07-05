//! Deployment addressing: where each server's acceptor, feed, and
//! retransmitter live.

use std::net::SocketAddr;
use std::time::Duration;

/// The three addresses each server exposes.
#[derive(Debug, Clone, Copy)]
pub struct PeerAddrs {
    /// TCP: serves leader-election vote requests (always live).
    pub acceptor: SocketAddr,
    /// UDP: where this server *receives* the sequenced feed when a follower
    /// (and where the current leader publishes to).
    pub feed: SocketAddr,
    /// TCP: serves gap-fill/rewind when this server is the leader.
    pub retx: SocketAddr,
}

/// A whole deployment's addressing, shared by every server (each knows its
/// own index into `peers`).
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub peers: Vec<PeerAddrs>,
    /// One session id for the deployment's sequenced stream.
    pub session: u64,
    /// The peer index followers currently treat as leader for gap-fill.
    /// Stage A sets this explicitly around a promotion; Stage B's failure
    /// detector maintains it automatically.
    pub leader_hint: usize,
    /// How long a follower may see no liveness (no frame, no heartbeat) from
    /// the leader before presuming it dead and standing for election. The
    /// effective per-node timeout is `failover_timeout + idx * failover_stagger`,
    /// so the lowest-indexed survivor stands first — fewer dueling
    /// candidates, a predictable winner. Election safety does not depend on
    /// this staggering (dueling is safe), only its efficiency.
    pub failover_timeout: Duration,
    /// Per-index backoff added to `failover_timeout` (see above).
    pub failover_stagger: Duration,
    /// How often a leader emits a liveness heartbeat when it has no inputs
    /// to sequence. Must be comfortably shorter than `failover_timeout`.
    pub heartbeat_interval: Duration,
}

impl ClusterConfig {
    pub fn new(peers: Vec<PeerAddrs>, session: u64) -> Self {
        ClusterConfig {
            peers,
            session,
            leader_hint: 0,
            failover_timeout: Duration::from_millis(500),
            failover_stagger: Duration::from_millis(150),
            heartbeat_interval: Duration::from_millis(100),
        }
    }
}
