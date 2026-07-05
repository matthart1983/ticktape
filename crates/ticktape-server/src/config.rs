//! Deployment addressing: where each server's acceptor, feed, and
//! retransmitter live.

use std::net::SocketAddr;

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
    /// detector will maintain it.
    pub leader_hint: usize,
}

impl ClusterConfig {
    pub fn new(peers: Vec<PeerAddrs>, session: u64) -> Self {
        ClusterConfig {
            peers,
            session,
            leader_hint: 0,
        }
    }
}
