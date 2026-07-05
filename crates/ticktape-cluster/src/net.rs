//! The leadership network: vote requests/replies over TCP, an acceptor
//! server co-located with each replica, and a candidate that runs an
//! election across a set of acceptor addresses.
//!
//! This is the socket layer for [`Election`]/[`PersistentAcceptor`] — the
//! pure state machines stay pure; this just moves their messages between
//! nodes, with the same CRC'd length-prefixed framing as the transport.
//! A candidate collects grants until it has a majority (win), a rejection
//! reveals a higher promised epoch (retry higher), or it runs out of
//! reachable acceptors (lose — the deployment lacks a live majority).

use crate::election::{Election, ElectionOutcome, VoteReply, VoteRequest};
use crate::persist::PersistentAcceptor;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use ticktape_core::crc32c::crc32c;
use ticktape_core::Seq;
use ticktape_journal::Storage;

const REQUEST_MAGIC: &[u8; 4] = b"TKVQ"; // vote request
const REPLY_MAGIC: &[u8; 4] = b"TKVR"; // vote reply
const REQUEST_LEN: usize = 4 + 8 + 4; // magic + epoch + crc
const REPLY_LEN: usize = 4 + 1 + 8 + 8 + 4; // magic + kind + a + b + crc

fn encode_request(req: VoteRequest) -> [u8; REQUEST_LEN] {
    let mut out = [0u8; REQUEST_LEN];
    out[0..4].copy_from_slice(REQUEST_MAGIC);
    out[4..12].copy_from_slice(&req.epoch.to_le_bytes());
    let crc = crc32c(&out[0..12]);
    out[12..16].copy_from_slice(&crc.to_le_bytes());
    out
}

fn decode_request(bytes: &[u8; REQUEST_LEN]) -> Option<VoteRequest> {
    if &bytes[0..4] != REQUEST_MAGIC {
        return None;
    }
    if crc32c(&bytes[0..12]) != u32::from_le_bytes(bytes[12..16].try_into().unwrap()) {
        return None;
    }
    Some(VoteRequest {
        epoch: u64::from_le_bytes(bytes[4..12].try_into().unwrap()),
    })
}

fn encode_reply(reply: VoteReply) -> [u8; REPLY_LEN] {
    let mut out = [0u8; REPLY_LEN];
    out[0..4].copy_from_slice(REPLY_MAGIC);
    let (kind, a, b) = match reply {
        VoteReply::Grant { epoch, high_water } => (0u8, epoch, high_water.0),
        VoteReply::Reject { promised } => (1u8, promised, 0),
    };
    out[4] = kind;
    out[5..13].copy_from_slice(&a.to_le_bytes());
    out[13..21].copy_from_slice(&b.to_le_bytes());
    let crc = crc32c(&out[0..21]);
    out[21..25].copy_from_slice(&crc.to_le_bytes());
    out
}

fn decode_reply(bytes: &[u8; REPLY_LEN]) -> Option<VoteReply> {
    if &bytes[0..4] != REPLY_MAGIC {
        return None;
    }
    if crc32c(&bytes[0..21]) != u32::from_le_bytes(bytes[21..25].try_into().unwrap()) {
        return None;
    }
    let a = u64::from_le_bytes(bytes[5..13].try_into().unwrap());
    let b = u64::from_le_bytes(bytes[13..21].try_into().unwrap());
    match bytes[4] {
        0 => Some(VoteReply::Grant {
            epoch: a,
            high_water: Seq(b),
        }),
        1 => Some(VoteReply::Reject { promised: a }),
        _ => None,
    }
}

/// Serves vote requests over TCP from a shared [`PersistentAcceptor`]. The
/// acceptor is behind a mutex because a live replica also calls
/// `observe_seq`/`reset_high_water` on it as it journals frames.
pub struct AcceptorServer<St: Storage> {
    listener: TcpListener,
    acceptor: Arc<Mutex<PersistentAcceptor<St>>>,
}

impl<St: Storage + Send + 'static> AcceptorServer<St> {
    /// Bind and return the actual address (use port 0 to auto-pick).
    pub fn bind(
        addr: SocketAddr,
        acceptor: Arc<Mutex<PersistentAcceptor<St>>>,
    ) -> std::io::Result<(AcceptorServer<St>, SocketAddr)> {
        let listener = TcpListener::bind(addr)?;
        let local = listener.local_addr()?;
        Ok((AcceptorServer { listener, acceptor }, local))
    }

    /// Serve forever (run on its own thread). One request per connection.
    pub fn serve_forever(self) {
        for stream in self.listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let _ = self.serve_one(&mut stream);
        }
    }

    fn serve_one(&self, stream: &mut TcpStream) -> std::io::Result<()> {
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        let mut req_bytes = [0u8; REQUEST_LEN];
        stream.read_exact(&mut req_bytes)?;
        let Some(request) = decode_request(&req_bytes) else {
            return Ok(()); // malformed — drop
        };
        // A persistence failure must NOT produce a grant; drop the
        // connection so the candidate simply counts it as unreachable.
        let reply = {
            let mut acc = self.acceptor.lock().unwrap();
            match acc.handle(request) {
                Ok(reply) => reply,
                Err(_) => return Ok(()),
            }
        };
        stream.write_all(&encode_reply(reply))?;
        Ok(())
    }
}

/// Ask one acceptor for a vote (one TCP round-trip). `None` if the acceptor
/// is unreachable or replies malformed — treated as a non-vote.
fn request_vote(addr: SocketAddr, epoch: u64) -> Option<VoteReply> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream
        .write_all(&encode_request(VoteRequest { epoch }))
        .ok()?;
    let mut reply_bytes = [0u8; REPLY_LEN];
    stream.read_exact(&mut reply_bytes).ok()?;
    decode_reply(&reply_bytes)
}

/// Run one election for `epoch` across `acceptors` (all voter addresses,
/// including the candidate's own co-located acceptor). Contacts each in
/// turn, tallies with the pure [`Election`], and returns the outcome — a
/// [`ElectionOutcome::Won`] carries `sync_to`, the max high-water among the
/// granting majority, which a winner must hold before publishing.
///
/// This is a single blocking round; a caller that loses to a higher
/// promised epoch retries at `highest_promised + 1`.
pub fn run_election(epoch: u64, acceptors: &[SocketAddr]) -> ElectionOutcome {
    let mut election = Election::new(epoch, acceptors.len());
    for (i, &addr) in acceptors.iter().enumerate() {
        if let Some(reply) = request_vote(addr, epoch) {
            let outcome = election.on_reply(i as u32, reply);
            if matches!(outcome, ElectionOutcome::Won { .. }) {
                return outcome;
            }
        }
    }
    election.outcome()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use ticktape_journal::RealStorage;

    fn spawn_acceptor(dir: &std::path::Path, name: &str) -> SocketAddr {
        let acc = PersistentAcceptor::open(dir.join(name), RealStorage).unwrap();
        let (server, addr) =
            AcceptorServer::bind((Ipv4Addr::LOCALHOST, 0).into(), Arc::new(Mutex::new(acc)))
                .unwrap();
        std::thread::spawn(move || server.serve_forever());
        addr
    }

    #[test]
    fn request_reply_roundtrip() {
        let g = VoteReply::Grant {
            epoch: 7,
            high_water: Seq(42),
        };
        assert_eq!(decode_reply(&encode_reply(g)), Some(g));
        let r = VoteReply::Reject { promised: 9 };
        assert_eq!(decode_reply(&encode_reply(r)), Some(r));
        let q = VoteRequest { epoch: 3 };
        assert_eq!(decode_request(&encode_request(q)), Some(q));
    }

    #[test]
    fn candidate_wins_majority_over_the_network() {
        let dir = tempfile::tempdir().unwrap();
        let addrs: Vec<SocketAddr> = (0..3)
            .map(|i| spawn_acceptor(dir.path(), &format!("a{i}")))
            .collect();
        match run_election(1, &addrs) {
            ElectionOutcome::Won { epoch: 1, .. } => {}
            other => panic!("expected win, got {other:?}"),
        }
    }

    #[test]
    fn no_two_winners_for_one_epoch_over_the_network() {
        let dir = tempfile::tempdir().unwrap();
        let addrs: Vec<SocketAddr> = (0..5)
            .map(|i| spawn_acceptor(dir.path(), &format!("a{i}")))
            .collect();
        // First candidate wins epoch 2.
        assert!(matches!(
            run_election(2, &addrs),
            ElectionOutcome::Won { .. }
        ));
        // A second candidate for the SAME epoch must lose — the acceptors
        // already promised 2.
        assert!(matches!(
            run_election(2, &addrs),
            ElectionOutcome::Lost {
                highest_promised: 2
            }
        ));
        // A higher epoch wins.
        assert!(matches!(
            run_election(3, &addrs),
            ElectionOutcome::Won { .. }
        ));
    }

    #[test]
    fn loses_without_a_reachable_majority() {
        let dir = tempfile::tempdir().unwrap();
        // Only one of three acceptors is live; no majority.
        let live = spawn_acceptor(dir.path(), "live");
        let dead: SocketAddr = (Ipv4Addr::LOCALHOST, 1).into(); // unroutable port
        let addrs = vec![live, dead, dead];
        assert!(!matches!(
            run_election(1, &addrs),
            ElectionOutcome::Won { .. }
        ));
    }
}
