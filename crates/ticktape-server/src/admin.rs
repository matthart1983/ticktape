//! The admin / observability plane — the operator's window into a running
//! deployment.
//!
//! Both reviewed production platforms treat operability as a product
//! feature (jimgreco/core: a telnet shell to inspect any node; Aeron:
//! counters files). ticktape exposes a [`ServerStats`] snapshot per server
//! and a dependency-free HTTP endpoint ([`serve_metrics`]) that renders it
//! in Prometheus text-exposition format, so `curl host:port/metrics`
//! answers the questions an operator running the manual-failover
//! deployment actually asks: *is it healthy, which node leads, how far
//! behind is each standby, when was the last snapshot, is disk bounded.*
//!
//! Prometheus format is the sweet spot — human-readable under `curl`,
//! scrapeable by a real Prometheus, and zero dependencies (a tiny
//! hand-rolled HTTP/1.1 responder, same discipline as the retransmitter's
//! TCP server).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

/// A point-in-time view of one server, cheap to gather.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerStats {
    /// This server's index in the deployment.
    pub node: usize,
    /// `"leader"` or `"follower"`.
    pub role: &'static str,
    /// Current leadership epoch.
    pub epoch: u64,
    /// Highest seq this node has sequenced (leader) or applied (follower).
    pub seq: u64,
    /// Replication lag in frames: `leader_high_water - seq`. `0` for a
    /// leader and for a caught-up follower.
    pub lag: u64,
    /// Seq of the most recent durable snapshot, or 0 if none yet.
    pub snapshot_seq: u64,
    /// Journal segment files on disk — bounded under 24×7 compaction.
    pub journal_segments: u64,
}

impl ServerStats {
    /// Render as Prometheus text exposition. Stable metric names so a
    /// dashboard survives restarts; the `node` label distinguishes servers
    /// when several are scraped.
    pub fn to_prometheus(&self) -> String {
        let n = self.node;
        let is_leader = if self.role == "leader" { 1 } else { 0 };
        format!(
            "# HELP ticktape_role 1 if this node is the leader, else 0\n\
             # TYPE ticktape_role gauge\n\
             ticktape_role{{node=\"{n}\"}} {is_leader}\n\
             # HELP ticktape_epoch current leadership epoch\n\
             # TYPE ticktape_epoch counter\n\
             ticktape_epoch{{node=\"{n}\"}} {}\n\
             # HELP ticktape_seq highest sequenced/applied seq\n\
             # TYPE ticktape_seq counter\n\
             ticktape_seq{{node=\"{n}\"}} {}\n\
             # HELP ticktape_lag_frames replication lag behind the leader\n\
             # TYPE ticktape_lag_frames gauge\n\
             ticktape_lag_frames{{node=\"{n}\"}} {}\n\
             # HELP ticktape_snapshot_seq seq of the latest durable snapshot\n\
             # TYPE ticktape_snapshot_seq counter\n\
             ticktape_snapshot_seq{{node=\"{n}\"}} {}\n\
             # HELP ticktape_journal_segments journal segment files on disk\n\
             # TYPE ticktape_journal_segments gauge\n\
             ticktape_journal_segments{{node=\"{n}\"}} {}\n",
            self.epoch, self.seq, self.lag, self.snapshot_seq, self.journal_segments,
        )
    }
}

/// Serve `GET /metrics` forever from a live stats source (run on its own
/// thread). `stats` is called per request, so it always reflects current
/// state. Any request path other than `/metrics` gets a 404; a `/healthz`
/// convenience returns 200 `ok`.
pub fn serve_metrics<F>(listener: TcpListener, stats: F)
where
    F: Fn() -> ServerStats + Send + 'static,
{
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else { continue };
        let _ = serve_one(&mut stream, &stats);
    }
}

/// Bind a metrics listener, returning it plus the bound address.
pub fn bind_metrics(addr: SocketAddr) -> std::io::Result<(TcpListener, SocketAddr)> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    Ok((listener, local))
}

fn serve_one<F: Fn() -> ServerStats>(stream: &mut TcpStream, stats: &F) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf)?;
    let req = String::from_utf8_lossy(&buf[..n]);
    let path = req
        .split_whitespace()
        .nth(1) // "GET <path> HTTP/1.1"
        .unwrap_or("/");
    let (status, content_type, body) = if path.starts_with("/metrics") {
        (
            "200 OK",
            "text/plain; version=0.0.4",
            stats().to_prometheus(),
        )
    } else if path.starts_with("/healthz") {
        ("200 OK", "text/plain", "ok\n".to_string())
    } else {
        ("404 Not Found", "text/plain", "not found\n".to_string())
    };
    let response = format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ServerStats {
        ServerStats {
            node: 2,
            role: "follower",
            epoch: 3,
            seq: 998,
            lag: 5,
            snapshot_seq: 500,
            journal_segments: 2,
        }
    }

    #[test]
    fn prometheus_render_has_all_metrics() {
        let text = sample().to_prometheus();
        assert!(text.contains("ticktape_role{node=\"2\"} 0"));
        assert!(text.contains("ticktape_epoch{node=\"2\"} 3"));
        assert!(text.contains("ticktape_seq{node=\"2\"} 998"));
        assert!(text.contains("ticktape_lag_frames{node=\"2\"} 5"));
        assert!(text.contains("ticktape_snapshot_seq{node=\"2\"} 500"));
        assert!(text.contains("ticktape_journal_segments{node=\"2\"} 2"));
    }

    #[test]
    fn http_endpoint_serves_metrics_and_healthz() {
        use std::net::Ipv4Addr;
        let (listener, addr) = bind_metrics((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
        std::thread::spawn(move || serve_metrics(listener, sample));

        let fetch = |path: &str| -> String {
            let mut s = TcpStream::connect(addr).unwrap();
            s.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
                .unwrap();
            let mut out = String::new();
            s.read_to_string(&mut out).unwrap();
            out
        };

        let metrics = fetch("/metrics");
        assert!(metrics.contains("200 OK"));
        assert!(metrics.contains("ticktape_seq{node=\"2\"} 998"));

        let health = fetch("/healthz");
        assert!(health.contains("200 OK") && health.contains("ok"));

        let missing = fetch("/nope");
        assert!(missing.contains("404 Not Found"));
    }
}
