//! Socket shells around the pure [`Reassembler`]: UDP/Unix-datagram A/B
//! publishing, the receiving poll loop, and the TCP retransmitter.

use crate::reassembler::Reassembler;
use crate::wire::{Packet, RetransmitRequest, MAX_PACKET_BYTES, REQUEST_LEN};
use crate::TransportError;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::os::unix::net::UnixDatagram;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use ticktape_core::{Frame, Seq};

/// Anything that can carry packets: UDP sockets, Unix datagram sockets.
pub trait PacketSource {
    /// Receive one datagram into `buf` with a bounded wait. `Ok(None)` on
    /// timeout.
    fn recv_packet(&self, buf: &mut [u8], wait: Duration) -> std::io::Result<Option<usize>>;
}

fn timeout_to_none<T>(result: std::io::Result<T>) -> std::io::Result<Option<T>> {
    match result {
        Ok(v) => Ok(Some(v)),
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ) =>
        {
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

impl PacketSource for UdpSocket {
    fn recv_packet(&self, buf: &mut [u8], wait: Duration) -> std::io::Result<Option<usize>> {
        self.set_read_timeout(Some(wait.max(Duration::from_millis(1))))?;
        timeout_to_none(self.recv(buf))
    }
}

impl PacketSource for UnixDatagram {
    fn recv_packet(&self, buf: &mut [u8], wait: Duration) -> std::io::Result<Option<usize>> {
        self.set_read_timeout(Some(wait.max(Duration::from_millis(1))))?;
        timeout_to_none(self.recv(buf))
    }
}

#[derive(Debug, Clone)]
pub struct PublisherConfig {
    /// Identifies one continuous run of the stream; receivers lock onto it.
    pub session: u64,
    /// Primary feed destination (unicast, or a multicast group).
    pub dest_a: SocketAddr,
    /// Redundant feed destination. `None` disables A/B redundancy.
    pub dest_b: Option<SocketAddr>,
}

/// Publishes sequenced frames as packets on one or two UDP channels.
///
/// One packet per `publish` call (batching is a planned optimization);
/// frames larger than [`MAX_PACKET_BYTES`] still go out — UDP fragments —
/// but the store/gap-fill path guarantees delivery regardless.
pub struct Publisher {
    socket: UdpSocket,
    config: PublisherConfig,
    next_seq: u64,
}

impl Publisher {
    pub fn new(config: PublisherConfig) -> std::io::Result<Publisher> {
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
        Ok(Publisher {
            socket,
            config,
            next_seq: 1,
        })
    }

    /// Send one frame on both channels.
    pub fn publish(&mut self, frame: &Frame) -> Result<(), TransportError> {
        self.next_seq = frame.seq.0 + 1;
        let packet = Packet::Data {
            session: self.config.session,
            frames: vec![frame.clone()],
        };
        self.send(&packet.encode())
    }

    /// Advertise liveness + high-water so receivers can detect tail loss.
    pub fn heartbeat(&mut self) -> Result<(), TransportError> {
        let packet = Packet::Heartbeat {
            session: self.config.session,
            next_seq: Seq(self.next_seq),
        };
        self.send(&packet.encode())
    }

    fn send(&self, bytes: &[u8]) -> Result<(), TransportError> {
        self.socket.send_to(bytes, self.config.dest_a)?;
        if let Some(dest_b) = self.config.dest_b {
            self.socket.send_to(bytes, dest_b)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ReceiverConfig {
    /// First seq this receiver wants delivered (Seq(1) = full history).
    pub from: Seq,
    /// Where to fetch missing ranges (SoupBinTCP-style). `None` means gaps
    /// are unrecoverable errors.
    pub retransmitter: Option<SocketAddr>,
}

/// Receives the sequenced stream from one or two packet sources, delivering
/// frames strictly in seq order and gap-filling over TCP as needed.
pub struct Receiver<Src: PacketSource> {
    channel_a: Src,
    channel_b: Option<Src>,
    reassembler: Reassembler,
    config: ReceiverConfig,
    buf: Vec<u8>,
}

/// Bind a UDP receiver socket, joining the multicast group if `addr` is one.
pub fn bind_udp(addr: SocketAddr) -> std::io::Result<UdpSocket> {
    let socket = UdpSocket::bind(addr)?;
    if let IpAddr::V4(ip) = addr.ip() {
        if ip.is_multicast() {
            socket.join_multicast_v4(&ip, &Ipv4Addr::UNSPECIFIED)?;
        }
    }
    Ok(socket)
}

impl<Src: PacketSource> Receiver<Src> {
    pub fn new(channel_a: Src, channel_b: Option<Src>, config: ReceiverConfig) -> Self {
        Receiver {
            channel_a,
            channel_b,
            reassembler: Reassembler::new(config.from),
            config,
            buf: vec![0u8; 64 * 1024],
        }
    }

    /// Next in-order frame, waiting up to `wait`. `Ok(None)` if nothing new
    /// arrived in time. Gaps are filled transparently from the
    /// retransmitter; a gap with no retransmitter configured is an error.
    pub fn poll(&mut self, wait: Duration) -> Result<Option<Frame>, TransportError> {
        if let Some(frame) = self.reassembler.next_frame() {
            return Ok(Some(frame));
        }
        let deadline = std::time::Instant::now() + wait;
        loop {
            // Drain both channels with short waits so neither starves.
            let slice = Duration::from_millis(2);
            if let Some(len) = self.channel_a.recv_packet(&mut self.buf, slice)? {
                let packet = Packet::decode(&self.buf[..len])?;
                self.reassembler.ingest(packet)?;
            }
            if let Some(channel_b) = &self.channel_b {
                if let Some(len) = channel_b.recv_packet(&mut self.buf, slice)? {
                    let packet = Packet::decode(&self.buf[..len])?;
                    self.reassembler.ingest(packet)?;
                }
            }
            if let Some(frame) = self.reassembler.next_frame() {
                return Ok(Some(frame));
            }
            if let Some((from, count)) = self.reassembler.gap() {
                self.fill_gap(from, count)?;
                if let Some(frame) = self.reassembler.next_frame() {
                    return Ok(Some(frame));
                }
            }
            if std::time::Instant::now() >= deadline {
                return Ok(None);
            }
        }
    }

    /// The seq the next delivered frame will carry.
    pub fn next_expected(&self) -> Seq {
        self.reassembler.next_expected()
    }

    fn fill_gap(&mut self, from: Seq, count: u64) -> Result<(), TransportError> {
        let Some(addr) = self.config.retransmitter else {
            return Err(TransportError::GapUnrecoverable {
                from: from.0,
                count,
            });
        };
        let session = self.reassembler.session().unwrap_or(0);
        let mut wanted = from;
        let mut remaining = count;
        while remaining > 0 {
            let chunk = remaining.min(u32::MAX as u64) as u32;
            let request = RetransmitRequest {
                session,
                from: wanted,
                count: chunk,
            };
            let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
            stream.set_read_timeout(Some(Duration::from_secs(2)))?;
            stream.write_all(&request.encode())?;
            let mut reply = Vec::new();
            stream.read_to_end(&mut reply)?;
            if reply.is_empty() {
                return Err(TransportError::GapUnrecoverable {
                    from: wanted.0,
                    count: remaining,
                });
            }
            let mut served = 0u64;
            let mut cursor = 0usize;
            while cursor < reply.len() {
                // Replies are length-prefixed packets (TCP is a stream).
                if reply.len() - cursor < 4 {
                    return Err(TransportError::Corrupt("truncated retransmit reply"));
                }
                let len =
                    u32::from_le_bytes(reply[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                if reply.len() - cursor < len {
                    return Err(TransportError::Corrupt("truncated retransmit packet"));
                }
                let packet = Packet::decode(&reply[cursor..cursor + len])?;
                if let Packet::Data { frames, .. } = &packet {
                    served += frames.len() as u64;
                }
                cursor += len;
                self.reassembler.ingest(packet)?;
            }
            if served == 0 {
                return Err(TransportError::GapUnrecoverable {
                    from: wanted.0,
                    count: remaining,
                });
            }
            wanted = Seq(wanted.0 + served);
            remaining = remaining.saturating_sub(served);
        }
        Ok(())
    }
}

/// In-memory frame store feeding the retransmitter. Frame at seq `k` lives
/// at index `k-1`; the publisher records every frame it sends.
#[derive(Clone, Default)]
pub struct MemStore {
    frames: Arc<RwLock<Vec<Frame>>>,
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a frame (must be seq-contiguous with what's stored).
    pub fn record(&self, frame: Frame) {
        let mut frames = self.frames.write().unwrap();
        debug_assert_eq!(frame.seq.0, frames.len() as u64 + 1, "gapless store");
        frames.push(frame);
    }

    pub fn range(&self, from: Seq, count: usize) -> Vec<Frame> {
        let frames = self.frames.read().unwrap();
        if from.0 == 0 {
            return Vec::new();
        }
        let start = (from.0 - 1) as usize;
        frames
            .get(start..frames.len().min(start + count))
            .map(<[Frame]>::to_vec)
            .unwrap_or_default()
    }

    pub fn high_water(&self) -> Seq {
        Seq(self.frames.read().unwrap().len() as u64)
    }
}

/// Serves retransmit range requests over TCP from a [`MemStore`].
pub struct Retransmitter {
    listener: TcpListener,
    store: MemStore,
    session: u64,
}

impl Retransmitter {
    /// Bind and return the actual bound address (use port 0 to auto-pick).
    pub fn bind(
        addr: SocketAddr,
        session: u64,
        store: MemStore,
    ) -> std::io::Result<(Retransmitter, SocketAddr)> {
        let listener = TcpListener::bind(addr)?;
        let local = listener.local_addr()?;
        Ok((
            Retransmitter {
                listener,
                store,
                session,
            },
            local,
        ))
    }

    /// Serve requests forever (run on its own thread). Malformed requests
    /// drop the connection; the caller's gap simply stays unfilled.
    pub fn serve_forever(self) {
        for stream in self.listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let _ = self.serve_one(&mut stream);
        }
    }

    fn serve_one(&self, stream: &mut TcpStream) -> Result<(), TransportError> {
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        let mut request_bytes = [0u8; REQUEST_LEN];
        stream.read_exact(&mut request_bytes)?;
        let request = RetransmitRequest::decode(&request_bytes)?;
        if request.session != self.session {
            return Err(TransportError::SessionMismatch {
                expected: self.session,
                got: request.session,
            });
        }
        // Reply with length-prefixed data packets, a few frames per packet.
        let frames = self.store.range(request.from, request.count as usize);
        for chunk in frames.chunks(8) {
            let packet = Packet::Data {
                session: self.session,
                frames: chunk.to_vec(),
            };
            let bytes = packet.encode();
            stream.write_all(&(bytes.len() as u32).to_le_bytes())?;
            stream.write_all(&bytes)?;
        }
        Ok(())
    }
}

/// Convenience: everything a leader needs to feed followers — publisher +
/// store + retransmitter thread. Returns the publisher, the store handle,
/// and the retransmitter's address for `ReceiverConfig`.
pub fn spawn_feed(config: PublisherConfig) -> std::io::Result<(Publisher, MemStore, SocketAddr)> {
    let store = MemStore::new();
    let (retransmitter, addr) = Retransmitter::bind(
        (Ipv4Addr::LOCALHOST, 0).into(),
        config.session,
        store.clone(),
    )?;
    std::thread::spawn(move || retransmitter.serve_forever());
    let publisher = Publisher::new(config)?;
    Ok((publisher, store, addr))
}

// Silence unused-constant lint until batching lands.
const _: usize = MAX_PACKET_BYTES;
