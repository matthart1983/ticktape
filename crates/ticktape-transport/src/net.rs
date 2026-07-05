//! Socket shells around the pure [`Reassembler`]: UDP/Unix-datagram A/B
//! publishing, the receiving poll loop, and the TCP retransmitter.

use crate::reassembler::Reassembler;
use crate::wire::{self, data_packet_len, Packet, RetransmitRequest, MAX_PACKET_BYTES, REQUEST_LEN};
use crate::TransportError;
use std::collections::VecDeque;
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

/// Split `frames` into the largest seq-contiguous runs that each fit in one
/// packet (≤ [`MAX_PACKET_BYTES`]). A single oversized frame gets its own
/// window (it fragments on the wire). Assumes the input is already
/// seq-contiguous, as the sequencer stream is.
fn pack_windows(frames: &[Frame]) -> impl Iterator<Item = &[Frame]> {
    let mut start = 0;
    std::iter::from_fn(move || {
        if start >= frames.len() {
            return None;
        }
        let mut end = start;
        while end < frames.len() {
            // Would adding this frame overflow the packet? (Always take at
            // least one, so an oversized frame still makes progress.)
            if end > start && data_packet_len(&frames[start..=end]) > MAX_PACKET_BYTES {
                break;
            }
            end += 1;
        }
        let window = &frames[start..end];
        start = end;
        Some(window)
    })
}

/// Publishes sequenced frames as packets on one or two UDP channels.
///
/// [`Publisher::publish_batch`] packs as many seq-contiguous frames as fit
/// into each packet (up to [`MAX_PACKET_BYTES`]) — one syscall per packet
/// instead of per frame — encoding through a reusable buffer so the fan-out
/// path allocates nothing per frame. Frames larger than the budget still go
/// out alone (UDP fragments; the store/gap-fill path guarantees delivery
/// regardless).
pub struct Publisher {
    socket: UdpSocket,
    config: PublisherConfig,
    next_seq: u64,
    /// Reusable packet-encode scratch (alloc-free hot path).
    buf: Vec<u8>,
}

impl Publisher {
    pub fn new(config: PublisherConfig) -> std::io::Result<Publisher> {
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
        Ok(Publisher {
            socket,
            config,
            next_seq: 1,
            buf: Vec::with_capacity(MAX_PACKET_BYTES),
        })
    }

    /// Send one frame on both channels.
    pub fn publish(&mut self, frame: &Frame) -> Result<(), TransportError> {
        self.next_seq = frame.seq.0 + 1;
        wire::encode_data_into(self.config.session, std::slice::from_ref(frame), &mut self.buf);
        self.send_buf()
    }

    /// Send one frame to a single explicit destination — for fan-out to N
    /// followers, where the publisher's fixed A/B dests aren't enough.
    pub fn publish_to(&mut self, frame: &Frame, dest: SocketAddr) -> Result<(), TransportError> {
        self.next_seq = frame.seq.0 + 1;
        wire::encode_data_into(self.config.session, std::slice::from_ref(frame), &mut self.buf);
        self.socket.send_to(&self.buf, dest)?;
        Ok(())
    }

    /// Publish a run of **seq-contiguous** frames on both channels, packing
    /// as many as fit per packet. One `send` per packet rather than per
    /// frame — the throughput win for a busy sequencer. A frame too large to
    /// fit alone still goes out (fragmented).
    pub fn publish_batch(&mut self, frames: &[Frame]) -> Result<(), TransportError> {
        for window in pack_windows(frames) {
            self.next_seq = window[window.len() - 1].seq.0 + 1;
            wire::encode_data_into(self.config.session, window, &mut self.buf);
            self.send_buf()?;
        }
        Ok(())
    }

    /// [`Publisher::publish_batch`] to a single explicit destination.
    pub fn publish_batch_to(
        &mut self,
        frames: &[Frame],
        dest: SocketAddr,
    ) -> Result<(), TransportError> {
        for window in pack_windows(frames) {
            self.next_seq = window[window.len() - 1].seq.0 + 1;
            wire::encode_data_into(self.config.session, window, &mut self.buf);
            self.socket.send_to(&self.buf, dest)?;
        }
        Ok(())
    }

    /// Send the current encode buffer on both channels.
    fn send_buf(&self) -> Result<(), TransportError> {
        self.socket.send_to(&self.buf, self.config.dest_a)?;
        if let Some(dest_b) = self.config.dest_b {
            self.socket.send_to(&self.buf, dest_b)?;
        }
        Ok(())
    }

    /// Advertise liveness + high-water so receivers can detect tail loss.
    pub fn heartbeat(&mut self) -> Result<(), TransportError> {
        let packet = Packet::Heartbeat {
            session: self.config.session,
            next_seq: Seq(self.next_seq),
        };
        self.send(&packet.encode())
    }

    /// Advertise liveness + high-water to a single explicit destination —
    /// the heartbeat counterpart of [`Publisher::publish_to`], for fan-out
    /// to N follower feeds.
    pub fn heartbeat_to(&mut self, dest: SocketAddr) -> Result<(), TransportError> {
        let packet = Packet::Heartbeat {
            session: self.config.session,
            next_seq: Seq(self.next_seq),
        };
        self.socket.send_to(&packet.encode(), dest)?;
        Ok(())
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

    /// Point gap-fill at a (possibly new) retransmitter — e.g. a follower
    /// switching to a freshly promoted leader.
    pub fn set_retransmitter(&mut self, addr: Option<SocketAddr>) {
        self.config.retransmitter = addr;
    }

    /// Highest seq the upstream leader has announced (via data/heartbeats) —
    /// the numerator for a follower's replication lag.
    pub fn announced_high_water(&self) -> Seq {
        self.reassembler.announced_high_water()
    }

    /// Monotonic count of packets received from upstream (data + heartbeats).
    /// A follower's failure detector samples this each poll: if it has not
    /// advanced within the timeout, the leader is presumed dead. Heartbeats
    /// count, so an idle leader is not mistaken for a failed one.
    pub fn packets_seen(&self) -> u64 {
        self.reassembler.packets_seen()
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

/// Anything that can serve historical frames by seq range for gap-fill.
///
/// The retransmit path has two natural implementations (the
/// jimgreco/core `MoldRepeater`/`MoldRewinder` split): a bounded in-memory
/// [`MemStore`] **repeater** for live recent-window gap-fill, and a
/// journal-backed [`JournalRewinder`] for historical ranges that have
/// aged out of the window. [`ChainStore`] tries one then the other, so a
/// long-running feed serves recent gaps from RAM and old gaps from disk
/// without ever holding unbounded history in memory.
pub trait FrameStore: Send + Sync {
    /// Up to `count` frames starting at `from`, in seq order. May return
    /// fewer (or none) if this store doesn't hold that range.
    fn range(&self, from: Seq, count: usize) -> Vec<Frame>;
    /// Highest seq this store can serve (`Seq::GENESIS` if empty).
    fn high_water(&self) -> Seq;
    /// Lowest seq this store can serve (`Seq(1)` for a full store; higher
    /// for a bounded window or a compacted journal).
    fn low_water(&self) -> Seq;
}

/// Bounded in-memory recent-window store — the **repeater**. Retains at
/// most `capacity` most-recent frames; older frames age out (they are
/// served from a [`JournalRewinder`] instead). This is the bound that
/// makes 24×7 operation possible — the old unbounded `Vec` leaked the
/// entire feed history.
#[derive(Clone)]
pub struct MemStore {
    frames: Arc<RwLock<VecDeque<Frame>>>,
    capacity: usize,
}

impl MemStore {
    /// A store retaining the most recent `capacity` frames.
    pub fn with_capacity(capacity: usize) -> Self {
        MemStore {
            frames: Arc::new(RwLock::new(VecDeque::new())),
            capacity: capacity.max(1),
        }
    }

    /// Default recent window (256k frames).
    pub fn new() -> Self {
        Self::with_capacity(256 * 1024)
    }

    /// Record a frame (must be seq-contiguous with what's stored).
    pub fn record(&self, frame: Frame) {
        let mut frames = self.frames.write().unwrap();
        debug_assert!(
            frames
                .back()
                .is_none_or(|last| frame.seq.0 == last.seq.0 + 1),
            "gapless store"
        );
        frames.push_back(frame);
        while frames.len() > self.capacity {
            frames.pop_front();
        }
    }
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameStore for MemStore {
    fn range(&self, from: Seq, count: usize) -> Vec<Frame> {
        let frames = self.frames.read().unwrap();
        let Some(base) = frames.front().map(|f| f.seq.0) else {
            return Vec::new();
        };
        if from.0 < base {
            return Vec::new(); // aged out of the window
        }
        let start = (from.0 - base) as usize;
        frames.iter().skip(start).take(count).cloned().collect()
    }

    fn high_water(&self) -> Seq {
        Seq(self.frames.read().unwrap().back().map_or(0, |f| f.seq.0))
    }

    fn low_water(&self) -> Seq {
        Seq(self.frames.read().unwrap().front().map_or(1, |f| f.seq.0))
    }
}

/// Journal-backed historical store — the **rewinder**. Serves ranges by
/// re-reading the journal on demand, so late joiners and old gap-fills
/// cost disk reads, not RAM. Bounded from below by journal compaction
/// (post-compaction it serves snapshot-tail onward).
#[derive(Clone)]
pub struct JournalRewinder<St: ticktape_journal::Storage + Clone> {
    config: ticktape_journal::JournalConfig,
    storage: St,
}

impl<St: ticktape_journal::Storage + Clone> JournalRewinder<St> {
    pub fn new(config: ticktape_journal::JournalConfig, storage: St) -> Self {
        JournalRewinder { config, storage }
    }

    fn recovered(&self) -> Option<ticktape_journal::Recovered<St>> {
        ticktape_journal::Journal::open_with(self.config.clone(), self.storage.clone()).ok()
    }
}

impl<St: ticktape_journal::Storage + Clone + Send + Sync + 'static> FrameStore
    for JournalRewinder<St>
{
    fn range(&self, from: Seq, count: usize) -> Vec<Frame> {
        let Some(rec) = self.recovered() else {
            return Vec::new();
        };
        rec.frames
            .into_iter()
            .filter(|f| f.seq >= from)
            .take(count)
            .collect()
    }

    fn high_water(&self) -> Seq {
        self.recovered()
            .map_or(Seq::GENESIS, |r| r.journal.last_seq())
    }

    fn low_water(&self) -> Seq {
        self.recovered().map_or(Seq(1), |r| r.first_seq)
    }
}

/// Try `primary` (the repeater), fall back to `secondary` (the rewinder)
/// for ranges the repeater has aged out. The whole point of the split.
#[derive(Clone)]
pub struct ChainStore<A, B> {
    pub primary: A,
    pub secondary: B,
}

impl<A: FrameStore, B: FrameStore> FrameStore for ChainStore<A, B> {
    fn range(&self, from: Seq, count: usize) -> Vec<Frame> {
        let primary = self.primary.range(from, count);
        // Use the repeater only if it actually covers `from`; otherwise the
        // range starts before its window and the rewinder must serve it.
        if primary.first().map(|f| f.seq) == Some(from) {
            primary
        } else {
            self.secondary.range(from, count)
        }
    }

    fn high_water(&self) -> Seq {
        self.primary.high_water().max(self.secondary.high_water())
    }

    fn low_water(&self) -> Seq {
        self.primary.low_water().min(self.secondary.low_water())
    }
}

/// Serves retransmit range requests over TCP from any [`FrameStore`].
pub struct Retransmitter<S: FrameStore = MemStore> {
    listener: TcpListener,
    store: S,
    session: u64,
}

impl<S: FrameStore> Retransmitter<S> {
    /// Bind and return the actual bound address (use port 0 to auto-pick).
    pub fn bind(
        addr: SocketAddr,
        session: u64,
        store: S,
    ) -> std::io::Result<(Retransmitter<S>, SocketAddr)> {
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
    pub fn serve_forever(self)
    where
        S: 'static,
    {
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
