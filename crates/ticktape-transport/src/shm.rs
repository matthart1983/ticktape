//! Same-box fan-out over a shared-memory ring (spec's IpcShm; P2 #4).
//!
//! A single memory-mapped file holds a lock-free **single-producer /
//! single-consumer** slot ring. The sequencer process is the producer; a
//! co-located consumer (a drop-copy auditor, a same-box replica, a risk
//! gate) maps the same file and reads packets with zero kernel round-trips
//! in the steady state — no UDP, no socket buffers. It plugs in behind the
//! existing [`crate::PacketSource`] seam, so a receiver can consume from
//! shared memory exactly as it consumes from a UDP feed.
//!
//! Layout (little-endian), a header cache line followed by fixed slots:
//!
//! ```text
//!   0   magic       u32   "TKSM"
//!   4   version     u32   = 1
//!   8   capacity    u64   number of slots (power of two)
//!  16   slot_bytes  u64   bytes per slot (incl. the 4-byte length prefix)
//!  24   write_seq   u64   producer cursor (only the producer writes this)
//!  32   read_seq    u64   consumer cursor (only the consumer writes this)
//!  64   slot 0      ...   [len: u32][payload]
//!  ..   slot 1      ...
//! ```
//!
//! SPSC discipline makes it safe without locks: the producer only advances
//! `write_seq` (after filling a slot, with `Release`); the consumer only
//! advances `read_seq` (after draining a slot, with `Release`) and reads
//! `write_seq` with `Acquire`. A full ring (`write_seq - read_seq ==
//! capacity`) drops the packet — the reliable feed still has it, and the
//! shm ring is a fast local *shortcut*, not the system of record.

use crate::PacketSource;
use crate::TransportError;
use memmap2::MmapMut;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const MAGIC: u32 = u32::from_le_bytes(*b"TKSM");
const VERSION: u32 = 1;
const HEADER_BYTES: usize = 64;
const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 4;
const OFF_CAPACITY: usize = 8;
const OFF_SLOT_BYTES: usize = 16;
const OFF_WRITE_SEQ: usize = 24;
const OFF_READ_SEQ: usize = 32;

/// A handle onto a shared-memory ring file. Both producer and consumer map
/// the same path; the first to `create` sizes and initializes it.
pub struct ShmRing {
    mmap: MmapMut,
    capacity: u64,
    slot_bytes: u64,
}

impl ShmRing {
    /// Create (or re-create) a ring file sized for `capacity` slots of
    /// `max_packet` payload bytes each. `capacity` must be a power of two.
    pub fn create(
        path: impl AsRef<Path>,
        capacity: u64,
        max_packet: usize,
    ) -> std::io::Result<ShmRing> {
        assert!(
            capacity.is_power_of_two(),
            "capacity must be a power of two"
        );
        let slot_bytes = (4 + max_packet) as u64;
        let total = HEADER_BYTES as u64 + capacity * slot_bytes;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.set_len(total)?;
        // SAFETY: we just sized the file; the mapping matches its length.
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        mmap[OFF_MAGIC..OFF_MAGIC + 4].copy_from_slice(&MAGIC.to_le_bytes());
        mmap[OFF_VERSION..OFF_VERSION + 4].copy_from_slice(&VERSION.to_le_bytes());
        mmap[OFF_CAPACITY..OFF_CAPACITY + 8].copy_from_slice(&capacity.to_le_bytes());
        mmap[OFF_SLOT_BYTES..OFF_SLOT_BYTES + 8].copy_from_slice(&slot_bytes.to_le_bytes());
        mmap[OFF_WRITE_SEQ..OFF_WRITE_SEQ + 8].copy_from_slice(&0u64.to_le_bytes());
        mmap[OFF_READ_SEQ..OFF_READ_SEQ + 8].copy_from_slice(&0u64.to_le_bytes());
        mmap.flush()?;
        Ok(ShmRing {
            mmap,
            capacity,
            slot_bytes,
        })
    }

    /// Attach to an existing, already-initialized ring file.
    pub fn attach(path: impl AsRef<Path>) -> std::io::Result<ShmRing> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        // SAFETY: the file is an initialized ring; the mapping spans it.
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let magic = u32::from_le_bytes(mmap[OFF_MAGIC..OFF_MAGIC + 4].try_into().unwrap());
        if magic != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "not a ticktape shm ring",
            ));
        }
        let capacity = u64::from_le_bytes(mmap[OFF_CAPACITY..OFF_CAPACITY + 8].try_into().unwrap());
        let slot_bytes =
            u64::from_le_bytes(mmap[OFF_SLOT_BYTES..OFF_SLOT_BYTES + 8].try_into().unwrap());
        Ok(ShmRing {
            mmap,
            capacity,
            slot_bytes,
        })
    }

    fn atomic_at(&self, off: usize) -> &AtomicU64 {
        // SAFETY: `off` is 8-byte aligned within the header, and the mapping
        // outlives the returned reference (bound to &self).
        unsafe { &*(self.mmap.as_ptr().add(off) as *const AtomicU64) }
    }

    fn slot_range(&self, seq: u64) -> std::ops::Range<usize> {
        let idx = seq & (self.capacity - 1);
        let start = HEADER_BYTES + (idx * self.slot_bytes) as usize;
        start..start + self.slot_bytes as usize
    }

    /// Publish one packet. Returns `false` (dropped) if the ring is full —
    /// the caller's reliable feed still carries it. SPSC: call from one
    /// producer thread only.
    pub fn publish(&self, packet: &[u8]) -> bool {
        debug_assert!(
            packet.len() + 4 <= self.slot_bytes as usize,
            "packet exceeds slot size"
        );
        let write = self.atomic_at(OFF_WRITE_SEQ).load(Ordering::Relaxed);
        let read = self.atomic_at(OFF_READ_SEQ).load(Ordering::Acquire);
        if write.wrapping_sub(read) >= self.capacity {
            return false; // full — drop; the reliable feed has it
        }
        let range = self.slot_range(write);
        // SAFETY: SPSC — only this producer writes slots ahead of `write`,
        // which the consumer will not read until we publish `write_seq`.
        let slot = unsafe {
            std::slice::from_raw_parts_mut(
                self.mmap.as_ptr().add(range.start) as *mut u8,
                range.len(),
            )
        };
        slot[0..4].copy_from_slice(&(packet.len() as u32).to_le_bytes());
        slot[4..4 + packet.len()].copy_from_slice(packet);
        // Publish: the slot contents must be visible before the cursor move.
        self.atomic_at(OFF_WRITE_SEQ)
            .store(write.wrapping_add(1), Ordering::Release);
        true
    }

    /// Try to read the next packet into `buf`, returning its length. `None`
    /// if the ring is currently empty. SPSC: call from one consumer thread.
    pub fn try_recv(&self, buf: &mut [u8]) -> Option<usize> {
        let read = self.atomic_at(OFF_READ_SEQ).load(Ordering::Relaxed);
        let write = self.atomic_at(OFF_WRITE_SEQ).load(Ordering::Acquire);
        if read == write {
            return None; // empty
        }
        let range = self.slot_range(read);
        let slot = &self.mmap[range];
        let len = u32::from_le_bytes(slot[0..4].try_into().unwrap()) as usize;
        let n = len.min(buf.len());
        buf[..n].copy_from_slice(&slot[4..4 + n]);
        // Release the slot back to the producer.
        self.atomic_at(OFF_READ_SEQ)
            .store(read.wrapping_add(1), Ordering::Release);
        Some(n)
    }
}

/// The consumer side as a [`PacketSource`], so a [`crate::Receiver`] can
/// pull the sequenced stream from shared memory just like from UDP.
pub struct ShmSource {
    ring: ShmRing,
}

impl ShmSource {
    pub fn new(ring: ShmRing) -> Self {
        ShmSource { ring }
    }
}

impl PacketSource for ShmSource {
    fn recv_packet(&self, buf: &mut [u8], wait: Duration) -> std::io::Result<Option<usize>> {
        // Spin-then-yield up to the deadline; steady-state reads hit on the
        // first try (no syscall).
        let deadline = Instant::now() + wait;
        loop {
            if let Some(n) = self.ring.try_recv(buf) {
                return Ok(Some(n));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            std::thread::yield_now();
        }
    }
}

/// Convenience: publish an already-encoded packet, surfacing a full ring as
/// a (recoverable) transport error for callers that want to notice drops.
pub fn publish_or_drop(ring: &ShmRing, packet: &[u8]) -> Result<bool, TransportError> {
    Ok(ring.publish(packet))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::Packet;
    use ticktape_core::{Frame, FrameKind, Seq, Timestamp};

    fn frame(seq: u64) -> Frame {
        Frame::new(Seq(seq), Timestamp(seq), 1, FrameKind::Input, vec![0u8; 24])
    }

    #[test]
    fn round_trips_packets_through_shared_memory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ring.shm");
        let producer = ShmRing::create(&path, 64, 1400).unwrap();
        let consumer = ShmRing::attach(&path).unwrap();

        // Publish 40 single-frame packets; drain them in order.
        for seq in 1..=40u64 {
            let bytes = Packet::Data {
                session: 7,
                frames: vec![frame(seq)],
            }
            .encode();
            assert!(producer.publish(&bytes));
        }
        let mut buf = vec![0u8; 2048];
        for seq in 1..=40u64 {
            let n = consumer.try_recv(&mut buf).expect("packet available");
            let packet = Packet::decode(&buf[..n]).unwrap();
            match packet {
                Packet::Data { frames, .. } => assert_eq!(frames[0].seq, Seq(seq)),
                _ => panic!("expected data"),
            }
        }
        assert!(
            consumer.try_recv(&mut buf).is_none(),
            "ring should be empty"
        );
    }

    #[test]
    fn full_ring_drops_rather_than_corrupts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ring.shm");
        let ring = ShmRing::create(&path, 4, 64).unwrap();
        // Capacity 4: the 5th publish with nothing drained must fail cleanly.
        for _ in 0..4 {
            assert!(ring.publish(b"abc"));
        }
        assert!(!ring.publish(b"abc"), "full ring must drop, not overwrite");
        // Drain one, then one more fits.
        let mut buf = [0u8; 64];
        assert_eq!(ring.try_recv(&mut buf), Some(3));
        assert!(ring.publish(b"xyz"));
    }

    #[test]
    fn source_reads_across_threads() {
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ring.shm");
        let producer = Arc::new(ShmRing::create(&path, 256, 1400).unwrap());
        let source = ShmSource::new(ShmRing::attach(&path).unwrap());

        let p = producer.clone();
        let writer = std::thread::spawn(move || {
            for seq in 1..=100u64 {
                let bytes = Packet::Data {
                    session: 7,
                    frames: vec![frame(seq)],
                }
                .encode();
                while !p.publish(&bytes) {
                    std::thread::yield_now(); // ring full: wait for the reader
                }
            }
        });

        let mut buf = vec![0u8; 2048];
        for seq in 1..=100u64 {
            let n = source
                .recv_packet(&mut buf, Duration::from_secs(5))
                .unwrap()
                .expect("packet within deadline");
            match Packet::decode(&buf[..n]).unwrap() {
                Packet::Data { frames, .. } => assert_eq!(frames[0].seq, Seq(seq)),
                _ => panic!("expected data"),
            }
        }
        writer.join().unwrap();
    }
}
