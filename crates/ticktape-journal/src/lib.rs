//! The Ticktape journal: a segmented, append-only, CRC-checked durable log
//! of the sequenced input stream.
//!
//! The journal is the system of record. In-memory state is a derived,
//! disposable projection: any component rebuilds from
//! `snapshot + replay(journal)`. Only *inputs* (and control frames like
//! ticks) are journaled — outputs are deterministically recomputable and
//! are not (the LMAX discipline).
//!
//! # On-disk layout
//!
//! ```text
//! journal/
//! ├── 00000000000000000001.seg   # segments, named by the first seq they contain
//! ├── 00000000000001048577.seg
//! └── ...
//! ```
//!
//! Each segment is a 28-byte header (`magic`, `format_version`, `first_seq`,
//! `epoch`, CRC) followed by back-to-back encoded [`Frame`]s. Every frame
//! carries its own header + payload CRCs, so a torn tail write is detected
//! on recovery and truncated to the last intact frame.
//!
//! Deferred past M0 (tracked in the spec): the sidecar `manifest` file
//! (recovery scans segment filenames instead), sealed segment footers, and
//! `O_DIRECT`/`io_uring` I/O. Group commit is time-window based via
//! [`FsyncPolicy::Micros`].

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod iouring;
pub mod snapshot;
pub mod storage;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use iouring::{IoUringFile, IoUringStorage};
pub use snapshot::{LoadedSnapshot, SnapshotStore};
pub use storage::{RealStorage, Storage, StorageFile};

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use ticktape_core::crc32c::crc32c;
use ticktape_core::{Frame, FrameError, Seq};

const SEGMENT_MAGIC: &[u8; 4] = b"TKTJ";
const FORMAT_VERSION: u32 = 1;
/// magic(4) + version(4) + first_seq(8) + epoch(8) + crc(4)
const SEGMENT_HEADER_LEN: usize = 28;

/// When the journal calls `fdatasync` — the dominant single-node
/// latency/durability dial.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// Sync after every appended frame. Safest, slowest.
    EveryFrame,
    /// Time-bounded group commit: sync at most once per window; frames
    /// appended inside a window are at risk until it closes.
    Micros(u64),
    /// Never sync; rely on replication (or accept loss) for durability.
    Never,
}

#[derive(Debug, Clone)]
pub struct JournalConfig {
    pub dir: PathBuf,
    /// Roll to a new segment once the current one would exceed this size.
    pub segment_bytes: u64,
    pub fsync: FsyncPolicy,
}

impl JournalConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        JournalConfig {
            dir: dir.into(),
            segment_bytes: 1 << 30, // 1 GiB
            fsync: FsyncPolicy::Micros(50),
        }
    }
}

#[derive(Debug)]
pub enum JournalError {
    Io(std::io::Error),
    /// A sealed (non-final) segment failed validation; recovery cannot
    /// safely continue past it.
    Corrupt {
        segment: PathBuf,
        offset: u64,
        reason: String,
    },
    /// Segment header was malformed or version-incompatible.
    BadSegmentHeader {
        segment: PathBuf,
        reason: String,
    },
    /// Frame sequence numbers are not gapless/monotonic.
    NonContiguousSeq {
        segment: PathBuf,
        expected: Seq,
        found: Seq,
    },
    /// `append` was called with a seq that is not `last_seq + 1`.
    OutOfOrderAppend {
        expected: Seq,
        found: Seq,
    },
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JournalError::Io(e) => write!(f, "journal I/O error: {e}"),
            JournalError::Corrupt {
                segment,
                offset,
                reason,
            } => write!(
                f,
                "corrupt journal segment {} at offset {offset}: {reason}",
                segment.display()
            ),
            JournalError::BadSegmentHeader { segment, reason } => {
                write!(f, "bad segment header in {}: {reason}", segment.display())
            }
            JournalError::NonContiguousSeq {
                segment,
                expected,
                found,
            } => write!(
                f,
                "non-contiguous seq in {}: expected {expected}, found {found}",
                segment.display()
            ),
            JournalError::OutOfOrderAppend { expected, found } => {
                write!(
                    f,
                    "out-of-order append: expected seq {expected}, got {found}"
                )
            }
        }
    }
}

impl std::error::Error for JournalError {}

impl From<std::io::Error> for JournalError {
    fn from(e: std::io::Error) -> Self {
        JournalError::Io(e)
    }
}

/// Result of opening a journal: the writer, positioned at the tail, plus
/// every intact frame for replay.
pub struct Recovered<St: Storage = RealStorage> {
    pub journal: Journal<St>,
    /// All journaled frames in seq order, materialized. For memory-bounded
    /// recovery on a large journal, prefer [`Journal::replay_open`], which
    /// streams frames to a callback and never builds this `Vec`.
    pub frames: Vec<Frame>,
    /// The seq of the first frame the journal holds. `Seq(1)` for an
    /// uncompacted journal; greater after compaction — the caller must
    /// have a snapshot covering everything before it.
    pub first_seq: Seq,
    /// Whether a torn tail was detected and truncated during recovery.
    pub truncated_torn_tail: bool,
}

/// Metadata from a [`Journal::replay_open`] streaming recovery — everything
/// [`Recovered`] carries except the materialized frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayMeta {
    /// Seq of the first frame the journal holds (see [`Recovered::first_seq`]).
    pub first_seq: Seq,
    /// Seq of the last frame the journal holds (`first_seq - 1` if empty).
    pub last_seq: Seq,
    /// Whether a torn tail was detected and truncated during recovery.
    pub truncated_torn_tail: bool,
}

/// Append-only writer over the segmented log.
pub struct Journal<St: Storage = RealStorage> {
    config: JournalConfig,
    storage: St,
    /// Open segment writer plus its current byte size.
    current: Option<OpenSegment<St::File>>,
    last_seq: Seq,
    epoch: u64,
    last_sync: Instant,
    dirty: bool,
    /// Reusable encode buffer so `append`/`append_batch` don't allocate a
    /// fresh `Vec` per frame on the hot path (the alloc-free journal path).
    scratch: Vec<u8>,
}

struct OpenSegment<F> {
    file: F,
    size: u64,
    frames: u64,
}

impl Journal<RealStorage> {
    /// Open (or create) a journal on the real filesystem. See [`Self::open_with`].
    pub fn open(config: JournalConfig) -> Result<Recovered, JournalError> {
        Self::open_with(config, RealStorage)
    }
}

impl<St: Storage> Journal<St> {
    /// Open (or create) the journal at `config.dir`, validating and
    /// replaying every segment. A torn tail in the final segment is
    /// truncated to the last intact frame; corruption anywhere else is an
    /// error.
    ///
    /// A compacted journal (segments pruned below a snapshot) starts at
    /// some seq k > 1; the stream is validated gapless from the first
    /// *remaining* segment. Whether history before k is covered by a
    /// snapshot is the caller's responsibility ([`Recovered::first_seq`]
    /// says where the journal starts).
    pub fn open_with(config: JournalConfig, storage: St) -> Result<Recovered<St>, JournalError> {
        let mut frames = Vec::new();
        let (journal, meta) = Self::open_inner(config, storage, |frame| frames.push(frame))?;
        Ok(Recovered {
            journal,
            frames,
            first_seq: meta.first_seq,
            truncated_torn_tail: meta.truncated_torn_tail,
        })
    }

    /// **Streaming recovery.** Invoke `on_frame` for each intact frame in seq
    /// order *without* materializing them, returning the tail-positioned
    /// writer plus recovery metadata. Peak memory is one frame + one segment
    /// buffer, independent of journal length — the caller (crash recovery
    /// replay, `verify_replay`) applies each frame as it arrives instead of
    /// holding the whole `Vec<Frame>`.
    pub fn replay_open(
        config: JournalConfig,
        storage: St,
        on_frame: impl FnMut(Frame),
    ) -> Result<(Journal<St>, ReplayMeta), JournalError> {
        Self::open_inner(config, storage, on_frame)
    }

    /// The shared segment scan behind both [`Journal::open_with`] (which
    /// collects frames into a `Vec`) and [`Journal::replay_open`] (which
    /// streams them to a callback). Validates contiguity, truncates a torn
    /// tail on the final segment, reopens that segment for appending, and
    /// returns the writer + metadata.
    fn open_inner(
        config: JournalConfig,
        storage: St,
        mut on_frame: impl FnMut(Frame),
    ) -> Result<(Journal<St>, ReplayMeta), JournalError> {
        storage.create_dir_all(&config.dir)?;

        let mut segment_paths: Vec<PathBuf> = storage
            .list_dir(&config.dir)?
            .into_iter()
            .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("seg"))
            .collect();
        segment_paths.sort();

        let mut count: u64 = 0;
        let mut first_seq: Option<Seq> = None;
        let mut expected_next = Seq::GENESIS.next();
        let mut truncated = false;
        let mut epoch = 0u64;

        for (i, path) in segment_paths.iter().enumerate() {
            let is_last = i == segment_paths.len() - 1;
            let bytes = storage.read(path)?;
            let header = parse_segment_header(path, &bytes)?;
            if i == 0 {
                // Compaction may have removed leading segments; the
                // journal's history now starts wherever this one does.
                expected_next = header.first_seq;
            }
            if header.first_seq != expected_next {
                return Err(JournalError::NonContiguousSeq {
                    segment: path.clone(),
                    expected: expected_next,
                    found: header.first_seq,
                });
            }
            epoch = header.epoch;

            let mut cursor = &bytes[SEGMENT_HEADER_LEN..];
            loop {
                if cursor.is_empty() {
                    break;
                }
                let offset = (bytes.len() - cursor.len()) as u64;
                match Frame::read_from(&mut cursor) {
                    Ok(frame) => {
                        if frame.seq != expected_next {
                            return Err(JournalError::NonContiguousSeq {
                                segment: path.clone(),
                                expected: expected_next,
                                found: frame.seq,
                            });
                        }
                        expected_next = frame.seq.next();
                        first_seq.get_or_insert(frame.seq);
                        count += 1;
                        on_frame(frame);
                    }
                    Err(err) if is_last => {
                        // Torn tail: keep the intact prefix, truncate the rest.
                        storage.truncate(path, offset)?;
                        truncated = true;
                        let _ = err;
                        break;
                    }
                    Err(err) => {
                        return Err(JournalError::Corrupt {
                            segment: path.clone(),
                            offset,
                            reason: frame_error_reason(&err),
                        });
                    }
                }
            }
        }

        let last_seq = Seq(expected_next.0.saturating_sub(1));
        // Empty journal: history "starts" at whatever would come next.
        let first_seq = first_seq.unwrap_or(Seq(last_seq.0 + 1));

        // Re-open the final segment for appending, if any.
        let current = match segment_paths.last() {
            Some(path) => {
                let file = storage.open_append(path)?;
                let size = file.len()?;
                Some(OpenSegment {
                    file,
                    size,
                    frames: count, // only used for roll gating; ≥1 is what matters
                })
            }
            None => None,
        };

        let journal = Journal {
            config,
            storage,
            current,
            last_seq,
            epoch,
            last_sync: Instant::now(),
            dirty: false,
            scratch: Vec::new(),
        };
        Ok((
            journal,
            ReplayMeta {
                first_seq,
                last_seq,
                truncated_torn_tail: truncated,
            },
        ))
    }

    /// Compaction: delete every segment whose frames all have
    /// `seq <= cutoff` (typically the oldest *retained* snapshot's seq, so
    /// every remaining snapshot still has the journal tail it needs). The
    /// active (last) segment is never deleted. Returns the number of
    /// segments removed.
    ///
    /// This is what bounds disk for 24×7 operation: snapshot, prune old
    /// snapshots, then compact below the oldest kept one.
    pub fn compact_below(&mut self, cutoff: Seq) -> Result<u64, JournalError> {
        let mut segment_paths: Vec<PathBuf> = self
            .storage
            .list_dir(&self.config.dir)?
            .into_iter()
            .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("seg"))
            .collect();
        segment_paths.sort();
        if segment_paths.len() <= 1 {
            return Ok(0);
        }
        // Segments are named by their first seq; segment i holds seqs
        // [first[i], first[i+1]) — deletable iff first[i+1] <= cutoff + 1.
        let firsts: Vec<u64> = segment_paths
            .iter()
            .map(|path| {
                path.file_stem()
                    .and_then(|stem| stem.to_str())
                    .and_then(|stem| stem.parse::<u64>().ok())
                    .ok_or_else(|| JournalError::BadSegmentHeader {
                        segment: path.clone(),
                        reason: "segment filename is not a seq".to_string(),
                    })
            })
            .collect::<Result<_, _>>()?;
        let mut removed = 0u64;
        for i in 0..segment_paths.len() - 1 {
            if firsts[i + 1] <= cutoff.0 + 1 {
                self.storage.remove(&segment_paths[i])?;
                removed += 1;
            } else {
                break;
            }
        }
        if removed > 0 {
            self.storage.sync_dir(&self.config.dir)?;
        }
        Ok(removed)
    }

    /// Reseat the journal to resume appending at `next_seq`, dropping all
    /// segments below it. Used when recovery anchors on a snapshot whose
    /// seq is beyond the journal's surviving tail (a synced snapshot can
    /// outlive the journal's unsynced tail under lazy fsync): the frames
    /// between the journal tail and the snapshot are gone but the snapshot
    /// *is* the state, so we start a fresh gapless segment at `next_seq`
    /// and let the snapshot cover everything before it.
    pub fn reseat_to(&mut self, next_seq: Seq) -> Result<(), JournalError> {
        debug_assert!(next_seq.0 >= 1);
        self.sync()?;
        // Roll a new active segment starting at next_seq.
        let path = self.config.dir.join(format!("{:020}.seg", next_seq.0));
        let mut file = self.storage.create_new(&path)?;
        file.write_all(&segment_header_bytes(next_seq, self.epoch))?;
        file.sync_data()?;
        self.storage.sync_dir(&self.config.dir)?;
        self.current = Some(OpenSegment {
            file,
            size: SEGMENT_HEADER_LEN as u64,
            frames: 0,
        });
        self.last_seq = Seq(next_seq.0 - 1);
        // Every older segment is now non-active and entirely below the
        // reseat point; drop them so the journal stays gapless.
        self.compact_below(Seq(next_seq.0 - 1))?;
        Ok(())
    }

    /// The seq of the last durably-loggable frame ([`Seq::GENESIS`] if empty).
    pub fn last_seq(&self) -> Seq {
        self.last_seq
    }

    /// Number of segment files on disk — a cheap disk-pressure gauge for
    /// operators (bounded under compaction; unbounded without it).
    pub fn segment_count(&self) -> usize {
        self.storage
            .list_dir(&self.config.dir)
            .map(|paths| {
                paths
                    .iter()
                    .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("seg"))
                    .count()
            })
            .unwrap_or(0)
    }

    /// Append one frame. `frame.seq` must be exactly `last_seq + 1`.
    pub fn append(&mut self, frame: &Frame) -> Result<(), JournalError> {
        let expected = self.last_seq.next();
        if frame.seq != expected {
            return Err(JournalError::OutOfOrderAppend {
                expected,
                found: frame.seq,
            });
        }

        let encoded_len = frame.encoded_len() as u64;
        let needs_roll = match &self.current {
            None => true,
            Some(seg) => seg.frames > 0 && seg.size + encoded_len > self.config.segment_bytes,
        };
        if needs_roll {
            self.roll_segment(frame.seq)?;
        }

        // Encode into the reusable scratch buffer, not a fresh Vec.
        self.scratch.clear();
        frame.write_to(&mut self.scratch);
        let seg = self.current.as_mut().expect("segment open after roll");
        seg.file.write_all(&self.scratch)?;
        seg.size += self.scratch.len() as u64;
        seg.frames += 1;
        self.last_seq = frame.seq;
        self.dirty = true;

        self.maybe_sync()?;
        Ok(())
    }

    /// Append a run of **seq-contiguous** frames in one shot: encode them all
    /// into the reusable buffer, write with a single `write_all`, and apply
    /// the fsync policy **once** for the whole batch. This is the group-commit
    /// primitive — a caller that has accumulated concurrent ingress commits
    /// the window with one write + one `fdatasync` instead of N, which is
    /// what makes the synced-fsync budget reachable under load. `frames[0].seq`
    /// must be `last_seq + 1` and the run must be gapless. A batch that would
    /// overflow the current segment rolls once at its start (batches are far
    /// smaller than a segment).
    pub fn append_batch(&mut self, frames: &[Frame]) -> Result<(), JournalError> {
        if frames.is_empty() {
            return Ok(());
        }
        // Validate contiguity up front — either the whole batch lands or none
        // of it does (we haven't written yet).
        let mut expected = self.last_seq.next();
        for frame in frames {
            if frame.seq != expected {
                return Err(JournalError::OutOfOrderAppend {
                    expected,
                    found: frame.seq,
                });
            }
            expected = frame.seq.next();
        }

        // Encode the whole batch into the scratch buffer.
        self.scratch.clear();
        for frame in frames {
            frame.write_to(&mut self.scratch);
        }
        let batch_len = self.scratch.len() as u64;

        // Roll if the open segment can't hold the batch (or there is none).
        let needs_roll = match &self.current {
            None => true,
            Some(seg) => seg.frames > 0 && seg.size + batch_len > self.config.segment_bytes,
        };
        if needs_roll {
            self.roll_segment(frames[0].seq)?;
        }

        let seg = self.current.as_mut().expect("segment open after roll");
        seg.file.write_all(&self.scratch)?;
        seg.size += batch_len;
        seg.frames += frames.len() as u64;
        self.last_seq = frames[frames.len() - 1].seq;
        self.dirty = true;

        self.maybe_sync()?;
        Ok(())
    }

    /// Apply the configured fsync policy after a write.
    fn maybe_sync(&mut self) -> Result<(), JournalError> {
        match self.config.fsync {
            FsyncPolicy::EveryFrame => self.sync()?,
            FsyncPolicy::Micros(window) => {
                if self.last_sync.elapsed() >= Duration::from_micros(window) {
                    self.sync()?;
                }
            }
            FsyncPolicy::Never => {}
        }
        Ok(())
    }

    /// Force outstanding appends to stable storage.
    pub fn sync(&mut self) -> Result<(), JournalError> {
        if let Some(seg) = &mut self.current {
            if self.dirty {
                seg.file.sync_data()?;
                self.dirty = false;
            }
        }
        self.last_sync = Instant::now();
        Ok(())
    }

    fn roll_segment(&mut self, first_seq: Seq) -> Result<(), JournalError> {
        // Seal the outgoing segment's bytes before starting a new one.
        self.sync()?;

        let path = self.config.dir.join(format!("{:020}.seg", first_seq.0));
        let mut file = self.storage.create_new(&path)?;
        file.write_all(&segment_header_bytes(first_seq, self.epoch))?;
        file.sync_data()?;
        self.storage.sync_dir(&self.config.dir)?;

        self.current = Some(OpenSegment {
            file,
            size: SEGMENT_HEADER_LEN as u64,
            frames: 0,
        });
        Ok(())
    }
}

impl<St: Storage> Drop for Journal<St> {
    fn drop(&mut self) {
        // Best-effort: a clean shutdown should not lose the tail window
        // (fails harmlessly on simulated-crashed storage).
        let _ = self.sync();
    }
}

struct SegmentHeader {
    first_seq: Seq,
    epoch: u64,
}

fn segment_header_bytes(first_seq: Seq, epoch: u64) -> [u8; SEGMENT_HEADER_LEN] {
    let mut out = [0u8; SEGMENT_HEADER_LEN];
    out[0..4].copy_from_slice(SEGMENT_MAGIC);
    out[4..8].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    out[8..16].copy_from_slice(&first_seq.0.to_le_bytes());
    out[16..24].copy_from_slice(&epoch.to_le_bytes());
    let crc = crc32c(&out[0..24]);
    out[24..28].copy_from_slice(&crc.to_le_bytes());
    out
}

fn parse_segment_header(path: &Path, bytes: &[u8]) -> Result<SegmentHeader, JournalError> {
    let bad = |reason: &str| JournalError::BadSegmentHeader {
        segment: path.to_path_buf(),
        reason: reason.to_string(),
    };
    if bytes.len() < SEGMENT_HEADER_LEN {
        return Err(bad("file shorter than segment header"));
    }
    if &bytes[0..4] != SEGMENT_MAGIC {
        return Err(bad("bad magic"));
    }
    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if version != FORMAT_VERSION {
        return Err(bad(&format!("unsupported format version {version}")));
    }
    let stored_crc = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
    if crc32c(&bytes[0..24]) != stored_crc {
        return Err(bad("header CRC mismatch"));
    }
    Ok(SegmentHeader {
        first_seq: Seq(u64::from_le_bytes(bytes[8..16].try_into().unwrap())),
        epoch: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
    })
}

fn frame_error_reason(err: &FrameError) -> String {
    err.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Write;
    use ticktape_core::{FrameKind, Timestamp};

    fn frame(seq: u64, payload: &[u8]) -> Frame {
        Frame::new(
            Seq(seq),
            Timestamp(seq * 1000),
            1,
            FrameKind::Input,
            payload.to_vec(),
        )
    }

    fn open(dir: &Path, segment_bytes: u64) -> Recovered {
        let mut config = JournalConfig::new(dir);
        config.segment_bytes = segment_bytes;
        config.fsync = FsyncPolicy::EveryFrame;
        Journal::open(config).expect("open journal")
    }

    #[test]
    fn append_batch_is_byte_identical_to_single_appends() {
        // A batch write and N single writes must produce the same on-disk
        // journal — batching is a syscall optimization, not a format change.
        let single = tempfile::tempdir().unwrap();
        let batched = tempfile::tempdir().unwrap();
        let frames: Vec<Frame> = (1..=50u64)
            .map(|i| frame(i, format!("p-{i}").as_bytes()))
            .collect();
        {
            let mut rec = open(single.path(), 1 << 20);
            for f in &frames {
                rec.journal.append(f).unwrap();
            }
        }
        {
            let mut rec = open(batched.path(), 1 << 20);
            rec.journal.append_batch(&frames).unwrap();
            assert_eq!(rec.journal.last_seq(), Seq(50));
        }
        let a = std::fs::read(single.path().join(format!("{:020}.seg", 1))).unwrap();
        let b = std::fs::read(batched.path().join(format!("{:020}.seg", 1))).unwrap();
        assert_eq!(a, b, "batched journal bytes differ from single-append bytes");

        // And it replays to the same frames.
        let rec = open(batched.path(), 1 << 20);
        assert_eq!(rec.frames.len(), 50);
        assert_eq!(rec.frames[49].seq, Seq(50));
    }

    #[test]
    fn replay_open_streams_the_same_frames_as_open_with() {
        // Streaming recovery must yield the identical frame sequence and
        // metadata as the materializing path — it just never builds the Vec.
        let dir = tempfile::tempdir().unwrap();
        {
            let mut rec = open(dir.path(), 200); // small segments → several files
            for i in 1..=100u64 {
                rec.journal
                    .append(&frame(i, format!("payload-{i}").as_bytes()))
                    .unwrap();
            }
        }
        // Materialized reference.
        let reference = Journal::<RealStorage>::open(JournalConfig::new(dir.path())).unwrap();

        // Streaming: collect via the callback (in real use the caller would
        // apply each frame and never retain it).
        let mut streamed = Vec::new();
        let (journal, meta) = Journal::<RealStorage>::replay_open(
            JournalConfig::new(dir.path()),
            RealStorage,
            |f| streamed.push(f),
        )
        .unwrap();

        assert_eq!(streamed, reference.frames, "streamed frames differ");
        assert_eq!(meta.first_seq, reference.first_seq);
        assert_eq!(meta.last_seq, Seq(100));
        assert_eq!(journal.last_seq(), Seq(100));
    }

    #[test]
    fn append_batch_rejects_a_gap() {
        let dir = tempfile::tempdir().unwrap();
        let mut rec = open(dir.path(), 1 << 20);
        rec.journal.append(&frame(1, b"a")).unwrap();
        // Batch starting at 3 (not 2) is out of order — nothing is written.
        let bad = [frame(3, b"c"), frame(4, b"d")];
        assert!(matches!(
            rec.journal.append_batch(&bad),
            Err(JournalError::OutOfOrderAppend { .. })
        ));
        assert_eq!(rec.journal.last_seq(), Seq(1), "failed batch left no trace");
    }

    #[test]
    fn append_reopen_replay() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut rec = open(dir.path(), 1 << 20);
            assert!(rec.frames.is_empty());
            for i in 1..=100u64 {
                rec.journal
                    .append(&frame(i, format!("payload-{i}").as_bytes()))
                    .unwrap();
            }
            assert_eq!(rec.journal.last_seq(), Seq(100));
        }
        let rec = open(dir.path(), 1 << 20);
        assert_eq!(rec.frames.len(), 100);
        assert!(!rec.truncated_torn_tail);
        assert_eq!(rec.frames[0].seq, Seq(1));
        assert_eq!(rec.frames[99].seq, Seq(100));
        assert_eq!(rec.frames[41].payload, b"payload-42");
        assert_eq!(rec.journal.last_seq(), Seq(100));
    }

    #[test]
    fn rolls_segments_and_replays_across_them() {
        let dir = tempfile::tempdir().unwrap();
        {
            // Tiny segments force several rolls.
            let mut rec = open(dir.path(), 256);
            for i in 1..=50u64 {
                rec.journal.append(&frame(i, b"0123456789abcdef")).unwrap();
            }
        }
        let seg_count = std::fs::read_dir(dir.path()).unwrap().count();
        assert!(seg_count > 1, "expected multiple segments, got {seg_count}");
        let rec = open(dir.path(), 256);
        assert_eq!(rec.frames.len(), 50);
        assert_eq!(rec.journal.last_seq(), Seq(50));
    }

    #[test]
    fn torn_tail_is_truncated_to_last_intact_frame() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut rec = open(dir.path(), 1 << 20);
            for i in 1..=10u64 {
                rec.journal.append(&frame(i, b"data")).unwrap();
            }
        }
        // Tear the last frame: chop off its final 3 bytes.
        let seg_path = std::fs::read_dir(dir.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let len = std::fs::metadata(&seg_path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&seg_path).unwrap();
        file.set_len(len - 3).unwrap();

        let rec = open(dir.path(), 1 << 20);
        assert!(rec.truncated_torn_tail);
        assert_eq!(rec.frames.len(), 9, "torn 10th frame must be dropped");
        assert_eq!(rec.journal.last_seq(), Seq(9));

        // And the journal must be appendable again at seq 10.
        let mut journal = rec.journal;
        journal.append(&frame(10, b"replacement")).unwrap();
        drop(journal);
        let rec = open(dir.path(), 1 << 20);
        assert_eq!(rec.frames.len(), 10);
        assert_eq!(rec.frames[9].payload, b"replacement");
    }

    #[test]
    fn garbage_tail_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut rec = open(dir.path(), 1 << 20);
            for i in 1..=5u64 {
                rec.journal.append(&frame(i, b"data")).unwrap();
            }
        }
        let seg_path = std::fs::read_dir(dir.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let mut file = OpenOptions::new().append(true).open(&seg_path).unwrap();
        Write::write_all(&mut file, &[0xAB; 40]).unwrap(); // garbage past the last frame
        drop(file);

        let rec = open(dir.path(), 1 << 20);
        assert!(rec.truncated_torn_tail);
        assert_eq!(rec.frames.len(), 5);
    }

    #[test]
    fn out_of_order_append_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut rec = open(dir.path(), 1 << 20);
        rec.journal.append(&frame(1, b"a")).unwrap();
        let err = rec.journal.append(&frame(3, b"skip")).unwrap_err();
        assert!(matches!(err, JournalError::OutOfOrderAppend { .. }));
    }

    #[test]
    fn corruption_in_sealed_segment_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut rec = open(dir.path(), 200); // force ≥2 segments
            for i in 1..=30u64 {
                rec.journal.append(&frame(i, b"0123456789abcdef")).unwrap();
            }
        }
        let mut segs: Vec<PathBuf> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        segs.sort();
        assert!(segs.len() >= 2);
        // Flip a byte in the middle of the FIRST (sealed) segment's frames.
        let bytes = std::fs::read(&segs[0]).unwrap();
        let mut corrupted = bytes.clone();
        corrupted[SEGMENT_HEADER_LEN + 40] ^= 0xFF;
        std::fs::write(&segs[0], &corrupted).unwrap();

        let mut config = JournalConfig::new(dir.path());
        config.segment_bytes = 200;
        let err = Journal::open(config)
            .err()
            .expect("must refuse corrupt sealed segment");
        assert!(matches!(
            err,
            JournalError::Corrupt { .. } | JournalError::NonContiguousSeq { .. }
        ));
    }
}
