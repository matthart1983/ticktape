//! The snapshot store: durable, CRC-checked snapshots of service state at a
//! specific seq, enabling fast recovery (`restore(snapshot) + replay(tail)`
//! instead of replaying from genesis).
//!
//! Snapshots live alongside the journal segments as `{seq:020}.snap` files:
//!
//! ```text
//!  offset size field
//!    0     4   magic          b"TKTS"
//!    4     4   format_version u32
//!    8     8   seq            u64  the seq this snapshot captures
//!   16     8   epoch          u64
//!   24     4   payload_len    u32
//!   28     4   header_crc     u32  CRC32C of bytes [0,28)
//!   32   ...   payload             app-encoded Service::Snapshot
//!   ..     4   payload_crc    u32  CRC32C of payload
//! ```
//!
//! Snapshots are an *optimization*, never the system of record: a corrupt
//! or torn snapshot is skipped (falling back to an older one, or to full
//! replay from genesis) — it must never make recovery fail or lie. A
//! snapshot is always taken at a deterministic point (a specific seq),
//! never "now", so every replica's snapshot at seq k is byte-identical.

use crate::storage::{Storage, StorageFile};
use crate::JournalError;
use std::path::PathBuf;
use ticktape_core::crc32c::crc32c;
use ticktape_core::Seq;

const SNAPSHOT_MAGIC: &[u8; 4] = b"TKTS";
const FORMAT_VERSION: u32 = 1;
/// magic(4) + version(4) + seq(8) + epoch(8) + payload_len(4) + crc(4)
const SNAPSHOT_HEADER_LEN: usize = 32;

/// A validated snapshot loaded from disk.
pub struct LoadedSnapshot {
    pub seq: Seq,
    pub epoch: u64,
    /// App-encoded `Service::Snapshot` bytes (CRC-verified).
    pub payload: Vec<u8>,
}

/// Reads and writes `.snap` files in a directory, through [`Storage`].
pub struct SnapshotStore<St: Storage> {
    dir: PathBuf,
    storage: St,
}

impl<St: Storage> SnapshotStore<St> {
    pub fn new(dir: impl Into<PathBuf>, storage: St) -> Self {
        SnapshotStore {
            dir: dir.into(),
            storage,
        }
    }

    /// Durably write a snapshot of state at `seq`. The file is fully synced
    /// before this returns, so callers may safely advertise its existence
    /// (e.g. by appending a `SnapshotMark` frame).
    pub fn write(&self, seq: Seq, epoch: u64, payload: &[u8]) -> Result<(), JournalError> {
        let path = self.path_for(seq);
        let mut bytes = Vec::with_capacity(SNAPSHOT_HEADER_LEN + payload.len() + 4);
        bytes.extend_from_slice(SNAPSHOT_MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&seq.0.to_le_bytes());
        bytes.extend_from_slice(&epoch.to_le_bytes());
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        let header_crc = crc32c(&bytes[0..28]);
        bytes.extend_from_slice(&header_crc.to_le_bytes());
        bytes.extend_from_slice(payload);
        bytes.extend_from_slice(&crc32c(payload).to_le_bytes());

        let mut file = self.storage.create_new(&path)?;
        file.write_all(&bytes)?;
        file.sync_data()?;
        self.storage.sync_dir(&self.dir)?;
        Ok(())
    }

    /// Load the newest valid snapshot with `seq <= max_seq`. Corrupt, torn,
    /// or future-versioned snapshot files are skipped, not fatal.
    pub fn load_latest(&self, max_seq: Seq) -> Result<Option<LoadedSnapshot>, JournalError> {
        let mut paths: Vec<PathBuf> = self
            .storage
            .list_dir(&self.dir)?
            .into_iter()
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("snap"))
            .collect();
        paths.sort();
        for path in paths.iter().rev() {
            match self.storage.read(path) {
                Ok(bytes) => {
                    if let Some(snap) = parse_snapshot(&bytes) {
                        if snap.seq <= max_seq {
                            return Ok(Some(snap));
                        }
                    }
                }
                Err(_) => continue,
            }
        }
        Ok(None)
    }

    /// Delete every snapshot with `seq > max_seq`.
    ///
    /// MUST be called on recovery, before trusting any snapshot: a crash
    /// that truncates the journal below a snapshot's seq invalidates that
    /// snapshot — it captures a history that no longer exists. Left in
    /// place, it would poison a later recovery once new (different) frames
    /// re-reach its seq, and collide with the fresh snapshot written there.
    /// Purging on every open keeps the remaining set consistent: a snapshot
    /// at seq k survives only if no truncation ever went below k.
    pub fn purge_after(&self, max_seq: Seq) -> Result<(), JournalError> {
        for path in self.storage.list_dir(&self.dir)? {
            if path.extension().and_then(|e| e.to_str()) != Some("snap") {
                continue;
            }
            let stale = match path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
            {
                Some(seq) => seq > max_seq.0,
                // Unparseable name: not one of ours; leave it alone.
                None => false,
            };
            if stale {
                self.storage.remove(&path)?;
            }
        }
        self.storage.sync_dir(&self.dir)?;
        Ok(())
    }

    fn path_for(&self, seq: Seq) -> PathBuf {
        self.dir.join(format!("{:020}.snap", seq.0))
    }
}

/// Validate + parse snapshot bytes; `None` means "unusable, skip it".
fn parse_snapshot(bytes: &[u8]) -> Option<LoadedSnapshot> {
    if bytes.len() < SNAPSHOT_HEADER_LEN + 4 || &bytes[0..4] != SNAPSHOT_MAGIC {
        return None;
    }
    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if version != FORMAT_VERSION {
        return None;
    }
    let stored_header_crc = u32::from_le_bytes(bytes[28..32].try_into().unwrap());
    if crc32c(&bytes[0..28]) != stored_header_crc {
        return None;
    }
    let seq = Seq(u64::from_le_bytes(bytes[8..16].try_into().unwrap()));
    let epoch = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let payload_len = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
    let total = SNAPSHOT_HEADER_LEN + payload_len + 4;
    if bytes.len() < total {
        return None; // torn write
    }
    let payload = &bytes[SNAPSHOT_HEADER_LEN..SNAPSHOT_HEADER_LEN + payload_len];
    let stored_payload_crc = u32::from_le_bytes(bytes[total - 4..total].try_into().unwrap());
    if crc32c(payload) != stored_payload_crc {
        return None;
    }
    Some(LoadedSnapshot {
        seq,
        epoch,
        payload: payload.to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::RealStorage;
    use std::path::Path;

    fn store(dir: &Path) -> SnapshotStore<RealStorage> {
        SnapshotStore::new(dir, RealStorage)
    }

    #[test]
    fn write_and_load_latest() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path());
        store.write(Seq(10), 0, b"state-at-10").unwrap();
        store.write(Seq(20), 0, b"state-at-20").unwrap();
        store.write(Seq(30), 0, b"state-at-30").unwrap();

        let snap = store.load_latest(Seq(25)).unwrap().unwrap();
        assert_eq!(snap.seq, Seq(20));
        assert_eq!(snap.payload, b"state-at-20");

        let snap = store.load_latest(Seq(1000)).unwrap().unwrap();
        assert_eq!(snap.seq, Seq(30));

        assert!(store.load_latest(Seq(5)).unwrap().is_none());
    }

    #[test]
    fn corrupt_snapshot_is_skipped_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path());
        store.write(Seq(10), 0, b"good-old").unwrap();
        store.write(Seq(20), 0, b"soon-corrupt").unwrap();

        // Flip a payload byte in the newest snapshot.
        let path = dir.path().join(format!("{:020}.snap", 20));
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[SNAPSHOT_HEADER_LEN + 2] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let snap = store.load_latest(Seq(100)).unwrap().unwrap();
        assert_eq!(snap.seq, Seq(10), "must fall back to the older snapshot");
        assert_eq!(snap.payload, b"good-old");
    }

    #[test]
    fn torn_snapshot_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path());
        store.write(Seq(10), 0, b"whole").unwrap();
        let path = dir.path().join(format!("{:020}.snap", 10));
        let len = std::fs::metadata(&path).unwrap().len();
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 2).unwrap();
        assert!(store.load_latest(Seq(100)).unwrap().is_none());
    }
}
