//! In-memory storage with seeded fault injection — the simulated disk.
//!
//! Models the durability semantics that matter for crash testing:
//!
//! - Writes land in an **unsynced** buffer; `sync_data` moves them to the
//!   **synced** region (readers see both — page-cache semantics).
//! - [`SimStorage::crash`] simulates power loss: for every file, a seeded
//!   *prefix* of the unsynced bytes survives (the OS may have flushed some
//!   of the tail, in order) and the rest vanishes; optionally a surviving
//!   tail byte is bit-flipped (a torn sector). What survives becomes the
//!   new on-disk truth.
//! - Crashing bumps an **epoch**; file handles opened before the crash fail
//!   every subsequent operation, so a "dead" process's buffered writes and
//!   drop-time syncs cannot leak into the post-crash world.

use crate::rng::Rng;
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use ticktape_journal::{Storage, StorageFile};

#[derive(Default)]
struct FileData {
    synced: Vec<u8>,
    unsynced: Vec<u8>,
}

impl FileData {
    fn combined(&self) -> Vec<u8> {
        let mut out = self.synced.clone();
        out.extend_from_slice(&self.unsynced);
        out
    }

    fn len(&self) -> u64 {
        (self.synced.len() + self.unsynced.len()) as u64
    }
}

#[derive(Default)]
struct Inner {
    // BTreeMap, not HashMap: directory listings must be deterministic.
    files: BTreeMap<PathBuf, FileData>,
    epoch: u64,
}

/// A cloneable handle to one simulated disk. Clones share state, so the
/// simulator keeps a handle across simulated process crashes/restarts.
#[derive(Clone, Default)]
pub struct SimStorage {
    inner: Arc<Mutex<Inner>>,
}

impl SimStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Simulate power loss. Every file keeps its synced bytes plus a seeded
    /// prefix of its unsynced bytes; with `torn`, one surviving unsynced
    /// byte may be bit-flipped. Survivors become durable. All pre-crash
    /// file handles are fenced off (epoch bump) and fail from now on.
    pub fn crash(&self, rng: &mut Rng, torn: bool) {
        let mut inner = self.inner.lock().unwrap();
        inner.epoch += 1;
        for data in inner.files.values_mut() {
            let keep = rng.below(data.unsynced.len() as u64 + 1) as usize;
            data.unsynced.truncate(keep);
            if torn && keep > 0 && rng.chance(1, 4) {
                let idx = rng.below(keep as u64) as usize;
                let bit = rng.below(8) as u32;
                data.unsynced[idx] ^= 1 << bit;
            }
            // The survivors are what's physically on disk now.
            let survived = std::mem::take(&mut data.unsynced);
            data.synced.extend_from_slice(&survived);
        }
    }

    /// Flip one bit inside the *synced* region of the given file — silent
    /// media corruption (bit rot). The journal must detect this and refuse
    /// to serve corrupt data, never return it silently.
    pub fn rot_synced_byte(&self, path: &Path, offset: usize, bit: u32) {
        let mut inner = self.inner.lock().unwrap();
        let data = inner.files.get_mut(path).expect("rot target exists");
        data.synced[offset] ^= 1 << (bit % 8);
    }

    /// Paths of all files, sorted (for test assertions / rot targeting).
    pub fn file_paths(&self) -> Vec<PathBuf> {
        self.inner.lock().unwrap().files.keys().cloned().collect()
    }

    fn stale() -> io::Error {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "file handle from before a simulated crash",
        )
    }
}

/// A handle into [`SimStorage`], fenced by the epoch it was opened in.
pub struct SimFile {
    storage: SimStorage,
    path: PathBuf,
    epoch: u64,
}

impl SimFile {
    fn with_data<R>(&self, f: impl FnOnce(&mut FileData) -> R) -> io::Result<R> {
        let mut inner = self.storage.inner.lock().unwrap();
        if inner.epoch != self.epoch {
            return Err(SimStorage::stale());
        }
        let data = inner
            .files
            .get_mut(&self.path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file removed"))?;
        Ok(f(data))
    }
}

impl StorageFile for SimFile {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.with_data(|data| data.unsynced.extend_from_slice(buf))
    }

    fn sync_data(&mut self) -> io::Result<()> {
        self.with_data(|data| {
            let flushed = std::mem::take(&mut data.unsynced);
            data.synced.extend_from_slice(&flushed);
        })
    }

    fn len(&self) -> io::Result<u64> {
        self.with_data(|data| data.len())
    }
}

impl Storage for SimStorage {
    type File = SimFile;

    fn create_dir_all(&self, _dir: &Path) -> io::Result<()> {
        Ok(())
    }

    fn list_dir(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .files
            .keys()
            .filter(|p| p.parent() == Some(dir))
            .cloned()
            .collect())
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        let inner = self.inner.lock().unwrap();
        inner
            .files
            .get(path)
            .map(FileData::combined)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))
    }

    fn open_append(&self, path: &Path) -> io::Result<SimFile> {
        let inner = self.inner.lock().unwrap();
        if !inner.files.contains_key(path) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "no such file"));
        }
        Ok(SimFile {
            storage: self.clone(),
            path: path.to_path_buf(),
            epoch: inner.epoch,
        })
    }

    fn create_new(&self, path: &Path) -> io::Result<SimFile> {
        let mut inner = self.inner.lock().unwrap();
        if inner.files.contains_key(path) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "file exists"));
        }
        inner.files.insert(path.to_path_buf(), FileData::default());
        let epoch = inner.epoch;
        drop(inner);
        Ok(SimFile {
            storage: self.clone(),
            path: path.to_path_buf(),
            epoch,
        })
    }

    fn truncate(&self, path: &Path, len: u64) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let data = inner
            .files
            .get_mut(path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))?;
        // set_len + fsync semantics: the truncated content is durable.
        let mut combined = data.combined();
        combined.truncate(len as usize);
        data.synced = combined;
        data.unsynced.clear();
        Ok(())
    }

    fn sync_dir(&self, _dir: &Path) -> io::Result<()> {
        // Simplification: file creation is modeled as immediately durable
        // (directory-entry loss on crash is not simulated yet).
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsynced_tail_can_be_lost_on_crash() {
        let storage = SimStorage::new();
        let path = Path::new("/j/seg");
        let mut file = storage.create_new(path).unwrap();
        file.write_all(b"synced").unwrap();
        file.sync_data().unwrap();
        file.write_all(b"-unsynced").unwrap();

        // Seed chosen arbitrarily; whatever survives must be a prefix.
        let mut rng = Rng::new(7);
        storage.crash(&mut rng, false);

        let contents = storage.read(path).unwrap();
        assert!(contents.starts_with(b"synced"));
        assert!(contents.len() <= b"synced-unsynced".len());

        // The old handle is fenced.
        assert!(file.write_all(b"zombie").is_err());
        assert!(file.sync_data().is_err());
        assert_eq!(storage.read(path).unwrap(), contents);
    }

    #[test]
    fn synced_bytes_always_survive() {
        let storage = SimStorage::new();
        let path = Path::new("/j/seg");
        let mut file = storage.create_new(path).unwrap();
        file.write_all(b"durable").unwrap();
        file.sync_data().unwrap();
        for seed in 0..20 {
            storage.crash(&mut Rng::new(seed), true);
            assert!(storage.read(path).unwrap().starts_with(b"durable"));
        }
    }

    #[test]
    fn truncate_discards_tail_durably() {
        let storage = SimStorage::new();
        let path = Path::new("/j/seg");
        let mut file = storage.create_new(path).unwrap();
        file.write_all(b"keep-me-drop-me").unwrap();
        storage.truncate(path, 7).unwrap();
        assert_eq!(storage.read(path).unwrap(), b"keep-me");
        storage.crash(&mut Rng::new(1), false);
        assert_eq!(storage.read(path).unwrap(), b"keep-me");
    }
}
