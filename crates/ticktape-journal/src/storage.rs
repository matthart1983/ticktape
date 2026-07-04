//! The storage abstraction the journal writes through.
//!
//! Everything the journal needs from a filesystem is behind [`Storage`], so
//! the deterministic simulator (`ticktape-sim`) can substitute an in-memory
//! implementation with seeded fault injection — crashes that lose the
//! unsynced tail, torn writes, bit rot — while production uses
//! [`RealStorage`] (std::fs).

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// An open, appendable file.
// `len` returns io::Result, so clippy's is_empty pairing doesn't apply.
#[allow(clippy::len_without_is_empty)]
pub trait StorageFile {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()>;
    /// Persist written data to stable storage (fdatasync semantics).
    fn sync_data(&mut self) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
}

/// A minimal filesystem: exactly the operations journal recovery and
/// appending require.
pub trait Storage {
    type File: StorageFile;

    fn create_dir_all(&self, dir: &Path) -> io::Result<()>;
    /// All file paths directly inside `dir` (order unspecified; the journal
    /// sorts).
    fn list_dir(&self, dir: &Path) -> io::Result<Vec<PathBuf>>;
    /// Read a whole file. Sees unsynced writes (page-cache semantics).
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;
    /// Open an existing file for appending.
    fn open_append(&self, path: &Path) -> io::Result<Self::File>;
    /// Create a new file for appending; error if it already exists.
    fn create_new(&self, path: &Path) -> io::Result<Self::File>;
    /// Truncate a file to `len` and durably persist the new length
    /// (set_len + fsync semantics). Used to discard a torn tail.
    fn truncate(&self, path: &Path, len: u64) -> io::Result<()>;
    /// Durably persist directory entries (fsync-the-directory semantics)
    /// after creating a file. Platforms without this are a no-op.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
}

/// `std::fs`-backed storage: what production runs on.
#[derive(Debug, Clone, Copy, Default)]
pub struct RealStorage;

impl StorageFile for File {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        Write::write_all(self, buf)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        File::sync_data(self)
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }
}

impl Storage for RealStorage {
    type File = File;

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(dir)
    }

    fn list_dir(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        std::fs::read_dir(dir)?
            .map(|entry| Ok(entry?.path()))
            .collect()
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        File::open(path)?.read_to_end(&mut buf)?;
        Ok(buf)
    }

    fn open_append(&self, path: &Path) -> io::Result<File> {
        OpenOptions::new().append(true).open(path)
    }

    fn create_new(&self, path: &Path) -> io::Result<File> {
        OpenOptions::new().create_new(true).append(true).open(path)
    }

    fn truncate(&self, path: &Path, len: u64) -> io::Result<()> {
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(len)?;
        file.sync_all()
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        // Windows cannot fsync a directory handle; accept the weaker
        // guarantee there.
        #[cfg(unix)]
        {
            File::open(dir)?.sync_all()?;
        }
        #[cfg(not(unix))]
        {
            let _ = dir;
        }
        Ok(())
    }
}
