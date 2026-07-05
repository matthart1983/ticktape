//! An `io_uring`-backed [`Storage`] for the journal write path (P2 #3).
//!
//! Linux-only, feature-gated (`io-uring`), and off by default. The journal's
//! hot path is *append + fdatasync*; this routes both through an `io_uring`
//! instance per open segment so the write and the durability barrier are
//! kernel-submitted without a `write(2)`/`fdatasync(2)` syscall pair each.
//! Combined with [`crate::Journal::append_batch`] (one append per commit
//! window), a batch becomes a single submitted write + a single submitted
//! `fdatasync` — the shape that reaches the synced-latency budget on NVMe.
//!
//! Cold-path operations (directory listing, whole-segment reads on recovery,
//! create/truncate/remove) stay on `std::fs`: they are one-time and not worth
//! the ring. Only the per-append write and the per-commit fsync — the paths
//! that run millions of times — use the ring.
//!
//! **Verification note:** this compiles and runs only on Linux with the
//! `io-uring` feature. It is exercised by `tests/iouring.rs` (also gated), to
//! be run on a Linux host.

use crate::storage::{Storage, StorageFile};
use io_uring::{opcode, types, IoUring};
use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

/// A [`Storage`] whose open files submit their appends and data-syncs through
/// `io_uring`.
#[derive(Clone, Default)]
pub struct IoUringStorage;

impl IoUringStorage {
    pub fn new() -> Self {
        IoUringStorage
    }
}

/// An open journal segment with its own small submission ring and an explicit
/// write offset (io_uring writes are positioned, not append-mode).
pub struct IoUringFile {
    file: File,
    ring: IoUring,
    offset: u64,
}

impl IoUringFile {
    fn open(file: File, offset: u64) -> io::Result<Self> {
        // A depth of 4 is plenty: the journal submits one op and waits.
        let ring = IoUring::new(4)?;
        Ok(IoUringFile { file, ring, offset })
    }

    /// Submit one op, wait for its completion, and return the raw result
    /// (>= 0 on success, `-errno` on failure).
    fn submit_wait(&mut self, entry: &io_uring::squeue::Entry) -> io::Result<i32> {
        // SAFETY: the buffers referenced by `entry` outlive the wait below
        // (callers hold them across `submit_wait`), and the ring has room.
        unsafe {
            self.ring
                .submission()
                .push(entry)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "io_uring SQ full"))?;
        }
        self.ring.submit_and_wait(1)?;
        let cqe = self
            .ring
            .completion()
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "io_uring: no completion"))?;
        Ok(cqe.result())
    }
}

impl StorageFile for IoUringFile {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        let mut written = 0usize;
        while written < buf.len() {
            let chunk = &buf[written..];
            let fd = types::Fd(self.file.as_raw_fd());
            let entry = opcode::Write::new(fd, chunk.as_ptr(), chunk.len() as u32)
                .offset(self.offset + written as u64)
                .build()
                .user_data(0);
            let n = self.submit_wait(&entry)?;
            if n < 0 {
                return Err(io::Error::from_raw_os_error(-n));
            }
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "io_uring wrote 0"));
            }
            written += n as usize;
        }
        self.offset += buf.len() as u64;
        Ok(())
    }

    fn sync_data(&mut self) -> io::Result<()> {
        let fd = types::Fd(self.file.as_raw_fd());
        let entry = opcode::Fsync::new(fd)
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .user_data(1);
        let n = self.submit_wait(&entry)?;
        if n < 0 {
            return Err(io::Error::from_raw_os_error(-n));
        }
        Ok(())
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.offset.max(self.file.metadata()?.len()))
    }
}

impl Storage for IoUringStorage {
    type File = IoUringFile;

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

    fn open_append(&self, path: &Path) -> io::Result<IoUringFile> {
        // Positioned writes need the current end as the starting offset.
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let offset = file.metadata()?.len();
        IoUringFile::open(file, offset)
    }

    fn create_new(&self, path: &Path) -> io::Result<IoUringFile> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        IoUringFile::open(file, 0)
    }

    fn truncate(&self, path: &Path, len: u64) -> io::Result<()> {
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(len)?;
        file.sync_all()
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        File::open(dir)?.sync_all()
    }
}
