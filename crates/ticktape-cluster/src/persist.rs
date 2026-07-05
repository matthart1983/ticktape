//! Durable acceptor state — the fix for the documented double-election
//! hazard.
//!
//! An [`Acceptor`](crate::Acceptor) that forgets its highest promised epoch
//! across a restart can grant the same epoch to two candidates, electing
//! two leaders — the one way the election safety argument breaks. The
//! Paxos/VSR requirement is that `promised` be written to stable storage
//! **before** a grant is returned. [`PersistentAcceptor`] enforces exactly
//! that: it persists the new `promised` (fsync'd) before wrapping the
//! in-memory acceptor's grant, so a crash after the durable write but
//! before the reply is safe (the promise is remembered), and a crash
//! before the durable write means the grant never left the building.
//!
//! State is a tiny CRC-checked file written through the [`Storage`] trait,
//! so the deterministic simulator can crash and restart acceptors and prove
//! no epoch is ever granted twice (see the cluster sim's acceptor-crash
//! fault).

use crate::election::{Acceptor, VoteReply, VoteRequest};
use std::fmt;
use std::path::PathBuf;
use ticktape_core::crc32c::crc32c;
use ticktape_core::Seq;
use ticktape_journal::{Storage, StorageFile};

const MAGIC: &[u8; 4] = b"TKAP"; // ticktape acceptor persistence
const RECORD_LEN: usize = 4 + 8 + 4; // magic + promised(u64) + crc

#[derive(Debug)]
pub enum PersistError {
    Io(std::io::Error),
    /// The state file exists but is malformed/corrupt.
    Corrupt(&'static str),
}

impl fmt::Display for PersistError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PersistError::Io(e) => write!(f, "acceptor persistence I/O error: {e}"),
            PersistError::Corrupt(what) => write!(f, "corrupt acceptor state: {what}"),
        }
    }
}

impl std::error::Error for PersistError {}

impl From<std::io::Error> for PersistError {
    fn from(e: std::io::Error) -> Self {
        PersistError::Io(e)
    }
}

/// An [`Acceptor`] whose `promised` epoch survives crashes.
///
/// `high_water` is intentionally **not** persisted here — it is a property
/// of the co-located replica's journal, recovered from the journal on
/// restart and re-established via [`Self::observe_seq`] before this acceptor
/// answers any vote. Only `promised` needs its own durable record.
pub struct PersistentAcceptor<St: Storage> {
    inner: Acceptor,
    storage: St,
    path: PathBuf,
}

impl<St: Storage> PersistentAcceptor<St> {
    /// Open the acceptor at `path`, recovering `promised` if a prior record
    /// exists (a corrupt record is a hard error — silently resetting
    /// `promised` to 0 is the exact failure this type prevents).
    pub fn open(path: impl Into<PathBuf>, storage: St) -> Result<Self, PersistError> {
        let path = path.into();
        let mut inner = Acceptor::new();
        match storage.read(&path) {
            Ok(bytes) => {
                let promised = parse_record(&bytes)?;
                // Replay promises up to the recovered value without emitting
                // grants — brings the in-memory acceptor to the durable
                // epoch. `handle` only raises `promised`, so a single
                // request at `promised` sets it exactly.
                if promised > 0 {
                    let _ = inner.handle(VoteRequest { epoch: promised });
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Ok(PersistentAcceptor {
            inner,
            storage,
            path,
        })
    }

    /// See [`Acceptor::observe_seq`].
    pub fn observe_seq(&mut self, seq: Seq) {
        self.inner.observe_seq(seq);
    }

    /// See [`Acceptor::reset_high_water`].
    pub fn reset_high_water(&mut self, seq: Seq) {
        self.inner.reset_high_water(seq);
    }

    pub fn promised(&self) -> u64 {
        self.inner.promised()
    }

    /// Handle a vote request, persisting a raised `promised` **before**
    /// returning the grant. A durable-write failure surfaces as an error
    /// rather than an ungrounded grant.
    pub fn handle(&mut self, request: VoteRequest) -> Result<VoteReply, PersistError> {
        let before = self.inner.promised();
        let reply = self.inner.handle(request);
        let after = self.inner.promised();
        if after > before {
            // Promise raised: it MUST be durable before the grant is used.
            self.persist(after)?;
        }
        Ok(reply)
    }

    fn persist(&self, promised: u64) -> Result<(), PersistError> {
        let mut record = Vec::with_capacity(RECORD_LEN);
        record.extend_from_slice(MAGIC);
        record.extend_from_slice(&promised.to_le_bytes());
        record.extend_from_slice(&crc32c(&record[0..12]).to_le_bytes());
        // Write in place and fsync. The Storage trait has no atomic rename,
        // so a crash mid-rewrite can leave a torn/empty file — recovery then
        // treats it as corrupt (a HARD error), which bricks this one
        // acceptor. That is deliberately safe-over-available: a bricked
        // acceptor is a lost vote (the deployment tolerates a minority of
        // those), never a *forgotten* promise (which would let two leaders
        // be elected). A crash-safe two-slot A/B scheme could keep the
        // acceptor available across torn writes; deferred, since safety —
        // not availability of one voter — is the property that must hold.
        let mut file = self.storage.create_new(&self.path).or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                self.storage.truncate(&self.path, 0)?;
                self.storage.open_append(&self.path)
            } else {
                Err(e)
            }
        })?;
        file.write_all(&record)?;
        file.sync_data()?;
        Ok(())
    }
}

fn parse_record(bytes: &[u8]) -> Result<u64, PersistError> {
    if bytes.len() != RECORD_LEN {
        return Err(PersistError::Corrupt("wrong length"));
    }
    if &bytes[0..4] != MAGIC {
        return Err(PersistError::Corrupt("bad magic"));
    }
    let stored_crc = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
    if crc32c(&bytes[0..12]) != stored_crc {
        return Err(PersistError::Corrupt("crc mismatch"));
    }
    Ok(u64::from_le_bytes(bytes[4..12].try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ticktape_journal::RealStorage;

    #[test]
    fn promised_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("acceptor.state");
        {
            let mut acc = PersistentAcceptor::open(&path, RealStorage).unwrap();
            assert!(matches!(
                acc.handle(VoteRequest { epoch: 5 }).unwrap(),
                VoteReply::Grant { epoch: 5, .. }
            ));
        }
        // Reopen: the promise to 5 must be remembered.
        let mut acc = PersistentAcceptor::open(&path, RealStorage).unwrap();
        assert_eq!(acc.promised(), 5);
        // Granting epoch 5 again would be a double-election — must reject.
        assert!(matches!(
            acc.handle(VoteRequest { epoch: 5 }).unwrap(),
            VoteReply::Reject { promised: 5 }
        ));
        // A higher epoch still wins and re-persists.
        assert!(matches!(
            acc.handle(VoteRequest { epoch: 6 }).unwrap(),
            VoteReply::Grant { epoch: 6, .. }
        ));
    }

    #[test]
    fn corrupt_state_is_a_hard_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("acceptor.state");
        {
            let mut acc = PersistentAcceptor::open(&path, RealStorage).unwrap();
            acc.handle(VoteRequest { epoch: 3 }).unwrap();
        }
        // Corrupt the record.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[5] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        // Recovery must refuse rather than silently reset promised to 0.
        assert!(PersistentAcceptor::open(&path, RealStorage).is_err());
    }

    #[test]
    fn fresh_acceptor_starts_at_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("acceptor.state");
        let acc = PersistentAcceptor::open(&path, RealStorage).unwrap();
        assert_eq!(acc.promised(), 0);
    }
}
