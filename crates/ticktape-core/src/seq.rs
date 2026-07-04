//! Sequence numbers and sequenced time.

use core::fmt;

/// A global, strictly monotonic, gapless sequence number assigned by the
/// sequencer. `Seq(0)` is the genesis sentinel: no frame carries it; a
/// service at `Seq::GENESIS` has applied no inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Seq(pub u64);

impl Seq {
    /// The genesis sentinel (no inputs applied).
    pub const GENESIS: Seq = Seq(0);

    /// The next sequence number.
    #[must_use]
    pub fn next(self) -> Seq {
        Seq(self.0 + 1)
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Seq {
    fn from(v: u64) -> Self {
        Seq(v)
    }
}

/// Sequencer-assigned time: nanoseconds since the Unix epoch.
///
/// This is the ONLY time a [`crate::Service`] can observe. It enters the
/// system stamped onto sequenced frames, so every replica sees the same
/// time at the same [`Seq`] — live or on replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub const ZERO: Timestamp = Timestamp(0);

    pub fn as_nanos(self) -> u64 {
        self.0
    }

    pub fn from_nanos(nanos: u64) -> Self {
        Timestamp(nanos)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ns", self.0)
    }
}
