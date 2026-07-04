//! A follower: consumes the in-order sequenced stream into its own copy of
//! the service — redundancy through determinism. Ship the ordered inputs,
//! recompute identical state; a promoted standby *is* the service.

use std::fmt;
use ticktape_core::{decode_all, Ctx, Frame, FrameKind, OutBuf, Seq, Service};

#[derive(Debug)]
pub enum ReplicaError {
    /// The stream handed us a frame out of order — the transport must
    /// deliver gapless; this is a bug upstream, never something to paper
    /// over silently.
    OutOfOrder { expected: Seq, got: Seq },
    /// An input frame no longer decodes as `S::Input`.
    CorruptInput {
        seq: Seq,
        err: ticktape_core::CodecError,
    },
}

impl fmt::Display for ReplicaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplicaError::OutOfOrder { expected, got } => {
                write!(f, "out-of-order frame: expected seq {expected}, got {got}")
            }
            ReplicaError::CorruptInput { seq, err } => {
                write!(f, "input at seq {seq} failed to decode: {err}")
            }
        }
    }
}

impl std::error::Error for ReplicaError {}

/// A deterministic follower of the sequenced stream.
pub struct Replica<S: Service> {
    service: S,
    seq: Seq,
    outputs: OutBuf<S::Output>,
}

impl<S: Service> Replica<S> {
    pub fn new(config: &S::Config) -> Self {
        Replica {
            service: S::genesis(config),
            seq: Seq::GENESIS,
            outputs: OutBuf::new(),
        }
    }

    /// Resume from a snapshot taken at `seq` (feed the stream from
    /// `seq + 1`).
    pub fn from_snapshot(snapshot: S::Snapshot, seq: Seq, config: &S::Config) -> Self {
        Replica {
            service: S::restore(snapshot, config),
            seq,
            outputs: OutBuf::new(),
        }
    }

    /// Apply one in-order frame. Input frames step the state machine and
    /// return its outputs; control frames (ticks, marks, heartbeats) just
    /// advance the seq.
    pub fn apply(&mut self, frame: &Frame) -> Result<Vec<S::Output>, ReplicaError> {
        let expected = self.seq.next();
        if frame.seq != expected {
            return Err(ReplicaError::OutOfOrder {
                expected,
                got: frame.seq,
            });
        }
        self.seq = frame.seq;
        if frame.kind != FrameKind::Input {
            return Ok(Vec::new());
        }
        let input: S::Input =
            decode_all(&frame.payload).map_err(|err| ReplicaError::CorruptInput {
                seq: frame.seq,
                err,
            })?;
        let mut ctx = Ctx::new(frame.seq, frame.timestamp, &mut self.outputs);
        self.service.apply(frame.seq, &input, &mut ctx);
        Ok(self.outputs.drain())
    }

    /// The seq of the last applied frame.
    pub fn seq(&self) -> Seq {
        self.seq
    }

    pub fn service(&self) -> &S {
        &self.service
    }
}
