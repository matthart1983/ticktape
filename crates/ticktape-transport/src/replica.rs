//! A follower: consumes the in-order sequenced stream into its own copy of
//! the service — redundancy through determinism. Ship the ordered inputs,
//! recompute identical state; a promoted standby *is* the service.

use std::fmt;
use ticktape_core::{decode_all, Ctx, Frame, FrameKind, OutBuf, Seq, Service, TimerReq};

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
    /// Discarded scratch for timer requests: a follower mirrors the leader's
    /// journaled `TimerFired` frames rather than running its own scheduler,
    /// so scheduling requests made during replay have no effect here.
    timer_ops: Vec<TimerReq>,
}

impl<S: Service> Replica<S> {
    pub fn new(config: &S::Config) -> Self {
        Replica {
            service: S::genesis(config),
            seq: Seq::GENESIS,
            outputs: OutBuf::new(),
            timer_ops: Vec::new(),
        }
    }

    /// Resume from a snapshot taken at `seq` (feed the stream from
    /// `seq + 1`).
    pub fn from_snapshot(snapshot: S::Snapshot, seq: Seq, config: &S::Config) -> Self {
        Replica {
            service: S::restore(snapshot, config),
            seq,
            outputs: OutBuf::new(),
            timer_ops: Vec::new(),
        }
    }

    /// Apply one in-order frame. Input frames step the state machine; a
    /// `TimerFired` frame delivers the leader's deterministic timer firing to
    /// `on_timer` (identical state on every replica); other control frames
    /// (ticks, marks, heartbeats) just advance the seq. Returns any outputs.
    pub fn apply(&mut self, frame: &Frame) -> Result<Vec<S::Output>, ReplicaError> {
        let expected = self.seq.next();
        if frame.seq != expected {
            return Err(ReplicaError::OutOfOrder {
                expected,
                got: frame.seq,
            });
        }
        self.seq = frame.seq;
        self.timer_ops.clear();
        match frame.kind {
            FrameKind::Input => {
                let input: S::Input =
                    decode_all(&frame.payload).map_err(|err| ReplicaError::CorruptInput {
                        seq: frame.seq,
                        err,
                    })?;
                let mut ctx = Ctx::new(
                    frame.seq,
                    frame.timestamp,
                    &mut self.outputs,
                    &mut self.timer_ops,
                );
                self.service.apply(frame.seq, &input, &mut ctx);
                Ok(self.outputs.drain())
            }
            FrameKind::TimerFired => {
                let id: u64 =
                    decode_all(&frame.payload).map_err(|err| ReplicaError::CorruptInput {
                        seq: frame.seq,
                        err,
                    })?;
                let mut ctx = Ctx::new(
                    frame.seq,
                    frame.timestamp,
                    &mut self.outputs,
                    &mut self.timer_ops,
                );
                self.service.on_timer(id, &mut ctx);
                Ok(self.outputs.drain())
            }
            _ => Ok(Vec::new()),
        }
    }

    /// The seq of the last applied frame.
    pub fn seq(&self) -> Seq {
        self.seq
    }

    pub fn service(&self) -> &S {
        &self.service
    }
}
