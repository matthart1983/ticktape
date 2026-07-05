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
    /// The stream's leader declared an input-schema version (in its
    /// `EpochChange` fence) that differs from this replica's
    /// `S::SCHEMA_VERSION` — a version skew caught explicitly rather than as a
    /// later decode failure. `theirs` is the leader's, `ours` is this build's.
    SchemaMismatch { theirs: u32, ours: u32, seq: Seq },
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
            ReplicaError::SchemaMismatch { theirs, ours, seq } => write!(
                f,
                "schema version mismatch at fence seq {seq}: leader {theirs}, this replica {ours}"
            ),
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
            FrameKind::EpochChange => {
                // The fence carries the leader's schema version (payload =
                // `(epoch, first_seq, schema_version)`, tolerant of the older
                // 2-field form). Reject a stream whose leader's schema differs
                // from ours, explicitly, rather than mis-decoding later inputs.
                if frame.payload.len() >= 20 {
                    let (_epoch, _first, theirs): (u64, u64, u32) = decode_all(&frame.payload)
                        .map_err(|err| ReplicaError::CorruptInput {
                            seq: frame.seq,
                            err,
                        })?;
                    if theirs != S::SCHEMA_VERSION {
                        return Err(ReplicaError::SchemaMismatch {
                            theirs,
                            ours: S::SCHEMA_VERSION,
                            seq: frame.seq,
                        });
                    }
                }
                Ok(Vec::new())
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

#[cfg(test)]
mod tests {
    use super::*;
    use ticktape_core::{encode_to_vec, Ctx, FrameKind, Timestamp};

    /// A service pinned to schema version 5.
    struct V5;
    impl Service for V5 {
        type Input = u32;
        type Output = ();
        type Snapshot = ();
        type Config = ();
        const SCHEMA_VERSION: u32 = 5;
        fn genesis(_: &()) -> Self {
            V5
        }
        fn apply(&mut self, _: Seq, _: &u32, _: &mut Ctx<'_, ()>) {}
        fn snapshot(&self) {}
        fn restore(_: (), _: &()) -> Self {
            V5
        }
    }

    fn fence(first_seq: u64, schema: u32) -> Frame {
        Frame::new(
            Seq(first_seq),
            Timestamp(1),
            1,
            FrameKind::EpochChange,
            encode_to_vec(&(9u64, first_seq, schema)),
        )
    }

    #[test]
    fn matching_schema_fence_is_accepted() {
        let mut replica: Replica<V5> = Replica::new(&());
        assert!(replica.apply(&fence(1, 5)).is_ok());
        assert_eq!(replica.seq(), Seq(1));
    }

    #[test]
    fn mismatched_schema_fence_is_rejected() {
        let mut replica: Replica<V5> = Replica::new(&());
        // The leader stamped schema 6; this replica is 5 → explicit rejection.
        match replica.apply(&fence(1, 6)) {
            Err(ReplicaError::SchemaMismatch { theirs, ours, seq }) => {
                assert_eq!((theirs, ours, seq), (6, 5, Seq(1)));
            }
            other => panic!("expected SchemaMismatch, got {other:?}"),
        }
    }

    #[test]
    fn legacy_two_field_fence_is_tolerated() {
        // A fence with no schema field (16-byte payload) is not checked —
        // treated as "schema unknown", so old streams still flow.
        let legacy = Frame::new(
            Seq(1),
            Timestamp(1),
            1,
            FrameKind::EpochChange,
            encode_to_vec(&(9u64, 1u64)),
        );
        let mut replica: Replica<V5> = Replica::new(&());
        assert!(replica.apply(&legacy).is_ok());
    }
}
