//! io_uring journal backend (P2 #3) — Linux + `io-uring` feature only.
//!
//! Run on a Linux host:  `cargo test -p ticktape-journal --features io-uring`
//!
//! Proves the io_uring-backed `Storage` produces a journal byte-identical to
//! the `RealStorage` one and replays to the same frames — i.e. it is a pure
//! write-path acceleration, not a format change.
#![cfg(all(target_os = "linux", feature = "io-uring"))]

use ticktape_core::{Frame, FrameKind, Seq, Timestamp};
use ticktape_journal::{FsyncPolicy, IoUringStorage, Journal, JournalConfig, RealStorage};

fn frame(seq: u64) -> Frame {
    Frame::new(
        Seq(seq),
        Timestamp(seq * 1000),
        1,
        FrameKind::Input,
        format!("payload-{seq}").into_bytes(),
    )
}

fn config(dir: &std::path::Path) -> JournalConfig {
    let mut c = JournalConfig::new(dir);
    c.fsync = FsyncPolicy::EveryFrame;
    c
}

#[test]
fn iouring_journal_is_byte_identical_and_replays() {
    let real_dir = tempfile::tempdir().unwrap();
    let uring_dir = tempfile::tempdir().unwrap();
    let frames: Vec<Frame> = (1..=200u64).map(frame).collect();

    // Write the same stream through RealStorage and IoUringStorage.
    {
        let mut j = Journal::open_with(config(real_dir.path()), RealStorage)
            .unwrap()
            .journal;
        for f in &frames {
            j.append(f).unwrap();
        }
        j.sync().unwrap();
    }
    {
        let mut j = Journal::open_with(config(uring_dir.path()), IoUringStorage::new())
            .unwrap()
            .journal;
        // Exercise both the single and the batched (group-commit) paths.
        for f in &frames[..100] {
            j.append(f).unwrap();
        }
        j.append_batch(&frames[100..]).unwrap();
        j.sync().unwrap();
    }

    let seg = format!("{:020}.seg", 1);
    assert_eq!(
        std::fs::read(real_dir.path().join(&seg)).unwrap(),
        std::fs::read(uring_dir.path().join(&seg)).unwrap(),
        "io_uring journal bytes differ from RealStorage",
    );

    // And it recovers to the identical frame sequence.
    let recovered = Journal::open_with(config(uring_dir.path()), IoUringStorage::new()).unwrap();
    assert_eq!(recovered.frames.len(), 200);
    assert_eq!(recovered.frames[199].seq, Seq(200));
    assert_eq!(recovered.journal.last_seq(), Seq(200));
}
