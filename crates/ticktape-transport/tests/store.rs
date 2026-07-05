//! Repeater/rewinder split: the bounded in-memory window ages frames out,
//! the journal-backed rewinder serves them, and ChainStore composes both.

use ticktape_core::{Frame, FrameKind, Seq, Timestamp};
use ticktape_journal::{FsyncPolicy, Journal, JournalConfig};
use ticktape_transport::{ChainStore, FrameStore, JournalRewinder, MemStore};

fn frame(seq: u64) -> Frame {
    Frame::new(
        Seq(seq),
        Timestamp(seq),
        1,
        FrameKind::Input,
        seq.to_le_bytes().to_vec(),
    )
}

#[test]
fn repeater_is_bounded_and_ages_out() {
    let store = MemStore::with_capacity(10);
    for seq in 1..=100 {
        store.record(frame(seq));
    }
    // Only the last 10 frames are retained.
    assert_eq!(store.low_water(), Seq(91));
    assert_eq!(store.high_water(), Seq(100));
    // Old range is gone (aged out); recent range served.
    assert!(store.range(Seq(50), 5).is_empty());
    let recent = store.range(Seq(95), 3);
    assert_eq!(
        recent.iter().map(|f| f.seq.0).collect::<Vec<_>>(),
        vec![95, 96, 97]
    );
}

#[test]
fn rewinder_serves_from_journal() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = JournalConfig::new(dir.path());
    config.fsync = FsyncPolicy::EveryFrame;
    {
        let mut journal = Journal::open(config.clone()).unwrap().journal;
        for seq in 1..=60 {
            journal.append(&frame(seq)).unwrap();
        }
    }
    let rewinder = JournalRewinder::new(config, ticktape_journal::RealStorage);
    assert_eq!(rewinder.low_water(), Seq(1));
    assert_eq!(rewinder.high_water(), Seq(60));
    let got = rewinder.range(Seq(10), 4);
    assert_eq!(
        got.iter().map(|f| f.seq.0).collect::<Vec<_>>(),
        vec![10, 11, 12, 13]
    );
}

#[test]
fn chain_serves_recent_from_repeater_and_old_from_rewinder() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = JournalConfig::new(dir.path());
    config.fsync = FsyncPolicy::EveryFrame;
    let repeater = MemStore::with_capacity(10);
    {
        let mut journal = Journal::open(config.clone()).unwrap().journal;
        for seq in 1..=100 {
            journal.append(&frame(seq)).unwrap();
            repeater.record(frame(seq)); // repeater keeps only last 10
        }
    }
    let chain = ChainStore {
        primary: repeater,
        secondary: JournalRewinder::new(config, ticktape_journal::RealStorage),
    };
    // Recent range (in the window) → served (from repeater).
    let recent = chain.range(Seq(95), 3);
    assert_eq!(
        recent.iter().map(|f| f.seq.0).collect::<Vec<_>>(),
        vec![95, 96, 97]
    );
    // Old range (aged out of the window) → served from the journal.
    let old = chain.range(Seq(5), 4);
    assert_eq!(
        old.iter().map(|f| f.seq.0).collect::<Vec<_>>(),
        vec![5, 6, 7, 8]
    );
    assert_eq!(chain.low_water(), Seq(1));
    assert_eq!(chain.high_water(), Seq(100));
}
