//! The M0 acceptance test: the KV store survives a crash and rebuilds
//! bit-identical state by replay.

use kv::{Cmd, Evt, Kv};
use ticktape::{FsyncPolicy, Node, NodeConfig, Seq};

fn config(dir: &std::path::Path) -> NodeConfig {
    let mut config = NodeConfig::new(dir);
    config.journal.fsync = FsyncPolicy::EveryFrame;
    config
}

#[test]
fn kv_survives_crash_and_replays() {
    let dir = tempfile::tempdir().unwrap();

    {
        let mut node: Node<Kv> = Node::open(config(dir.path()), ()).unwrap();
        node.submit(Cmd::Put {
            key: "a".into(),
            value: "1".into(),
        })
        .unwrap();
        node.submit(Cmd::Put {
            key: "b".into(),
            value: "2".into(),
        })
        .unwrap();
        node.submit(Cmd::Put {
            key: "a".into(),
            value: "overwritten".into(),
        })
        .unwrap();
        node.submit(Cmd::Del { key: "b".into() }).unwrap();
        // Crash: no clean shutdown.
    }

    let mut node: Node<Kv> = Node::open(config(dir.path()), ()).unwrap();
    assert_eq!(node.seq(), Seq(4));
    assert_eq!(node.service().get("a"), Some("overwritten"));
    assert_eq!(node.service().get("b"), None);
    assert_eq!(node.service().len(), 1);

    // Sequenced reads answer identically to what a replay would compute.
    let (_, outs) = node.submit(Cmd::Get { key: "a".into() }).unwrap();
    assert_eq!(outs, vec![Evt::Value(Some("overwritten".into()))]);

    assert!(node.verify_replay().unwrap());
}

#[test]
fn kv_replay_is_deterministic_across_many_ops() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut node: Node<Kv> = Node::open(config(dir.path()), ()).unwrap();
        for i in 0..500u32 {
            let key = format!("key-{}", i % 50);
            match i % 3 {
                0 => {
                    node.submit(Cmd::Put {
                        key,
                        value: format!("v{i}"),
                    })
                    .unwrap();
                }
                1 => {
                    node.submit(Cmd::Get { key }).unwrap();
                }
                _ => {
                    node.submit(Cmd::Del { key }).unwrap();
                }
            }
        }
    }
    let mut node: Node<Kv> = Node::open(config(dir.path()), ()).unwrap();
    assert_eq!(node.seq(), Seq(500));
    assert!(node.verify_replay().unwrap());
}
