//! The M1 deliverable: `cargo test` fuzzes the KV store under seeded
//! storage faults — crashes, lost unsynced tails, torn writes — checking
//! durability, total order, determinism, and the KV invariants after every
//! recovery. A failing seed prints a deterministic reproduction recipe.

use kv::{Cmd, Kv};
use ticktape_sim::{vopr, Rng, SimConfig};

fn gen_cmd(rng: &mut Rng) -> Cmd {
    let key = format!("key-{}", rng.below(40));
    match rng.below(10) {
        0..=4 => Cmd::Put {
            key,
            value: format!("v{}", rng.below(1_000_000)),
        },
        5..=7 => Cmd::Get { key },
        _ => Cmd::Del { key },
    }
}

#[test]
fn kv_survives_seeded_fault_injection() {
    let base = SimConfig::new(0);
    let stats = vopr::<Kv>(&base, 0..40, (), gen_cmd)
        .unwrap_or_else(|failure| panic!("kv failed under faults: {failure}"));
    assert!(stats.inputs > 5_000, "fuzz too shallow: {stats:?}");
    assert!(stats.crashes > 40, "fuzz too gentle: {stats:?}");
}
