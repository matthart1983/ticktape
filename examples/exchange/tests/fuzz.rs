//! The session layer under the M1 simulator: seeded client traffic —
//! including duplicate client seqs (a buggy or malicious gateway) and
//! surprise disconnects — with crashes and torn writes underneath, checking
//! the exchange's ownership/dedup invariants after every input and every
//! recovery.

use exchange::Exchange;
use orderbook::{Cmd, Side};
use std::collections::BTreeMap;
use ticktape_gateway::GatewayInput;
use ticktape_sim::{simulate, Rng, SimConfig, SimStats};

const SESSIONS: u64 = 4;

fn gen(
    counters: &mut BTreeMap<u64, u64>,
    next_order: &mut u64,
    rng: &mut Rng,
) -> GatewayInput<Cmd> {
    let session = rng.below(SESSIONS) + 1;
    if rng.chance(1, 12) {
        return GatewayInput::SessionClosed { session };
    }
    let counter = counters.entry(session).or_insert(0);
    // Mostly the next seq; sometimes a stale duplicate (must be a no-op).
    let client_seq = if rng.chance(1, 6) && *counter > 0 {
        1 + rng.below(*counter)
    } else {
        *counter += 1;
        *counter
    };
    let cmd = if rng.chance(1, 4) {
        Cmd::Cancel {
            id: rng.below(*next_order + 1),
        }
    } else {
        *next_order += 1;
        Cmd::Submit {
            id: *next_order,
            side: if rng.chance(1, 2) {
                Side::Buy
            } else {
                Side::Sell
            },
            price: 90 + rng.below(21) as u32,
            qty: rng.below(50) as u32,
        }
    };
    GatewayInput::Client {
        session,
        client_seq,
        cmd,
    }
}

#[test]
fn exchange_survives_faults_with_session_invariants() {
    // The workload generator is stateful (per-session seq counters), so
    // state is rebuilt fresh per seed to keep every run reproducible —
    // `simulate` per seed rather than the stateless `vopr` loop.
    let base = SimConfig::new(0);
    let mut totals = SimStats::default();
    for seed in 0..30u64 {
        let config = SimConfig {
            seed,
            ..base.clone()
        };
        let mut counters = BTreeMap::new();
        let mut next_order = 0u64;
        let stats =
            simulate::<Exchange>(&config, (), |rng| gen(&mut counters, &mut next_order, rng))
                .unwrap_or_else(|failure| panic!("exchange failed under faults: {failure}"));
        totals.inputs += stats.inputs;
        totals.crashes += stats.crashes;
    }
    assert!(totals.inputs > 4_000, "fuzz too shallow: {totals:?}");
    assert!(totals.crashes > 30, "fuzz too gentle: {totals:?}");
}
