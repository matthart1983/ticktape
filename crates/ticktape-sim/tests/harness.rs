//! Acceptance tests for the determinism harness itself: a correct service
//! survives heavy fault injection; broken services are caught; failures
//! reproduce and shrink.

use std::sync::atomic::{AtomicI64, Ordering};
use ticktape_codec::{Decode, Encode};
use ticktape_core::{Ctx, Seq, Service};
use ticktape_sim::demo::{gen_transfer, Bank};
use ticktape_sim::{simulate, vopr, InvariantViolation, Invariants, Rng, SimConfig};

#[test]
fn correct_service_survives_heavy_fault_injection() {
    let base = SimConfig::new(0);
    let stats = vopr::<Bank>(&base, 0..60, (), gen_transfer)
        .unwrap_or_else(|failure| panic!("clean service failed: {failure}"));
    // The fuzz actually exercised the machinery it claims to.
    assert!(stats.inputs > 5_000, "too few inputs: {stats:?}");
    assert!(stats.crashes > 60, "too few crashes: {stats:?}");
    assert!(stats.syncs > 500, "too few syncs: {stats:?}");
}

#[test]
fn same_seed_reproduces_identical_runs() {
    let config = SimConfig::new(0xDEAD_BEEF);
    let a = simulate::<Bank>(&config, (), gen_transfer);
    let b = simulate::<Bank>(&config, (), gen_transfer);
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
}

// ---------------------------------------------------------------------
// Bug class 1: ambient state. `apply` reads a process-global — state is no
// longer a pure function of the input stream, so an independent replay
// diverges from the incrementally-built live state.

static AMBIENT: AtomicI64 = AtomicI64::new(0);

struct AmbientService {
    tainted: i64,
}

#[derive(Encode, Decode, Debug)]
struct Poke(u8);

impl Service for AmbientService {
    type Input = Poke;
    type Output = ();
    type Snapshot = i64;
    type Config = ();

    fn genesis(_: &()) -> Self {
        AmbientService { tainted: 0 }
    }

    fn apply(&mut self, _seq: Seq, _input: &Poke, _ctx: &mut Ctx<'_, ()>) {
        // The bug: every apply, anywhere in the process, bumps a global.
        self.tainted = AMBIENT.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> i64 {
        self.tainted
    }

    fn restore(tainted: i64, _: &()) -> Self {
        AmbientService { tainted }
    }
}

impl Invariants for AmbientService {
    fn check(&self) -> Result<(), InvariantViolation> {
        Ok(())
    }
}

#[test]
fn catches_ambient_state_nondeterminism() {
    let mut config = SimConfig::new(1);
    config.steps = 200;
    let failure =
        simulate::<AmbientService>(&config, (), |rng: &mut Rng| Poke(rng.below(256) as u8))
            .expect_err("harness must catch the ambient-state bug");
    assert!(
        failure.violation.what.contains("determinism violated"),
        "wrong violation: {failure}"
    );
}

// ---------------------------------------------------------------------
// Bug class 2: a broken application invariant. This bank allows overdrafts,
// so a balance eventually goes negative; `Invariants::check` must catch it
// on a live step, and the failure must shrink + reproduce.

struct OverdraftBank {
    balances: Vec<i64>,
}

impl Service for OverdraftBank {
    type Input = ticktape_sim::demo::Cmd;
    type Output = ();
    type Snapshot = Vec<i64>;
    type Config = ();

    fn genesis(_: &()) -> Self {
        OverdraftBank {
            balances: vec![
                ticktape_sim::demo::OPENING_BALANCE;
                ticktape_sim::demo::ACCOUNTS as usize
            ],
        }
    }

    fn apply(&mut self, _seq: Seq, cmd: &Self::Input, _ctx: &mut Ctx<'_, ()>) {
        let ticktape_sim::demo::Cmd::Transfer { from, to, amount } = *cmd;
        if from == to {
            return;
        }
        // The bug: no balance check.
        self.balances[from as usize] -= amount as i64;
        self.balances[to as usize] += amount as i64;
    }

    fn snapshot(&self) -> Vec<i64> {
        self.balances.clone()
    }

    fn restore(balances: Vec<i64>, _: &()) -> Self {
        OverdraftBank { balances }
    }
}

impl Invariants for OverdraftBank {
    fn check(&self) -> Result<(), InvariantViolation> {
        if let Some(negative) = self.balances.iter().find(|b| **b < 0) {
            return Err(InvariantViolation::new(format!(
                "negative balance: {negative}"
            )));
        }
        Ok(())
    }
}

#[test]
fn catches_invariant_violation_and_shrinks() {
    let base = SimConfig::new(0);
    let shrunk = vopr::<OverdraftBank>(&base, 0..20, (), gen_transfer)
        .expect_err("harness must catch the overdraft bug");
    assert!(
        shrunk.failure.violation.what.contains("negative balance"),
        "{shrunk}"
    );

    // The shrunk recipe reproduces the identical violation.
    let config = SimConfig {
        seed: shrunk.failure.seed,
        steps: shrunk.min_steps,
        ..base
    };
    let reproduced =
        simulate::<OverdraftBank>(&config, (), gen_transfer).expect_err("must reproduce");
    assert_eq!(reproduced.violation, shrunk.failure.violation);
    assert!(
        shrunk.min_steps <= shrunk.failure.step + 1,
        "shrink did not reduce the schedule: {shrunk}"
    );
}

// ---------------------------------------------------------------------
// Bug class 3: silent media corruption must be *detected*. Bit-rot inside
// synced journal data may fail recovery loudly (sealed segment) or truncate
// the tail (final segment) — what it must never do is silently serve
// corrupt state that diverges from the surviving journal.

#[test]
fn bit_rot_in_synced_data_is_never_silently_wrong() {
    use ticktape_sim::SimStorage;
    // Build a journal through the public Node API, synced throughout.
    use ticktape_core::Timestamp;
    use ticktape_journal::{FsyncPolicy, Journal};
    use ticktape_runtime::{Node, NodeConfig, TimeSource};

    #[derive(Clone, Default)]
    struct FixedClock;
    impl TimeSource for FixedClock {
        fn now(&mut self) -> Timestamp {
            Timestamp(42)
        }
    }

    let storage = SimStorage::new();
    let mut nc = NodeConfig::new("/rot/journal");
    nc.journal.segment_bytes = 256; // several sealed segments
    nc.journal.fsync = FsyncPolicy::EveryFrame;

    {
        let mut node: Node<Bank, FixedClock, SimStorage> =
            Node::open_with(nc.clone(), (), FixedClock, storage.clone()).unwrap();
        let mut rng = Rng::new(9);
        for _ in 0..60 {
            node.submit(gen_transfer(&mut rng)).unwrap();
        }
    }

    // Rot one synced byte per segment. SimStorage clones share the disk,
    // so corruption (and any recovery truncation) accumulates across
    // iterations — which only makes the property stricter: every outcome
    // must be loud (error) or safe (validated truncation), never silent.
    for (i, path) in storage.file_paths().into_iter().enumerate() {
        storage.rot_synced_byte(&path, 40 + i * 3, (i % 8) as u32);
        match Journal::open_with(nc.journal.clone(), storage.clone()) {
            Err(_) => {} // loud refusal: correct
            Ok(recovered) => {
                // Only acceptable if the rot hit the final segment and the
                // journal truncated to an intact prefix — every surviving
                // frame must still be CRC-valid and gapless.
                for (j, frame) in recovered.frames.iter().enumerate() {
                    assert_eq!(
                        frame.seq.as_u64(),
                        j as u64 + 1,
                        "gap after rot in {path:?}"
                    );
                }
                assert!(recovered.frames.len() <= 60);
            }
        }
    }
}

// ---------------------------------------------------------------------
// Bug class 4: snapshot/restore asymmetry. `restore` mis-rebuilds state
// (off-by-one), so a recovery that goes through a snapshot diverges from a
// full replay of the same inputs. Only exercised when snapshots are on —
// this is the check the M2 snapshot path adds.

struct OffByOneRestore {
    total: i64,
}

impl Service for OffByOneRestore {
    type Input = Poke;
    type Output = ();
    type Snapshot = i64;
    type Config = ();

    fn genesis(_: &()) -> Self {
        OffByOneRestore { total: 0 }
    }

    fn apply(&mut self, _seq: Seq, input: &Poke, _ctx: &mut Ctx<'_, ()>) {
        self.total += input.0 as i64;
    }

    fn snapshot(&self) -> i64 {
        self.total
    }

    fn restore(total: i64, _: &()) -> Self {
        // The bug.
        OffByOneRestore { total: total + 1 }
    }
}

impl Invariants for OffByOneRestore {
    fn check(&self) -> Result<(), InvariantViolation> {
        Ok(())
    }
}

#[test]
fn catches_broken_restore_via_snapshot_recovery() {
    let mut config = SimConfig::new(2);
    config.steps = 400;
    config.snapshot_every = Some(10); // recover via snapshots often
    let failure =
        simulate::<OffByOneRestore>(&config, (), |rng: &mut Rng| Poke(rng.below(256) as u8))
            .expect_err("harness must catch the restore bug");
    assert!(
        failure.violation.what.contains("determinism violated"),
        "wrong violation: {failure}"
    );

    // Same service with snapshots disabled never touches restore: clean.
    config.snapshot_every = None;
    simulate::<OffByOneRestore>(&config, (), |rng: &mut Rng| Poke(rng.below(256) as u8))
        .expect("full-replay recovery does not exercise the bug");
}
