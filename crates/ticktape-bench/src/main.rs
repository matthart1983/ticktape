//! Benchmarks against the spec's performance budgets.
//!
//! These are design budgets, not SLAs: they bound the architecture and
//! catch order-of-magnitude regressions. Run in release; numbers vary with
//! hardware, filesystem, and (in CI) noisy neighbors:
//!
//! ```text
//! cargo run --release -p ticktape-bench
//! ```
//!
//! Methodology is deliberately simple and dependency-free: warmups, then
//! per-op `Instant` samples (≈20–30 ns overhead per sample, negligible at
//! the µs scale and reported honestly at the ns scale), percentiles over
//! the full sample set. No statistical machinery — a budget miss by 3× is
//! signal, a miss by 10% is weather.

use std::hint::black_box;
use std::time::Instant;
use ticktape_core::{encode_to_vec, Ctx, Frame, FrameKind, OutBuf, Seq, Service, Timestamp};
use ticktape_journal::{FsyncPolicy, Journal, JournalConfig};
use ticktape_runtime::{ManualClock, Node, NodeConfig};
use ticktape_sim::demo::{gen_transfer, Bank};
use ticktape_sim::{simulate, Rng, SimConfig};
use ticktape_transport::wire::Packet;
use ticktape_transport::Reassembler;

struct Percentiles {
    p50: u64,
    p99: u64,
    max: u64,
}

fn percentiles(mut samples: Vec<u64>) -> Percentiles {
    samples.sort_unstable();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
    Percentiles {
        p50: at(0.50),
        p99: at(0.99),
        max: *samples.last().unwrap(),
    }
}

fn row(name: &str, measured: String, budget: &str) {
    println!("{name:<44} {measured:<28} budget: {budget}");
}

fn main() {
    println!("ticktape bench — spec §14 budgets (budgets, not SLAs)\n");

    apply_step();
    submit_latency(
        "submit (fsync=never)",
        FsyncPolicy::Never,
        200_000,
        "p50 < 1 µs, p99 < 5 µs*",
    );
    submit_latency(
        "submit (fsync=group-commit 50µs)",
        FsyncPolicy::Micros(50),
        100_000,
        "p50 < 1 µs, p99 < 5 µs",
    );
    submit_latency(
        "submit (fsync=every frame)",
        FsyncPolicy::EveryFrame,
        2_000,
        "p99 < 15 µs (NVMe)",
    );
    cold_recovery();
    reassembler_throughput();
    simulator_speed();

    println!(
        "\n* the never/group-commit budgets correspond to the spec's IPC\n  \
         ingress→journal→publish path; this measures ingress→journal→apply\n  \
         (publish is the transport fan-out, benched separately by packet)."
    );
}

/// Core `apply` step, trivial service. Budget: < 200 ns.
fn apply_step() {
    let mut bank = Bank::genesis(&());
    let mut rng = Rng::new(7);
    let cmds: Vec<_> = (0..1024).map(|_| gen_transfer(&mut rng)).collect();
    let mut out = OutBuf::new();
    let mut timer_ops = Vec::new();

    // Warmup.
    for i in 0..100_000u64 {
        let mut ctx = Ctx::new(Seq(i + 1), Timestamp(i), &mut out, &mut timer_ops);
        bank.apply(Seq(i + 1), &cmds[(i % 1024) as usize], &mut ctx);
        out.drain();
    }

    let iters = 2_000_000u64;
    let start = Instant::now();
    for i in 0..iters {
        let mut ctx = Ctx::new(Seq(i + 1), Timestamp(i), &mut out, &mut timer_ops);
        bank.apply(Seq(i + 1), black_box(&cmds[(i % 1024) as usize]), &mut ctx);
        black_box(out.drain());
    }
    let ns = start.elapsed().as_nanos() as u64 / iters;
    row(
        "apply step (Bank service)",
        format!("{ns} ns/op"),
        "< 200 ns",
    );
}

/// Sequencer ingress → journal → apply, on the real filesystem.
fn submit_latency(name: &str, fsync: FsyncPolicy, ops: u64, budget: &str) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = NodeConfig::new(dir.path());
    config.journal.fsync = fsync;
    let mut node: Node<Bank, _> =
        Node::open_with_clock(config, (), ManualClock(Timestamp(1))).unwrap();
    let mut rng = Rng::new(9);
    let cmds: Vec<_> = (0..1024).map(|_| gen_transfer(&mut rng)).collect();

    for i in 0..(ops / 10).max(100) {
        node.submit(cmds[(i % 1024) as usize].clone()).unwrap();
    }

    let mut samples = Vec::with_capacity(ops as usize);
    let bench_start = Instant::now();
    for i in 0..ops {
        let cmd = cmds[(i % 1024) as usize].clone();
        let start = Instant::now();
        node.submit(black_box(cmd)).unwrap();
        samples.push(start.elapsed().as_nanos() as u64);
    }
    let wall = bench_start.elapsed();
    let stats = percentiles(samples);
    let throughput = ops as f64 / wall.as_secs_f64();
    row(
        name,
        format!(
            "p50 {} · p99 {} · max {} · {:.2} M/s",
            fmt_ns(stats.p50),
            fmt_ns(stats.p99),
            fmt_ns(stats.max),
            throughput / 1e6
        ),
        budget,
    );
}

/// Cold recovery: replay a journal through the service. Budget context:
/// < 1 s for a day of typical volume; reported as frames/s.
fn cold_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = JournalConfig::new(dir.path());
    config.fsync = FsyncPolicy::Never;
    let total = 1_000_000u64;
    {
        let mut journal = Journal::open(config.clone()).unwrap().journal;
        let mut rng = Rng::new(3);
        for seq in 1..=total {
            let frame = Frame::new(
                Seq(seq),
                Timestamp(seq),
                1,
                FrameKind::Input,
                encode_to_vec(&gen_transfer(&mut rng)),
            );
            journal.append(&frame).unwrap();
        }
        journal.sync().unwrap();
    }

    let start = Instant::now();
    let mut node_config = NodeConfig::new(dir.path());
    node_config.journal.fsync = FsyncPolicy::Never;
    let node: Node<Bank, _> =
        Node::open_with_clock(node_config, (), ManualClock(Timestamp(1))).unwrap();
    let elapsed = start.elapsed();
    assert_eq!(node.seq(), Seq(total));
    row(
        "cold recovery (journal read + replay)",
        format!(
            "{total} frames in {:.2} s ({:.2} M/s)",
            elapsed.as_secs_f64(),
            total as f64 / elapsed.as_secs_f64() / 1e6
        ),
        "< 1 s / day of data",
    );
}

/// Transport reliability core. Context for the < 2 µs fan-out budget: the
/// per-frame cost of the receive path's state machine.
fn reassembler_throughput() {
    let total = 1_000_000u64;
    let frames: Vec<Frame> = (1..=total)
        .map(|seq| Frame::new(Seq(seq), Timestamp(seq), 1, FrameKind::Input, vec![0u8; 32]))
        .collect();
    let start = Instant::now();
    let mut reassembler = Reassembler::new(Seq(1));
    let mut delivered = 0u64;
    for chunk in frames.chunks(4) {
        reassembler
            .ingest(Packet::Data {
                session: 1,
                frames: chunk.to_vec(),
            })
            .unwrap();
        while let Some(frame) = reassembler.next_frame() {
            black_box(&frame);
            delivered += 1;
        }
    }
    let elapsed = start.elapsed();
    assert_eq!(delivered, total);
    row(
        "reassembler ingest+deliver (in order)",
        format!(
            "{:.2} M frames/s ({} ns/frame)",
            total as f64 / elapsed.as_secs_f64() / 1e6,
            elapsed.as_nanos() as u64 / total
        ),
        "supports < 2 µs fan-out",
    );
}

/// Whole-node simulation speed: virtual time advanced per wall second.
/// Budget: ≥ 1000× wall-clock.
fn simulator_speed() {
    let mut config = SimConfig::new(11);
    config.steps = 4_000;
    let start = Instant::now();
    let stats = simulate::<Bank>(&config, (), gen_transfer).expect("clean sim run");
    let wall = start.elapsed();
    // The sim clock advances uniformly in [0, 1s) per step: E ≈ 0.5 s/step.
    let virtual_secs = config.steps as f64 * 0.5;
    row(
        "simulator speed (virtual/wall, est.)",
        format!(
            "{:.0}× ({} steps, {} crashes survived, {:.2} s wall)",
            virtual_secs / wall.as_secs_f64(),
            config.steps,
            stats.crashes,
            wall.as_secs_f64()
        ),
        ">= 1000×",
    );
}

fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.2} ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.1} µs", ns as f64 / 1e3)
    } else {
        format!("{ns} ns")
    }
}
