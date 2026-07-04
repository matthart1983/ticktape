//! The VOPR: fuzz the demo Bank service under seeded storage faults,
//! continuously or for a fixed number of runs.
//!
//! ```text
//! cargo run --release -p ticktape-sim --bin vopr              # run forever from a random-ish seed
//! cargo run --release -p ticktape-sim --bin vopr -- --runs 1000
//! cargo run --release -p ticktape-sim --bin vopr -- --seed 42 --steps 400   # reproduce one run
//! ```

use ticktape_sim::demo::{gen_transfer, Bank};
use ticktape_sim::{simulate, vopr, SimConfig};

fn main() {
    let mut seed: Option<u64> = None;
    let mut runs: Option<u64> = None;
    let mut steps: u32 = 400;

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let value = |i: usize| {
            args.get(i + 1)
                .unwrap_or_else(|| usage())
                .parse()
                .unwrap_or_else(|_| usage())
        };
        match args[i].as_str() {
            "--seed" => {
                seed = Some(value(i));
                i += 2;
            }
            "--runs" => {
                runs = Some(value(i));
                i += 2;
            }
            "--steps" => {
                steps = value(i) as u32;
                i += 2;
            }
            _ => usage(),
        }
    }

    let mut base = SimConfig::new(0);
    base.steps = steps;

    // Reproduce a single seed and exit.
    if let (Some(seed), None) = (seed, runs) {
        let config = SimConfig { seed, ..base };
        match simulate::<Bank>(&config, (), gen_transfer) {
            Ok(stats) => println!("seed {seed}: OK ({stats:?})"),
            Err(failure) => {
                eprintln!("{failure}");
                std::process::exit(1);
            }
        }
        return;
    }

    // Fuzz loop. The start seed is arbitrary but printed, so any failure is
    // reproducible; wall-clock nanos just avoid re-fuzzing the same seeds
    // every invocation.
    let start = seed.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1)
    });
    let total = runs.unwrap_or(u64::MAX);
    println!("vopr: fuzzing Bank from seed {start}, {steps} steps/run");

    const BATCH: u64 = 100;
    let mut done = 0u64;
    while done < total {
        let batch = BATCH.min(total - done);
        let seeds = start.wrapping_add(done)..start.wrapping_add(done + batch);
        match vopr::<Bank>(&base, seeds, (), gen_transfer) {
            Ok(stats) => {
                done += batch;
                println!(
                    "  {done} runs clean ({} inputs, {} crashes survived)",
                    stats.inputs, stats.crashes
                );
            }
            Err(shrunk) => {
                eprintln!("FAILURE: {shrunk}");
                std::process::exit(1);
            }
        }
    }
    println!("vopr: {done} runs, all clean");
}

fn usage() -> ! {
    eprintln!("usage: vopr [--seed N] [--runs N] [--steps N]");
    std::process::exit(2)
}
