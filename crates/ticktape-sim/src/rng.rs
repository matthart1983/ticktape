//! The simulator's seeded PRNG.
//!
//! SplitMix64: tiny, fast, well-distributed, and — the property everything
//! here depends on — the same seed produces the same stream on every
//! machine, forever. Deliberately not `rand`: no dependency, no algorithm
//! churn under our feet (a `rand` upgrade must never invalidate archived
//! failing seeds).

/// A deterministic, seedable PRNG (SplitMix64).
#[derive(Debug, Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform-ish in `[0, n)`. Modulo bias is irrelevant at simulation
    /// scale and keeps the generator's output stream simple to reason about.
    pub fn below(&mut self, n: u64) -> u64 {
        debug_assert!(n > 0);
        self.next_u64() % n
    }

    /// True with probability `num`/`den`.
    pub fn chance(&mut self, num: u64, den: u64) -> bool {
        self.below(den) < num
    }
}

#[cfg(test)]
mod tests {
    use super::Rng;

    #[test]
    fn same_seed_same_stream() {
        let a: Vec<u64> = {
            let mut r = Rng::new(42);
            (0..64).map(|_| r.next_u64()).collect()
        };
        let b: Vec<u64> = {
            let mut r = Rng::new(42);
            (0..64).map(|_| r.next_u64()).collect()
        };
        assert_eq!(a, b);
        let c: Vec<u64> = {
            let mut r = Rng::new(43);
            (0..64).map(|_| r.next_u64()).collect()
        };
        assert_ne!(a, c);
    }

    #[test]
    fn splitmix64_known_answer() {
        // Reference vector for seed 0 (Vigna's splitmix64.c).
        let mut r = Rng::new(0);
        assert_eq!(r.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(r.next_u64(), 0x6E78_9E6A_A1B9_65F4);
    }
}
