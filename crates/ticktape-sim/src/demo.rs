//! The demo service the `vopr` binary fuzzes: a tiny bank with the classic
//! conservation invariant. Also a template for wiring your own service into
//! the simulator.

use crate::{InvariantViolation, Invariants, Rng};
use ticktape_codec::{Decode, Encode};
use ticktape_core::{Ctx, Seq, Service};

pub const ACCOUNTS: u64 = 8;
pub const OPENING_BALANCE: i64 = 1_000;

/// Deterministic toy bank: fixed accounts, transfers only.
pub struct Bank {
    balances: Vec<i64>,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq)]
pub enum Cmd {
    Transfer { from: u8, to: u8, amount: u32 },
}

#[derive(Encode, Decode, Debug, PartialEq)]
pub enum Evt {
    Transferred {
        from: u8,
        to: u8,
        amount: u32,
    },
    /// Overdrafts and self-transfers are rejected, deterministically.
    Rejected,
}

impl Service for Bank {
    type Input = Cmd;
    type Output = Evt;
    type Snapshot = Vec<i64>;
    type Config = ();

    fn genesis(_: &()) -> Self {
        Bank {
            balances: vec![OPENING_BALANCE; ACCOUNTS as usize],
        }
    }

    fn apply(&mut self, _seq: Seq, cmd: &Cmd, ctx: &mut Ctx<'_, Evt>) {
        let Cmd::Transfer { from, to, amount } = *cmd;
        let (from_idx, to_idx) = (from as usize, to as usize);
        if from == to
            || from_idx >= self.balances.len()
            || to_idx >= self.balances.len()
            || self.balances[from_idx] < amount as i64
        {
            ctx.emit(Evt::Rejected);
            return;
        }
        self.balances[from_idx] -= amount as i64;
        self.balances[to_idx] += amount as i64;
        ctx.emit(Evt::Transferred { from, to, amount });
    }

    fn snapshot(&self) -> Vec<i64> {
        self.balances.clone()
    }

    fn restore(balances: Vec<i64>, _: &()) -> Self {
        Bank { balances }
    }
}

impl Invariants for Bank {
    fn check(&self) -> Result<(), InvariantViolation> {
        let total: i64 = self.balances.iter().sum();
        if total != ACCOUNTS as i64 * OPENING_BALANCE {
            return Err(InvariantViolation::new(format!(
                "money not conserved: total is {total}, expected {}",
                ACCOUNTS as i64 * OPENING_BALANCE
            )));
        }
        if let Some(negative) = self.balances.iter().find(|b| **b < 0) {
            return Err(InvariantViolation::new(format!(
                "negative balance: {negative}"
            )));
        }
        Ok(())
    }
}

/// The seeded workload for [`Bank`].
pub fn gen_transfer(rng: &mut Rng) -> Cmd {
    Cmd::Transfer {
        from: rng.below(ACCOUNTS) as u8,
        to: rng.below(ACCOUNTS) as u8,
        amount: rng.below(600) as u32,
    }
}
