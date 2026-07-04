//! A price-time-priority central limit order book (CLOB) as a Ticktape
//! `Service` — the flagship example: real matching semantics, deterministic
//! by construction, journaled, snapshotted, and fuzzed under fault
//! injection with exchange-grade invariants (no crossed book, share
//! conservation).
//!
//! Semantics:
//! - **Price priority**: a taker matches the best opposite price first
//!   (highest bid / lowest ask), and trades execute at the *maker's* price.
//! - **Time priority**: within a price level, resting orders fill strictly
//!   first-in-first-out.
//! - Partial fills rest on the book. Zero-qty and duplicate-id (currently
//!   resting) submissions are rejected deterministically.
//!
//! Determinism notes: `BTreeMap` levels + `VecDeque` FIFO queues give a
//! canonical iteration order; the snapshot serializes the book in that
//! order, so it is byte-identical on every replica at the same seq.

use std::collections::{BTreeMap, VecDeque};
use ticktape::{Ctx, Decode, Encode, Seq, Service};
use ticktape_sim::{InvariantViolation, Invariants};

pub type Price = u32;
pub type Qty = u32;
pub type OrderId = u64;

#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq)]
pub enum Cmd {
    Submit {
        id: OrderId,
        side: Side,
        price: Price,
        qty: Qty,
    },
    Cancel {
        id: OrderId,
    },
}

#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reject {
    ZeroQty,
    /// The id is already resting on the book.
    DuplicateId,
    UnknownOrder,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq)]
pub enum Evt {
    Accepted {
        id: OrderId,
    },
    Trade {
        taker: OrderId,
        maker: OrderId,
        price: Price,
        qty: Qty,
    },
    Canceled {
        id: OrderId,
        remaining: Qty,
    },
    Rejected {
        id: OrderId,
        reason: Reject,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Resting {
    id: OrderId,
    qty: Qty,
}

/// The book. Bids match from the highest price, asks from the lowest.
pub struct OrderBook {
    bids: BTreeMap<Price, VecDeque<Resting>>,
    asks: BTreeMap<Price, VecDeque<Resting>>,
    /// Where each resting order sits, for cancels and duplicate checks.
    index: BTreeMap<OrderId, (Side, Price)>,
    // Share-conservation ledger (see `Invariants`).
    accepted_shares: u64,
    traded_shares: u64,
    canceled_shares: u64,
}

/// Canonical serialized book: levels in ascending price order, FIFO order
/// within each level, plus the conservation counters.
#[derive(Encode, Decode, Debug, PartialEq)]
pub struct BookSnapshot {
    bids: Vec<(Price, Vec<(OrderId, Qty)>)>,
    asks: Vec<(Price, Vec<(OrderId, Qty)>)>,
    accepted_shares: u64,
    traded_shares: u64,
    canceled_shares: u64,
}

impl Service for OrderBook {
    type Input = Cmd;
    type Output = Evt;
    type Snapshot = BookSnapshot;
    type Config = ();

    fn genesis(_: &()) -> Self {
        OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            index: BTreeMap::new(),
            accepted_shares: 0,
            traded_shares: 0,
            canceled_shares: 0,
        }
    }

    fn apply(&mut self, _seq: Seq, cmd: &Cmd, ctx: &mut Ctx<'_, Evt>) {
        match *cmd {
            Cmd::Submit {
                id,
                side,
                price,
                qty,
            } => self.submit(id, side, price, qty, ctx),
            Cmd::Cancel { id } => self.cancel(id, ctx),
        }
    }

    fn snapshot(&self) -> BookSnapshot {
        let serialize = |side: &BTreeMap<Price, VecDeque<Resting>>| {
            side.iter()
                .map(|(&price, level)| (price, level.iter().map(|o| (o.id, o.qty)).collect()))
                .collect()
        };
        BookSnapshot {
            bids: serialize(&self.bids),
            asks: serialize(&self.asks),
            accepted_shares: self.accepted_shares,
            traded_shares: self.traded_shares,
            canceled_shares: self.canceled_shares,
        }
    }

    fn restore(snap: BookSnapshot, _: &()) -> Self {
        let mut book = OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            index: BTreeMap::new(),
            accepted_shares: snap.accepted_shares,
            traded_shares: snap.traded_shares,
            canceled_shares: snap.canceled_shares,
        };
        for (side, levels) in [(Side::Buy, snap.bids), (Side::Sell, snap.asks)] {
            for (price, orders) in levels {
                let mut level = VecDeque::with_capacity(orders.len());
                for (id, qty) in orders {
                    book.index.insert(id, (side, price));
                    level.push_back(Resting { id, qty });
                }
                book.side_mut(side).insert(price, level);
            }
        }
        book
    }
}

impl OrderBook {
    fn side_mut(&mut self, side: Side) -> &mut BTreeMap<Price, VecDeque<Resting>> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn submit(&mut self, id: OrderId, side: Side, price: Price, qty: Qty, ctx: &mut Ctx<'_, Evt>) {
        if qty == 0 {
            ctx.emit(Evt::Rejected {
                id,
                reason: Reject::ZeroQty,
            });
            return;
        }
        if self.index.contains_key(&id) {
            ctx.emit(Evt::Rejected {
                id,
                reason: Reject::DuplicateId,
            });
            return;
        }
        ctx.emit(Evt::Accepted { id });
        self.accepted_shares += qty as u64;

        let remaining = match side {
            Side::Buy => self.match_against_asks(id, price, qty, ctx),
            Side::Sell => self.match_against_bids(id, price, qty, ctx),
        };
        if remaining > 0 {
            self.index.insert(id, (side, price));
            self.side_mut(side)
                .entry(price)
                .or_default()
                .push_back(Resting { id, qty: remaining });
        }
    }

    /// Match a buy taker against asks, best (lowest) price first, FIFO
    /// within a level; trades execute at the maker's price. Returns the
    /// unfilled remainder.
    fn match_against_asks(
        &mut self,
        taker: OrderId,
        limit: Price,
        mut qty: Qty,
        ctx: &mut Ctx<'_, Evt>,
    ) -> Qty {
        while qty > 0 {
            let Some(best) = self.asks.keys().next().copied() else {
                break;
            };
            if best > limit {
                break;
            }
            qty = self.fill_level(Side::Sell, best, taker, qty, ctx);
        }
        qty
    }

    /// Match a sell taker against bids, best (highest) price first.
    fn match_against_bids(
        &mut self,
        taker: OrderId,
        limit: Price,
        mut qty: Qty,
        ctx: &mut Ctx<'_, Evt>,
    ) -> Qty {
        while qty > 0 {
            let Some(best) = self.bids.keys().next_back().copied() else {
                break;
            };
            if best < limit {
                break;
            }
            qty = self.fill_level(Side::Buy, best, taker, qty, ctx);
        }
        qty
    }

    /// Fill FIFO against the level at `price` on `maker_side`; removes the
    /// level when emptied. Returns the taker's unfilled remainder.
    fn fill_level(
        &mut self,
        maker_side: Side,
        price: Price,
        taker: OrderId,
        mut qty: Qty,
        ctx: &mut Ctx<'_, Evt>,
    ) -> Qty {
        let mut filled_ids: Vec<OrderId> = Vec::new();
        let mut traded_here = 0u64;
        {
            let level = self
                .side_mut(maker_side)
                .get_mut(&price)
                .expect("level exists for best price");
            while qty > 0 {
                let Some(maker) = level.front_mut() else {
                    break;
                };
                let fill = qty.min(maker.qty);
                maker.qty -= fill;
                qty -= fill;
                traded_here += fill as u64;
                ctx.emit(Evt::Trade {
                    taker,
                    maker: maker.id,
                    price,
                    qty: fill,
                });
                if maker.qty == 0 {
                    filled_ids.push(level.pop_front().expect("front exists").id);
                }
            }
        }
        // Counter + index updates outside the level borrow.
        self.traded_shares += traded_here;
        for id in &filled_ids {
            self.index.remove(id);
        }
        let level_empty = self
            .side_mut(maker_side)
            .get(&price)
            .is_some_and(VecDeque::is_empty);
        if level_empty {
            self.side_mut(maker_side).remove(&price);
        }
        qty
    }

    fn cancel(&mut self, id: OrderId, ctx: &mut Ctx<'_, Evt>) {
        let Some((side, price)) = self.index.remove(&id) else {
            ctx.emit(Evt::Rejected {
                id,
                reason: Reject::UnknownOrder,
            });
            return;
        };
        let level = self.side_mut(side).get_mut(&price).expect("indexed level");
        let pos = level
            .iter()
            .position(|o| o.id == id)
            .expect("indexed order in level");
        let order = level.remove(pos).expect("position valid");
        if level.is_empty() {
            self.side_mut(side).remove(&price);
        }
        self.canceled_shares += order.qty as u64;
        ctx.emit(Evt::Canceled {
            id,
            remaining: order.qty,
        });
    }

    pub fn best_bid(&self) -> Option<Price> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<Price> {
        self.asks.keys().next().copied()
    }

    /// Total shares resting on the book.
    pub fn resting_shares(&self) -> u64 {
        self.bids
            .values()
            .chain(self.asks.values())
            .flatten()
            .map(|o| o.qty as u64)
            .sum()
    }

    pub fn resting_orders(&self) -> usize {
        self.index.len()
    }

    /// Depth view for display: (price, total qty), bids best-first then
    /// asks best-first.
    #[allow(clippy::type_complexity)]
    pub fn depth(&self, levels: usize) -> (Vec<(Price, Qty)>, Vec<(Price, Qty)>) {
        let sum = |level: &VecDeque<Resting>| level.iter().map(|o| o.qty).sum::<Qty>();
        let bids = self
            .bids
            .iter()
            .rev()
            .take(levels)
            .map(|(&p, l)| (p, sum(l)))
            .collect();
        let asks = self
            .asks
            .iter()
            .take(levels)
            .map(|(&p, l)| (p, sum(l)))
            .collect();
        (bids, asks)
    }
}

impl Invariants for OrderBook {
    fn check(&self) -> Result<(), InvariantViolation> {
        // 1. The book is never crossed: matching must have consumed any
        //    overlap before orders rested.
        if let (Some(bid), Some(ask)) = (self.best_bid(), self.best_ask()) {
            if bid >= ask {
                return Err(InvariantViolation::new(format!(
                    "crossed book: best bid {bid} >= best ask {ask}"
                )));
            }
        }
        // 2. Share conservation: every accepted share is exactly one of
        //    traded (counted for both sides), canceled, or still resting.
        let resting = self.resting_shares();
        let accounted = 2 * self.traded_shares + self.canceled_shares + resting;
        if self.accepted_shares != accounted {
            return Err(InvariantViolation::new(format!(
                "share conservation broken: accepted {} != 2*traded {} + canceled {} + resting {resting}",
                self.accepted_shares, self.traded_shares, self.canceled_shares
            )));
        }
        // 3. Structural integrity: index ↔ book agree; no empty levels or
        //    zero-qty residents.
        let mut counted = 0usize;
        for (side, levels) in [(Side::Buy, &self.bids), (Side::Sell, &self.asks)] {
            for (&price, level) in levels {
                if level.is_empty() {
                    return Err(InvariantViolation::new(format!(
                        "empty level left at price {price}"
                    )));
                }
                for order in level {
                    if order.qty == 0 {
                        return Err(InvariantViolation::new(format!(
                            "zero-qty order {} resting at {price}",
                            order.id
                        )));
                    }
                    if self.index.get(&order.id) != Some(&(side, price)) {
                        return Err(InvariantViolation::new(format!(
                            "index out of sync for order {}",
                            order.id
                        )));
                    }
                    counted += 1;
                }
            }
        }
        if counted != self.index.len() {
            return Err(InvariantViolation::new(format!(
                "index has {} entries but book holds {counted} orders",
                self.index.len()
            )));
        }
        Ok(())
    }
}

/// Seeded workload for the simulator: small id space (forces duplicate
/// rejects), occasional zero qty (forces validation rejects), and a mix of
/// submits and cancels.
pub fn gen_cmd(rng: &mut ticktape_sim::Rng) -> Cmd {
    if rng.chance(1, 5) {
        Cmd::Cancel { id: rng.below(150) }
    } else {
        Cmd::Submit {
            id: rng.below(150),
            side: if rng.chance(1, 2) {
                Side::Buy
            } else {
                Side::Sell
            },
            price: 90 + rng.below(21) as Price, // tight band → lots of crossing
            qty: rng.below(50) as Qty,          // 0 included → rejects exercised
        }
    }
}
