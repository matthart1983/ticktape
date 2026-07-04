//! The exchange: the order book wrapped in session awareness — ownership,
//! per-session dedup, cancel-on-disconnect, and event addressing — as one
//! ordinary deterministic `Service`.
//!
//! Everything session-flavored lives *inside* the state machine, which
//! means it is journaled, replicated, replayable, and fuzzable like any
//! other input: reconnecting clients keep their dedup state because it IS
//! state; cancel-on-disconnect happens identically on every replica
//! because the disconnect is a sequenced input.

use orderbook::{Cmd, Evt, OrderBook, OrderId, Reject};
use std::collections::{BTreeMap, BTreeSet};
use ticktape::{decode_all, encode_to_vec, Ctx, OutBuf, Seq, Service};
use ticktape_gateway::{Addressed, GatewayInput};
use ticktape_sim::{InvariantViolation, Invariants};

pub type Session = u64;

pub struct Exchange {
    book: OrderBook,
    /// Owner of every currently-resting order.
    owners: BTreeMap<OrderId, Session>,
    /// Reverse index for cancel-on-disconnect.
    owned: BTreeMap<Session, BTreeSet<OrderId>>,
    /// Highest client seq applied per session — the deterministic half of
    /// dedup (the gateway's `SessionFlow` is the edge half; this guard
    /// makes duplicates harmless even if one slips past the edge).
    last_client_seq: BTreeMap<Session, u64>,
}

impl Service for Exchange {
    type Input = GatewayInput<Cmd>;
    type Output = Addressed<Evt>;
    /// Book snapshot + ownership + dedup state, all canonical bytes.
    type Snapshot = (
        Vec<u8>, // encoded orderbook::BookSnapshot
        Vec<(OrderId, Session)>,
        Vec<(Session, u64)>,
    );
    type Config = ();

    fn genesis(_: &()) -> Self {
        Exchange {
            book: OrderBook::genesis(&()),
            owners: BTreeMap::new(),
            owned: BTreeMap::new(),
            last_client_seq: BTreeMap::new(),
        }
    }

    fn apply(&mut self, seq: Seq, input: &Self::Input, ctx: &mut Ctx<'_, Self::Output>) {
        match input {
            GatewayInput::Client {
                session,
                client_seq,
                cmd,
            } => self.apply_client(seq, *session, *client_seq, cmd, ctx),
            GatewayInput::SessionClosed { session } => self.close_session(seq, *session, ctx),
        }
    }

    fn snapshot(&self) -> Self::Snapshot {
        (
            encode_to_vec(&self.book.snapshot()),
            self.owners.iter().map(|(&k, &v)| (k, v)).collect(),
            self.last_client_seq.iter().map(|(&k, &v)| (k, v)).collect(),
        )
    }

    fn restore((book, owners, last): Self::Snapshot, _: &()) -> Self {
        let book = OrderBook::restore(decode_all(&book).expect("valid book snapshot"), &());
        let owners: BTreeMap<OrderId, Session> = owners.into_iter().collect();
        let mut owned: BTreeMap<Session, BTreeSet<OrderId>> = BTreeMap::new();
        for (&id, &session) in &owners {
            owned.entry(session).or_default().insert(id);
        }
        Exchange {
            book,
            owners,
            owned,
            last_client_seq: last.into_iter().collect(),
        }
    }
}

impl Exchange {
    fn apply_client(
        &mut self,
        seq: Seq,
        session: Session,
        client_seq: u64,
        cmd: &Cmd,
        ctx: &mut Ctx<'_, Addressed<Evt>>,
    ) {
        // Deterministic dedup: a duplicate that slipped past the gateway
        // (or is being replayed) has no effect, exactly once means once.
        let last = self.last_client_seq.entry(session).or_insert(0);
        if client_seq <= *last {
            return;
        }
        *last = client_seq;

        // Only the owner of a resting order may cancel it; strangers learn
        // nothing beyond "unknown".
        if let Cmd::Cancel { id } = cmd {
            if self.owners.get(id) != Some(&session) {
                ctx.emit(Addressed {
                    session,
                    event: Evt::Rejected {
                        id: *id,
                        reason: Reject::UnknownOrder,
                    },
                });
                return;
            }
        }

        let events = self.run_book(seq, cmd, ctx.now());
        for event in events {
            self.route(session, event, ctx);
        }
    }

    /// Cancel-on-disconnect: pull every resting order the session owns, in
    /// deterministic (sorted) order.
    fn close_session(&mut self, seq: Seq, session: Session, ctx: &mut Ctx<'_, Addressed<Evt>>) {
        let ids: Vec<OrderId> = self
            .owned
            .get(&session)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        for id in ids {
            let events = self.run_book(seq, &Cmd::Cancel { id }, ctx.now());
            for event in events {
                // The owner is gone; drop-copy observers still see these.
                self.route(session, event, ctx);
            }
        }
        self.owned.remove(&session);
        // last_client_seq intentionally survives: a reconnecting session
        // resumes its dedup state.
    }

    /// Drive the inner book and reconcile ownership afterwards.
    fn run_book(&mut self, seq: Seq, cmd: &Cmd, now: ticktape::Timestamp) -> Vec<Evt> {
        let mut out = OutBuf::new();
        let mut ctx = Ctx::new(seq, now, &mut out);
        self.book.apply(seq, cmd, &mut ctx);
        out.drain()
    }

    /// Address one book event to every interested session and keep the
    /// ownership indexes in sync with residency.
    fn route(&mut self, origin: Session, event: Evt, ctx: &mut Ctx<'_, Addressed<Evt>>) {
        match event {
            Evt::Accepted { id } => {
                if self.book.has_order(id) {
                    self.owners.insert(id, origin);
                    self.owned.entry(origin).or_default().insert(id);
                }
                ctx.emit(Addressed {
                    session: origin,
                    event,
                });
            }
            Evt::Trade { maker, .. } => {
                let maker_session = self.owners.get(&maker).copied();
                if !self.book.has_order(maker) {
                    if let Some(owner) = maker_session {
                        self.owners.remove(&maker);
                        if let Some(set) = self.owned.get_mut(&owner) {
                            set.remove(&maker);
                        }
                    }
                }
                ctx.emit(Addressed {
                    session: origin,
                    event: event.clone(),
                });
                match maker_session {
                    Some(owner) if owner != origin => ctx.emit(Addressed {
                        session: owner,
                        event,
                    }),
                    _ => {}
                }
            }
            Evt::Canceled { id, .. } => {
                if let Some(owner) = self.owners.remove(&id) {
                    if let Some(set) = self.owned.get_mut(&owner) {
                        set.remove(&id);
                    }
                }
                ctx.emit(Addressed {
                    session: origin,
                    event,
                });
            }
            Evt::Rejected { .. } => ctx.emit(Addressed {
                session: origin,
                event,
            }),
        }
    }

    pub fn book(&self) -> &OrderBook {
        &self.book
    }

    /// `(session, last_client_seq)` pairs for `ServeConfig::resume` — how a
    /// restarted gateway reseeds dedup from recovered state.
    pub fn sessions(&self) -> Vec<(Session, u64)> {
        self.last_client_seq.iter().map(|(&s, &n)| (s, n)).collect()
    }
}

impl Invariants for Exchange {
    fn check(&self) -> Result<(), InvariantViolation> {
        self.book.check()?;
        // Ownership ↔ residency: every owned order rests; every resting
        // order has exactly one owner.
        for (&id, &owner) in &self.owners {
            if !self.book.has_order(id) {
                return Err(InvariantViolation::new(format!(
                    "owner map holds departed order {id}"
                )));
            }
            if !self.owned.get(&owner).is_some_and(|set| set.contains(&id)) {
                return Err(InvariantViolation::new(format!(
                    "reverse ownership index missing order {id}"
                )));
            }
        }
        if self.owners.len() != self.book.resting_orders() {
            return Err(InvariantViolation::new(format!(
                "{} owners for {} resting orders",
                self.owners.len(),
                self.book.resting_orders()
            )));
        }
        let reverse_total: usize = self.owned.values().map(BTreeSet::len).sum();
        if reverse_total != self.owners.len() {
            return Err(InvariantViolation::new(
                "reverse ownership index out of sync",
            ));
        }
        Ok(())
    }
}
