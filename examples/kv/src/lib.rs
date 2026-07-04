//! A durable, deterministic key-value store: the smallest end-to-end
//! Ticktape service with real state.
//!
//! Note the deliberate choices the determinism contract forces:
//! - `BTreeMap`, never `HashMap` (deterministic iteration order — the
//!   snapshot must be byte-identical on every replica).
//! - Reads (`Get`) are sequenced commands too: a read's answer is defined
//!   by its position in the total order, which is what makes it exactly
//!   reproducible on replay.

use std::collections::BTreeMap;
use ticktape::{Ctx, Decode, Encode, Seq, Service};
use ticktape_sim::{InvariantViolation, Invariants};

pub struct Kv {
    map: BTreeMap<String, String>,
}

#[derive(Encode, Decode, Debug, PartialEq)]
pub enum Cmd {
    Put { key: String, value: String },
    Del { key: String },
    Get { key: String },
}

#[derive(Encode, Decode, Debug, PartialEq)]
pub enum Evt {
    /// Applied a Put/Del; `true` if the key existed before.
    Ack { existed: bool },
    /// Answer to a Get at this point in the total order.
    Value(Option<String>),
}

impl Service for Kv {
    type Input = Cmd;
    type Output = Evt;
    type Snapshot = Vec<(String, String)>;
    type Config = ();

    fn genesis(_: &()) -> Self {
        Kv {
            map: BTreeMap::new(),
        }
    }

    fn apply(&mut self, _seq: Seq, cmd: &Cmd, ctx: &mut Ctx<'_, Evt>) {
        match cmd {
            Cmd::Put { key, value } => {
                let existed = self.map.insert(key.clone(), value.clone()).is_some();
                ctx.emit(Evt::Ack { existed });
            }
            Cmd::Del { key } => {
                let existed = self.map.remove(key).is_some();
                ctx.emit(Evt::Ack { existed });
            }
            Cmd::Get { key } => {
                ctx.emit(Evt::Value(self.map.get(key).cloned()));
            }
        }
    }

    fn snapshot(&self) -> Vec<(String, String)> {
        // BTreeMap iteration is sorted: canonical bytes by construction.
        self.map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn restore(snap: Vec<(String, String)>, _: &()) -> Self {
        Kv {
            map: snap.into_iter().collect(),
        }
    }
}

impl Invariants for Kv {
    fn check(&self) -> Result<(), InvariantViolation> {
        // BTreeMap should make these true by construction; checking them
        // through the snapshot exercises the same bytes replicas compare.
        let snap = self.snapshot();
        if !snap.windows(2).all(|w| w[0].0 < w[1].0) {
            return Err(InvariantViolation::new("snapshot keys not sorted/unique"));
        }
        if snap.len() != self.map.len() {
            return Err(InvariantViolation::new("snapshot length != map length"));
        }
        Ok(())
    }
}

impl Kv {
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.map.get(key).map(String::as_str)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.map.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
}
