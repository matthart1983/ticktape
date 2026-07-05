//! The "fixed" codec tier for Ticktape: canonical, little-endian,
//! declaration-order byte encoding, deterministic by construction.
//!
//! The [`Encode`]/[`Decode`] traits and primitive impls live in
//! `ticktape-core` (the orphan rule requires trait-owner-local impls); this
//! crate is the user-facing entry point, bundling the traits with the
//! `#[derive(Encode, Decode)]` macros:
//!
//! ```
//! use ticktape_codec::{decode_all, encode_to_vec, Decode, Encode};
//!
//! #[derive(Encode, Decode, PartialEq, Debug)]
//! struct Order {
//!     id: u64,
//!     side: Side,
//!     qty: u32,
//!     symbol: String,
//! }
//!
//! #[derive(Encode, Decode, PartialEq, Debug)]
//! enum Side { Buy, Sell }
//!
//! let order = Order { id: 7, side: Side::Sell, qty: 100, symbol: "ACME".into() };
//! let bytes = encode_to_vec(&order);
//! assert_eq!(decode_all::<Order>(&bytes).unwrap(), order);
//! ```
//!
//! Determinism rules enforced structurally: `HashMap`/`HashSet` and bare
//! `f32`/`f64` have no impls, so fields of those types fail to compile. Use
//! `BTreeMap` (as sorted `Vec<(K, V)>` snapshots) and integer/fixed-point
//! numerics. Enum variant order and struct field order are part of the wire
//! contract — append, never reorder.
//!
//! Planned adapters (rkyv zero-copy, FIX/SBE interop) will live here too.

// The trait and its derive macro share a name deliberately (as serde's
// Serialize does): traits and macros live in separate namespaces, so one
// `use ticktape_codec::Encode` brings in both.
pub use ticktape_core::codec::{decode_all, encode_to_vec, CodecError, Decode, Encode};
pub use ticktape_macros::{Decode, Encode};

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Encode, Decode, PartialEq, Debug, Clone)]
    struct Named {
        a: u32,
        b: String,
        c: Vec<u16>,
        d: Option<bool>,
    }

    #[derive(Encode, Decode, PartialEq, Debug)]
    struct Tuple(u8, String);

    #[derive(Encode, Decode, PartialEq, Debug)]
    struct Unit;

    #[derive(Encode, Decode, PartialEq, Debug)]
    enum Mixed {
        Nothing,
        One(u64),
        Pair { x: i32, y: i32 },
        Nested(Named),
    }

    // Generic types: the impl is bounded so `Wrap<T>` / `Env<T>` derive
    // whenever `T` is itself codable.
    #[derive(Encode, Decode, PartialEq, Debug)]
    struct Wrap<T> {
        tag: u16,
        value: T,
    }

    #[derive(Encode, Decode, PartialEq, Debug)]
    enum Env<T> {
        Empty,
        Full { id: u64, payload: T },
    }

    fn roundtrip<T: Encode + Decode + PartialEq + std::fmt::Debug>(value: T) {
        let bytes = encode_to_vec(&value);
        assert_eq!(bytes.len(), value.encoded_len(), "encoded_len mismatch");
        assert_eq!(decode_all::<T>(&bytes).unwrap(), value);
    }

    #[test]
    fn derived_struct_roundtrip() {
        roundtrip(Named {
            a: 1,
            b: "two".into(),
            c: vec![3, 4],
            d: Some(true),
        });
        roundtrip(Tuple(9, "t".into()));
        roundtrip(Unit);
    }

    #[test]
    fn derived_enum_roundtrip() {
        roundtrip(Mixed::Nothing);
        roundtrip(Mixed::One(u64::MAX));
        roundtrip(Mixed::Pair { x: -5, y: 5 });
        roundtrip(Mixed::Nested(Named {
            a: 0,
            b: String::new(),
            c: vec![],
            d: None,
        }));
    }

    #[test]
    fn derived_generic_roundtrip() {
        roundtrip(Wrap {
            tag: 7,
            value: String::from("hi"),
        });
        roundtrip(Wrap {
            tag: 1,
            value: vec![1u32, 2, 3],
        });
        roundtrip(Env::<u64>::Empty);
        roundtrip(Env::Full {
            id: 42,
            payload: Tuple(3, "x".into()),
        });
    }

    #[test]
    fn generic_bytes_match_the_monomorphized_layout() {
        // A generic derive must produce the same bytes as writing the fields
        // out by hand — the wire format is the type's layout, not its
        // genericity. Wrap<u32>{tag, value} = [tag u16][value u32].
        let bytes = encode_to_vec(&Wrap { tag: 0x0102u16, value: 0x0304_0506u32 });
        let mut expected = Vec::new();
        0x0102u16.encode(&mut expected);
        0x0304_0506u32.encode(&mut expected);
        assert_eq!(bytes, expected);
    }

    #[test]
    fn enum_discriminant_is_declaration_index() {
        let bytes = encode_to_vec(&Mixed::One(1));
        assert_eq!(
            &bytes[..2],
            &[1, 0],
            "discriminant must be u16 LE of variant index"
        );
    }

    #[test]
    fn unknown_discriminant_rejected() {
        let bytes = encode_to_vec(&99u16);
        assert!(matches!(
            decode_all::<Mixed>(&bytes),
            Err(CodecError::InvalidValue(_))
        ));
    }
}
