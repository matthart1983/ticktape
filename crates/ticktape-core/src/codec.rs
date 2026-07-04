//! Canonical byte encoding.
//!
//! The determinism invariant requires that the same logical value encodes
//! to identical bytes on every node — otherwise replicas diverge and CRCs
//! mismatch. These traits define the "fixed" codec tier: little-endian,
//! declaration-order, length-prefixed variable data, no floats, no
//! nondeterministically-ordered collections.
//!
//! `Decode` is cursor-style: it takes `&mut &[u8]` and advances past what it
//! consumed, so decoders compose field-by-field. (The draft spec sketched
//! `decode(buf: &[u8])`; that signature cannot compose sequential fields, so
//! the cursor form is canonical.) Use [`decode_all`] at the top level to
//! also reject trailing bytes.
//!
//! Deliberately **not** implemented: `f32`/`f64` (NaN/-0.0 canonicalization
//! is a later, opt-in feature), `HashMap`/`HashSet` (nondeterministic
//! iteration order). Their absence is the compile-time enforcement.

use core::fmt;

/// Errors from decoding canonical bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// Input ended before the value was complete.
    UnexpectedEof,
    /// A length prefix or enum discriminant was out of range.
    InvalidValue(&'static str),
    /// A `String` field held invalid UTF-8.
    InvalidUtf8,
    /// `decode_all` finished with bytes left over.
    TrailingBytes(usize),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::UnexpectedEof => write!(f, "unexpected end of input"),
            CodecError::InvalidValue(what) => write!(f, "invalid encoded value: {what}"),
            CodecError::InvalidUtf8 => write!(f, "invalid UTF-8 in string"),
            CodecError::TrailingBytes(n) => write!(f, "{n} trailing bytes after decode"),
        }
    }
}

impl std::error::Error for CodecError {}

/// Encode `self` as canonical bytes: identical logical value ⇒ identical
/// bytes, on every node, every time.
pub trait Encode {
    fn encode(&self, out: &mut Vec<u8>);

    fn encoded_len(&self) -> usize;
}

/// Decode a value from the front of `buf`, advancing it past the bytes
/// consumed.
pub trait Decode: Sized {
    fn decode(buf: &mut &[u8]) -> Result<Self, CodecError>;
}

/// Encode a value into a fresh, exactly-sized buffer.
pub fn encode_to_vec<T: Encode>(value: &T) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.encoded_len());
    value.encode(&mut out);
    out
}

/// Decode a value that must consume the entire buffer.
pub fn decode_all<T: Decode>(mut buf: &[u8]) -> Result<T, CodecError> {
    let value = T::decode(&mut buf)?;
    if buf.is_empty() {
        Ok(value)
    } else {
        Err(CodecError::TrailingBytes(buf.len()))
    }
}

fn take<'a>(buf: &mut &'a [u8], n: usize) -> Result<&'a [u8], CodecError> {
    if buf.len() < n {
        return Err(CodecError::UnexpectedEof);
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    Ok(head)
}

macro_rules! impl_int {
    ($($ty:ty),*) => {$(
        impl Encode for $ty {
            fn encode(&self, out: &mut Vec<u8>) {
                out.extend_from_slice(&self.to_le_bytes());
            }
            fn encoded_len(&self) -> usize {
                core::mem::size_of::<$ty>()
            }
        }
        impl Decode for $ty {
            fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
                let bytes = take(buf, core::mem::size_of::<$ty>())?;
                Ok(<$ty>::from_le_bytes(bytes.try_into().unwrap()))
            }
        }
    )*};
}

impl_int!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);

impl Encode for bool {
    fn encode(&self, out: &mut Vec<u8>) {
        out.push(*self as u8);
    }
    fn encoded_len(&self) -> usize {
        1
    }
}

impl Decode for bool {
    fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        match u8::decode(buf)? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(CodecError::InvalidValue("bool tag")),
        }
    }
}

impl Encode for () {
    fn encode(&self, _out: &mut Vec<u8>) {}
    fn encoded_len(&self) -> usize {
        0
    }
}

impl Decode for () {
    fn decode(_buf: &mut &[u8]) -> Result<Self, CodecError> {
        Ok(())
    }
}

fn encode_len(len: usize, out: &mut Vec<u8>) {
    debug_assert!(len <= u32::MAX as usize, "collection too large to encode");
    (len as u32).encode(out);
}

fn decode_len(buf: &mut &[u8]) -> Result<usize, CodecError> {
    Ok(u32::decode(buf)? as usize)
}

impl Encode for String {
    fn encode(&self, out: &mut Vec<u8>) {
        encode_len(self.len(), out);
        out.extend_from_slice(self.as_bytes());
    }
    fn encoded_len(&self) -> usize {
        4 + self.len()
    }
}

impl Decode for String {
    fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        let len = decode_len(buf)?;
        let bytes = take(buf, len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| CodecError::InvalidUtf8)
    }
}

impl<T: Encode> Encode for Vec<T> {
    fn encode(&self, out: &mut Vec<u8>) {
        encode_len(self.len(), out);
        for item in self {
            item.encode(out);
        }
    }
    fn encoded_len(&self) -> usize {
        4 + self.iter().map(Encode::encoded_len).sum::<usize>()
    }
}

impl<T: Decode> Decode for Vec<T> {
    fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        let len = decode_len(buf)?;
        // Guard capacity by remaining input so a corrupt length prefix
        // cannot trigger a huge allocation before hitting EOF.
        let mut items = Vec::with_capacity(len.min(buf.len()));
        for _ in 0..len {
            items.push(T::decode(buf)?);
        }
        Ok(items)
    }
}

impl<T: Encode> Encode for Option<T> {
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            None => out.push(0),
            Some(v) => {
                out.push(1);
                v.encode(out);
            }
        }
    }
    fn encoded_len(&self) -> usize {
        1 + self.as_ref().map_or(0, Encode::encoded_len)
    }
}

impl<T: Decode> Decode for Option<T> {
    fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        match u8::decode(buf)? {
            0 => Ok(None),
            1 => Ok(Some(T::decode(buf)?)),
            _ => Err(CodecError::InvalidValue("Option tag")),
        }
    }
}

macro_rules! impl_tuple {
    ($($name:ident : $idx:tt),+) => {
        impl<$($name: Encode),+> Encode for ($($name,)+) {
            fn encode(&self, out: &mut Vec<u8>) {
                $( self.$idx.encode(out); )+
            }
            fn encoded_len(&self) -> usize {
                0 $( + self.$idx.encoded_len() )+
            }
        }
        impl<$($name: Decode),+> Decode for ($($name,)+) {
            fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
                Ok(( $( $name::decode(buf)?, )+ ))
            }
        }
    };
}

impl_tuple!(A: 0);
impl_tuple!(A: 0, B: 1);
impl_tuple!(A: 0, B: 1, C: 2);
impl_tuple!(A: 0, B: 1, C: 2, D: 3);

impl Encode for crate::Seq {
    fn encode(&self, out: &mut Vec<u8>) {
        self.0.encode(out);
    }
    fn encoded_len(&self) -> usize {
        8
    }
}

impl Decode for crate::Seq {
    fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        Ok(crate::Seq(u64::decode(buf)?))
    }
}

impl Encode for crate::Timestamp {
    fn encode(&self, out: &mut Vec<u8>) {
        self.0.encode(out);
    }
    fn encoded_len(&self) -> usize {
        8
    }
}

impl Decode for crate::Timestamp {
    fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        Ok(crate::Timestamp(u64::decode(buf)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip<T: Encode + Decode + PartialEq + core::fmt::Debug>(value: T) {
        let bytes = encode_to_vec(&value);
        assert_eq!(bytes.len(), value.encoded_len(), "encoded_len mismatch");
        let back: T = decode_all(&bytes).expect("decode");
        assert_eq!(back, value);
    }

    #[test]
    fn primitives_roundtrip() {
        roundtrip(0xDEAD_BEEFu32);
        roundtrip(-42i64);
        roundtrip(true);
        roundtrip(u128::MAX);
        roundtrip(String::from("héllo"));
        roundtrip(vec![1u16, 2, 3]);
        roundtrip(Some(7u8));
        roundtrip(Option::<u8>::None);
        roundtrip((1u8, String::from("k"), vec![9u64]));
        roundtrip(crate::Seq(17));
        roundtrip(crate::Timestamp(1_700_000_000_000_000_000));
    }

    #[test]
    fn canonical_bytes_are_stable() {
        // Little-endian, declaration order: this exact byte string is the
        // wire contract; changing it is a format break.
        let bytes = encode_to_vec(&(0x0102u16, String::from("ab")));
        assert_eq!(bytes, [0x02, 0x01, 2, 0, 0, 0, b'a', b'b']);
    }

    #[test]
    fn decode_rejects_trailing_and_eof() {
        assert_eq!(
            decode_all::<u32>(&[1, 2, 3]).unwrap_err(),
            CodecError::UnexpectedEof
        );
        assert_eq!(
            decode_all::<u16>(&[1, 2, 3]).unwrap_err(),
            CodecError::TrailingBytes(1)
        );
    }

    #[test]
    fn corrupt_length_prefix_is_eof_not_alloc() {
        // u32::MAX length prefix with 2 bytes of data must error, not OOM.
        let mut bytes = encode_to_vec(&u32::MAX);
        bytes.extend_from_slice(b"ab");
        assert_eq!(
            decode_all::<Vec<u8>>(&bytes).unwrap_err(),
            CodecError::UnexpectedEof
        );
    }
}
