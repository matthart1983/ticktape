//! SBE (Simple Binary Encoding) interop adapter — spec §6 serialization
//! tier 3.
//!
//! Practitioner platforms in this space (FIX SBE, Aeron, jimgreco/core) put
//! **SBE** on the wire: fixed-layout little-endian message blocks prefixed by
//! a standard 8-byte *message header* (`blockLength`, `templateId`,
//! `schemaId`, `version`). This crate lets a Ticktape service speak that wire
//! without abandoning the canonical `fixed` tier for everything else:
//!
//! - [`SbeHeader`] is the exact SBE message header (SimpleOpenFramingHeader
//!   is *not* included — that framing belongs to the transport, which
//!   Ticktape already provides via its packet/frame layer).
//! - [`SbeMessage`] is what an application message type implements: its
//!   template/schema ids, its root-block layout, and how to read/write that
//!   block. Groups and variable-length data are the app's business inside
//!   `encode_block`/`decode_block`; this crate owns the header and the
//!   version-tolerant framing.
//! - [`Sbe<T>`] is the **adapter**: it implements Ticktape's own
//!   [`ticktape_core::Encode`]/[`Decode`], so `Service::Input = Sbe<MyMsg>`
//!   puts SBE-framed bytes straight into the journal and onto the sequenced
//!   stream — interoperable with SBE tooling, deterministic like any other
//!   fixed-layout payload.
//!
//! **Schema evolution** is SBE's whole point and is handled here: a decoder
//! reads the sender's `blockLength` and, if the sender wrote a *longer* block
//! (fields appended in a newer schema version), skips the excess after
//! decoding the fields it knows — so an old reader consumes a new writer's
//! stream. A shorter block (older writer) is rejected as truncated.

use ticktape_core::{CodecError, Decode, Encode};

/// The SBE message header (8 bytes, little-endian): the self-describing
/// prefix every SBE message carries.
///
/// ```text
///   0   2   block_length  u16   bytes in the fixed root block
///   2   2   template_id   u16   which message
///   4   2   schema_id     u16   which schema the template belongs to
///   6   2   version       u16   schema version the writer used
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SbeHeader {
    pub block_length: u16,
    pub template_id: u16,
    pub schema_id: u16,
    pub version: u16,
}

/// Header size in bytes.
pub const HEADER_LEN: usize = 8;

impl SbeHeader {
    pub fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.block_length.to_le_bytes());
        out.extend_from_slice(&self.template_id.to_le_bytes());
        out.extend_from_slice(&self.schema_id.to_le_bytes());
        out.extend_from_slice(&self.version.to_le_bytes());
    }

    pub fn read(buf: &[u8]) -> Result<SbeHeader, CodecError> {
        if buf.len() < HEADER_LEN {
            return Err(CodecError::UnexpectedEof);
        }
        Ok(SbeHeader {
            block_length: u16::from_le_bytes(buf[0..2].try_into().unwrap()),
            template_id: u16::from_le_bytes(buf[2..4].try_into().unwrap()),
            schema_id: u16::from_le_bytes(buf[4..6].try_into().unwrap()),
            version: u16::from_le_bytes(buf[6..8].try_into().unwrap()),
        })
    }
}

/// An application message that can be carried on the SBE wire.
///
/// Implementors declare their identity (`TEMPLATE_ID`, `SCHEMA_ID`,
/// `SCHEMA_VERSION`) and the length of their fixed root block
/// (`BLOCK_LENGTH`), and provide the block codec. `encode_block` must write
/// exactly `BLOCK_LENGTH` bytes of fixed fields (in SBE's declared field
/// order, little-endian); any variable-length tail (groups, var-data) is
/// appended by the implementor *after* returning from `encode_block` only if
/// they override [`SbeMessage::encode_var`]. Most messages are pure fixed
/// blocks and need only the block methods.
pub trait SbeMessage: Sized {
    const TEMPLATE_ID: u16;
    const SCHEMA_ID: u16;
    const SCHEMA_VERSION: u16;
    /// Bytes in the fixed root block this build writes.
    const BLOCK_LENGTH: u16;

    /// Write the fixed root block — exactly `BLOCK_LENGTH` bytes.
    fn encode_block(&self, out: &mut Vec<u8>);

    /// Decode the fixed root block from exactly this build's `BLOCK_LENGTH`
    /// bytes (the framing has already trimmed a longer sender block and
    /// verified a shorter one is not truncated).
    fn decode_block(block: &[u8]) -> Result<Self, CodecError>;
}

/// Frame an [`SbeMessage`] as `header ++ block`. The header's `block_length`
/// is this build's `BLOCK_LENGTH`.
pub fn encode_message<M: SbeMessage>(msg: &M, out: &mut Vec<u8>) {
    SbeHeader {
        block_length: M::BLOCK_LENGTH,
        template_id: M::TEMPLATE_ID,
        schema_id: M::SCHEMA_ID,
        version: M::SCHEMA_VERSION,
    }
    .write(out);
    let before = out.len();
    msg.encode_block(out);
    debug_assert_eq!(
        (out.len() - before) as u16,
        M::BLOCK_LENGTH,
        "encode_block wrote {} bytes, BLOCK_LENGTH is {}",
        out.len() - before,
        M::BLOCK_LENGTH
    );
}

/// Decode an [`SbeMessage`] from `buf`, returning the message and the total
/// bytes consumed (header + the sender's advertised block, so a caller can
/// advance past appended variable-length data it does not understand).
///
/// Version tolerance: if the sender's `block_length` exceeds this build's
/// `BLOCK_LENGTH`, the extra trailing block bytes (newer fields) are skipped
/// after decoding the known prefix. A shorter block, or a template/schema
/// mismatch, is an error.
pub fn decode_message<M: SbeMessage>(buf: &[u8]) -> Result<(M, usize), CodecError> {
    let header = SbeHeader::read(buf)?;
    if header.template_id != M::TEMPLATE_ID || header.schema_id != M::SCHEMA_ID {
        return Err(CodecError::InvalidValue("SBE template/schema mismatch"));
    }
    let sender_block = header.block_length as usize;
    let ours = M::BLOCK_LENGTH as usize;
    if sender_block < ours {
        // Older writer: it did not include fields this build requires.
        return Err(CodecError::InvalidValue("SBE block shorter than schema"));
    }
    if buf.len() < HEADER_LEN + sender_block {
        return Err(CodecError::UnexpectedEof);
    }
    // Decode only the fields we know; the rest of the sender's block is a
    // newer-version tail we skip.
    let block = &buf[HEADER_LEN..HEADER_LEN + ours];
    let msg = M::decode_block(block)?;
    Ok((msg, HEADER_LEN + sender_block))
}

/// The adapter: wraps an [`SbeMessage`] so it flows through Ticktape's
/// canonical [`Encode`]/[`Decode`] as SBE-framed bytes. Use it as a service's
/// `Input`/`Output`/`Snapshot` element to keep SBE on the wire while the rest
/// of the system treats it like any other codable value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sbe<T>(pub T);

impl<T: SbeMessage> Encode for Sbe<T> {
    fn encode(&self, out: &mut Vec<u8>) {
        encode_message(&self.0, out);
    }
    fn encoded_len(&self) -> usize {
        HEADER_LEN + T::BLOCK_LENGTH as usize
    }
}

impl<T: SbeMessage> Decode for Sbe<T> {
    fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        let (msg, consumed) = decode_message::<T>(buf)?;
        *buf = &buf[consumed..];
        Ok(Sbe(msg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ticktape_core::{decode_all, encode_to_vec};

    /// A NewOrderSingle-shaped message: a 22-byte fixed block.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct NewOrder {
        order_id: u64,
        price: i64,
        qty: u32,
        side: u8,
        ord_type: u8,
    }

    impl SbeMessage for NewOrder {
        const TEMPLATE_ID: u16 = 1;
        const SCHEMA_ID: u16 = 91;
        const SCHEMA_VERSION: u16 = 0;
        const BLOCK_LENGTH: u16 = 8 + 8 + 4 + 1 + 1; // 22

        fn encode_block(&self, out: &mut Vec<u8>) {
            out.extend_from_slice(&self.order_id.to_le_bytes());
            out.extend_from_slice(&self.price.to_le_bytes());
            out.extend_from_slice(&self.qty.to_le_bytes());
            out.push(self.side);
            out.push(self.ord_type);
        }

        fn decode_block(b: &[u8]) -> Result<Self, CodecError> {
            Ok(NewOrder {
                order_id: u64::from_le_bytes(b[0..8].try_into().unwrap()),
                price: i64::from_le_bytes(b[8..16].try_into().unwrap()),
                qty: u32::from_le_bytes(b[16..20].try_into().unwrap()),
                side: b[20],
                ord_type: b[21],
            })
        }
    }

    fn sample() -> NewOrder {
        NewOrder {
            order_id: 0xABCD_1234_5678,
            price: -125,
            qty: 500,
            side: 1,
            ord_type: 2,
        }
    }

    #[test]
    fn header_is_eight_le_bytes_in_sbe_order() {
        let mut out = Vec::new();
        encode_message(&sample(), &mut out);
        // block_length=22, template=1, schema=91, version=0 — all u16 LE.
        assert_eq!(&out[0..2], &22u16.to_le_bytes());
        assert_eq!(&out[2..4], &1u16.to_le_bytes());
        assert_eq!(&out[4..6], &91u16.to_le_bytes());
        assert_eq!(&out[6..8], &0u16.to_le_bytes());
        assert_eq!(out.len(), HEADER_LEN + 22);
    }

    #[test]
    fn message_round_trips() {
        let mut out = Vec::new();
        encode_message(&sample(), &mut out);
        let (back, consumed) = decode_message::<NewOrder>(&out).unwrap();
        assert_eq!(back, sample());
        assert_eq!(consumed, out.len());
    }

    #[test]
    fn adapter_flows_through_ticktape_codec() {
        // Sbe<NewOrder> encodes/decodes via the canonical Encode/Decode, so it
        // can be a Service Input and land in the journal SBE-framed.
        let value = Sbe(sample());
        let bytes = encode_to_vec(&value);
        assert_eq!(bytes.len(), value.encoded_len());
        assert_eq!(decode_all::<Sbe<NewOrder>>(&bytes).unwrap(), value);
    }

    #[test]
    fn newer_writer_with_a_longer_block_is_read_by_an_older_decoder() {
        // Simulate a v1 writer that appended a 4-byte field: block_length is
        // larger, and the extra bytes trail the known fields. An old decoder
        // (BLOCK_LENGTH=22) must read the known prefix and skip the rest.
        let mut out = Vec::new();
        SbeHeader {
            block_length: 22 + 4,
            template_id: 1,
            schema_id: 91,
            version: 1,
        }
        .write(&mut out);
        sample().encode_block(&mut out);
        out.extend_from_slice(&0x0102_0304u32.to_le_bytes()); // the new field
        let (back, consumed) = decode_message::<NewOrder>(&out).unwrap();
        assert_eq!(back, sample());
        assert_eq!(consumed, HEADER_LEN + 26, "must consume the full advertised block");
    }

    #[test]
    fn shorter_block_and_wrong_template_are_rejected() {
        // Older writer missing fields.
        let mut short = Vec::new();
        SbeHeader {
            block_length: 10,
            template_id: 1,
            schema_id: 91,
            version: 0,
        }
        .write(&mut short);
        short.extend_from_slice(&[0u8; 10]);
        assert!(decode_message::<NewOrder>(&short).is_err());

        // Right schema, wrong template.
        let mut wrong = Vec::new();
        encode_message(&sample(), &mut wrong);
        wrong[2..4].copy_from_slice(&999u16.to_le_bytes());
        assert!(decode_message::<NewOrder>(&wrong).is_err());
    }
}
