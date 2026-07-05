# Ticktape wire format

This is the authoritative byte-level specification of every persisted and
transmitted structure in Ticktape: the **frame** (the atom of the sequenced
stream and the journal), the **journal segment** and **snapshot** files, and
the **packet** / **retransmit-request** transport envelopes. It is
language-neutral by construction — a non-Rust node (a C++ matching engine, a
Python auditor) can produce or consume the stream by following this document
alone.

Conventions:

- **All integers are little-endian**, unsigned unless noted.
- **All checksums are CRC32C** (Castagnoli, polynomial `0x1EDC6F41`, reflected
  input/output, initial value `0xFFFFFFFF`, final XOR `0xFFFFFFFF`). This is
  the standard iSCSI/SSE4.2/ARMv8 CRC32C; `crc32c("123456789") == 0xE3069283`.
- Offsets in the tables are byte offsets from the start of the structure.
- `FORMAT_VERSION` is currently `1` for segments and snapshots.

---

## 1. Frame

The unit of the total order. Every journaled record and every transported
record is a frame. Layout: a 28-byte header, then the payload, then a 4-byte
payload checksum.

```
off  size  field         type   notes
  0     8   seq           u64    gapless sequence number, 1-based
  8     8   timestamp     u64    sequencer-assigned nanos (deterministic time)
 16     2   stream_id     u16    logical stream/topic
 18     2   kind          u16    FrameKind (see below)
 20     4   payload_len   u32    length of payload in bytes (<= 64 MiB)
 24     4   header_crc    u32    CRC32C of bytes [0, 24)
 28   var   payload       bytes  payload_len bytes; meaning depends on kind
 28+n   4   payload_crc   u32    CRC32C of the payload bytes
```

Total encoded size = `28 + payload_len + 4`.

A reader validates `header_crc` before trusting `payload_len` (so a corrupt
length can't drive a wild read), then validates `payload_crc` before trusting
the payload. `payload_len > 0x0400_0000` (64 MiB) is treated as corruption,
not an allocation request.

### FrameKind values

| Value    | Name         | Payload |
|----------|--------------|---------|
| `0x0001` | Input        | encoded application `Input` (see §6) |
| `0x0002` | Output       | encoded application `Output` |
| `0x0010` | Tick         | empty; `timestamp` is authoritative |
| `0x0013` | TimerFired   | `u64` timer id |
| `0x0011` | SessionOpen  | gateway session lifecycle |
| `0x0012` | SessionClose | gateway session lifecycle |
| `0x0020` | SnapshotMark | `u64` seq at which a snapshot was written |
| `0x0030` | EpochChange  | `(u64 epoch, u64 first_seq)` fence (see §6 tuple rules) |
| `0x00FF` | Heartbeat    | liveness + high-water (transport only) |

Only `Input` and `TimerFired` frames drive the application state machine on
replay; the rest advance the sequence and time or carry control metadata.

---

## 2. Journal segment file (`{first_seq:020}.seg`)

An append-only segment: a 28-byte header followed by back-to-back frames (§1)
in strictly increasing, gapless seq order. Segments roll at a configurable
size; the first frame's seq names the file.

```
off  size  field           type   notes
  0     4   magic           bytes  "TKTJ"
  4     4   format_version  u32    = 1
  8     8   first_seq       u64    seq of the first frame in this segment
 16     8   epoch           u64    leadership epoch that wrote it
 24     4   header_crc      u32    CRC32C of bytes [0, 24)
 28   var   frames          ...    concatenated frames (§1), gapless
```

A torn tail (a partially-written final frame after a crash) is detected via
the frame CRCs and truncated to the last intact frame on recovery. Corruption
in a non-final segment is a hard error. Compaction deletes whole segments
whose frames are all covered by a retained snapshot; the surviving journal
therefore need not begin at seq 1 (`first_seq` says where it begins).

---

## 3. Snapshot file (`{seq:020}.snap`)

A serialized state-machine snapshot at a deterministic seq, enabling
`restore(snapshot) + replay(tail)` fast recovery. Because a snapshot is always
taken at a specific seq (never "now"), every replica's snapshot at seq *k* is
byte-identical.

```
off  size  field           type   notes
  0     4   magic           bytes  "TKTS"
  4     4   format_version  u32    = 1
  8     8   seq             u64    the seq this snapshot captures
 16     8   epoch           u64    leadership epoch
 20 ->20    (see note)
 24     4   payload_len     u32    length of the payload
 28     4   header_crc      u32    CRC32C of bytes [0, 28)
 32   var   payload         bytes  encoded Service::Snapshot (§6)
 32+n   4   payload_crc     u32    CRC32C of the payload
```

Header is 32 bytes: `magic(4) + version(4) + seq(8) + epoch(8) +
payload_len(4) + header_crc(4)`. A corrupt or torn snapshot is skipped in
favor of an older retained one, or full replay — snapshots are an
optimization, never the system of record.

---

## 4. Transport packet (UDP / Unix-datagram / shm)

One datagram on the sequenced feed. A 27-byte header, then (for data packets)
seq-contiguous frames.

```
off  size  field         type   notes
  0     4   magic         bytes  "TKTW"
  4     1   kind          u8     0 = Data, 1 = Heartbeat
  5     2   count         u16    frame count (0 for Heartbeat)
  7     8   session       u64    stream session id
 15     8   first_seq     u64    Data: frames[0].seq; Heartbeat: next_seq
 23     4   header_crc    u32    CRC32C of bytes [0, 23)
 27   var   frames        ...    Data only: `count` frames (§1), seq-contiguous
```

- **Data**: carries `count` frames whose seqs are `first_seq, first_seq+1, …`.
  A receiver rejects a packet whose frames are not contiguous from `first_seq`.
  The publisher packs as many frames as fit under `MAX_PACKET_BYTES` (1400);
  an oversized single frame is sent alone and may IP-fragment.
- **Heartbeat**: no frames; `first_seq` carries the publisher's *next* seq, so
  a receiver can detect and gap-fill trailing loss even when no new data flows.

---

## 5. Retransmit request (TCP) and reply

A gap-fill request over a short-lived TCP connection to a leader's
retransmitter. Fixed 28 bytes:

```
off  size  field      type   notes
  0     4   magic      bytes  "TKTR"
  4     8   session    u64    stream session id
 12     8   from       u64    first missing seq
 20     4   count      u32    number of frames requested
 24     4   crc        u32    CRC32C of bytes [0, 24)
```

**Reply** (server → client, then the server closes the connection): a stream
of length-prefixed data packets — repeated `[len: u32][packet: §4 Data]`,
where `len` is the packet's byte length. The client reads to EOF and ingests
each packet. An empty reply means the range is unavailable.

---

## 6. Canonical value codec (`fixed` tier)

Application `Input`, `Output`, and `Snapshot` values inside frame/snapshot
payloads use the canonical **fixed** encoding — chosen so identical values
produce identical bytes on every machine (a prerequisite for byte-comparable
replicas):

- **Integers** (`u8`/`u16`/`u32`/`u64`/`i*`): little-endian, fixed width.
- **`bool`**: one byte, `0` or `1`.
- **`()`** (unit): zero bytes.
- **`String`**: `u32` byte-length prefix, then UTF-8 bytes.
- **`Vec<T>`**: `u32` element-count prefix, then each element encoded in order.
- **`Option<T>`**: one tag byte — `0` = None (nothing follows), `1` = Some
  (the value follows).
- **Tuples** `(A, B, …)`: each field encoded in declaration order, no padding.
- **Structs**: fields in declaration order.
- **Enums**: a `u16` discriminant (variant index in declaration order), then
  the variant's fields in order.

Deliberately **not** encodable (no `Encode`/`Decode` impl), because they would
break determinism: `HashMap`/`HashSet` (nondeterministic iteration order) and
bare floats (`NaN`, `-0.0`). Use `BTreeMap`/`BTreeSet` and integer/fixed-point
numerics.

Two other serialization tiers are defined by the spec but not required to read
the stream: a self-describing reflective tier and an SBE-interop tier; both
map onto the same frame envelope (§1) — only the payload bytes differ.

---

## Versioning

Segment and snapshot files carry an explicit `format_version`. The frame,
packet, and request headers are versioned implicitly by their magic; a
breaking change bumps the magic. Application schema evolution is the
application's concern — a decode failure on an `Input` frame surfaces as a
typed error naming the seq, rather than silent misinterpretation.
