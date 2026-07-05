//! CRC32C (Castagnoli), the checksum used for frame headers and payloads.
//!
//! Dispatches to a hardware implementation when the CPU has the CRC32C
//! instructions — ARMv8 `crc32c*` (aarch64) or SSE4.2 `crc32` (x86-64) —
//! detected once at runtime, and falls back to a compile-time software table
//! otherwise. All paths compute the identical checksum (a differential test
//! pins hardware == software), so the wire format is unchanged; hardware just
//! makes the header+payload checks on the sequencer hot path several times
//! cheaper. Still dependency-free: the intrinsics come from `core::arch`.

const POLY: u32 = 0x82F6_3B78; // reflected Castagnoli polynomial

const TABLE: [u32; 256] = build_table();

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// Portable software CRC32C — the reference, and the fallback when no
/// hardware instruction is available.
fn crc32c_sw(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc = (crc >> 8) ^ TABLE[((crc ^ byte as u32) & 0xFF) as usize];
    }
    !crc
}

/// CRC32C of `data`. Uses a hardware instruction where available.
#[inline]
pub fn crc32c(data: &[u8]) -> u32 {
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("crc") {
            // SAFETY: the `crc` feature was just detected at runtime.
            return unsafe { crc32c_aarch64(data) };
        }
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("sse4.2") {
            // SAFETY: `sse4.2` was just detected at runtime.
            return unsafe { crc32c_x86(data) };
        }
    }
    crc32c_sw(data)
}

/// ARMv8 CRC32C: fold 8 bytes at a time with `crc32cd`, then the tail with
/// the narrower instructions.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "crc")]
unsafe fn crc32c_aarch64(data: &[u8]) -> u32 {
    use std::arch::aarch64::{__crc32cb, __crc32cd, __crc32ch, __crc32cw};
    let mut crc = !0u32;
    let mut chunks = data.chunks_exact(8);
    for c in chunks.by_ref() {
        crc = __crc32cd(crc, u64::from_le_bytes(c.try_into().unwrap()));
    }
    let mut rem = chunks.remainder();
    if rem.len() >= 4 {
        crc = __crc32cw(crc, u32::from_le_bytes(rem[..4].try_into().unwrap()));
        rem = &rem[4..];
    }
    if rem.len() >= 2 {
        crc = __crc32ch(crc, u16::from_le_bytes(rem[..2].try_into().unwrap()));
        rem = &rem[2..];
    }
    if let Some(&b) = rem.first() {
        crc = __crc32cb(crc, b);
    }
    !crc
}

/// SSE4.2 CRC32C: fold 8 bytes at a time with `_mm_crc32_u64`, then the tail.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn crc32c_x86(data: &[u8]) -> u32 {
    use std::arch::x86_64::{_mm_crc32_u16, _mm_crc32_u32, _mm_crc32_u64, _mm_crc32_u8};
    let mut crc = !0u32;
    let mut chunks = data.chunks_exact(8);
    for c in chunks.by_ref() {
        crc = _mm_crc32_u64(crc as u64, u64::from_le_bytes(c.try_into().unwrap())) as u32;
    }
    let mut rem = chunks.remainder();
    if rem.len() >= 4 {
        crc = _mm_crc32_u32(crc, u32::from_le_bytes(rem[..4].try_into().unwrap()));
        rem = &rem[4..];
    }
    if rem.len() >= 2 {
        crc = _mm_crc32_u16(crc, u16::from_le_bytes(rem[..2].try_into().unwrap()));
        rem = &rem[2..];
    }
    if let Some(&b) = rem.first() {
        crc = _mm_crc32_u8(crc, b);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::{crc32c, crc32c_sw};

    #[test]
    fn known_vectors() {
        // RFC 3720 / common test vector.
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
        // 32 bytes of zeros, from the iSCSI spec examples.
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
        // 32 bytes of 0xFF.
        assert_eq!(crc32c(&[0xFFu8; 32]), 0x62A8_AB43);
    }

    #[test]
    fn hardware_matches_software_over_all_lengths() {
        // Every length 0..300 (crosses the 8/4/2/1 tail boundaries many
        // times), with a non-trivial byte pattern. The dispatched `crc32c`
        // (hardware where present) must byte-match the reference software
        // routine — otherwise a hardware node and a software node would
        // compute different checksums for the same frame.
        let mut data = Vec::new();
        for n in 0..300usize {
            data.clear();
            for i in 0..n {
                data.push((i as u8).wrapping_mul(31).wrapping_add(7));
            }
            assert_eq!(
                crc32c(&data),
                crc32c_sw(&data),
                "hardware/software CRC mismatch at len {n}"
            );
        }
    }
}
