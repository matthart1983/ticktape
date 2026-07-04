//! CRC32C (Castagnoli), the checksum used for frame headers and payloads.
//!
//! Software table implementation, built at compile time. Kept in-tree so
//! `ticktape-core` stays dependency-free; a hardware (SSE4.2 / ARMv8 CRC)
//! path can be added later behind the same function.

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

/// CRC32C of `data`.
pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc = (crc >> 8) ^ TABLE[((crc ^ byte as u32) & 0xFF) as usize];
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::crc32c;

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
}
