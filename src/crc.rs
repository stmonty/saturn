pub mod crc32c {
    // Reflected polynomial for CRC32C (Castagnoli).
    // Using the standard reflected algorithm:
    // init = 0xFFFF_FFFF, process LSB-first, xorout = 0xFFFF_FFFF.
    const POLY_REFLECTED: u32 = 0x82F6_3B78;

    #[inline]
    fn update_byte(mut crc: u32, b: u8) -> u32 {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg() & POLY_REFLECTED;
            crc = (crc >> 1) ^ mask;
        }
        crc
    }

    #[inline]
    pub fn value(data: &[u8]) -> u32 {
        let mut crc = 0xFFFF_FFFFu32;
        for &b in data {
            crc = update_byte(crc, b);
        }
        crc ^ 0xFFFF_FFFF
    }

    // Extend a prior CRC with more bytes (i.e., crc(data0 || data1)).
    #[inline]
    pub fn extend(initial_crc: u32, data: &[u8]) -> u32 {
        let mut crc = initial_crc ^ 0xFFFF_FFFF;
        for &b in data {
            crc = update_byte(crc, b);
        }
        crc ^ 0xFFFF_FFFF
    }

    // Same masking LevelDB uses: rotate-right by 15, add a constant.
    #[inline]
    pub fn mask(crc: u32) -> u32 {
        ((crc >> 15) | (crc << 17)).wrapping_add(0xA282_EAD8)
    }

    #[allow(dead_code)]
    #[inline]
    pub fn unmask(masked: u32) -> u32 {
        let rot = masked.wrapping_sub(0xA282_EAD8);
        (rot << 15) | (rot >> 17)
    }
}
